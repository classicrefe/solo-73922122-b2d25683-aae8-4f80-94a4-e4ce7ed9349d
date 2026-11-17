"""FastAPI application: intake, decision query and data-lake read API.

Run two instances behind the load balancer defined in docker-compose.yml;
each instance serves HTTP and runs one background decision worker.
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import (
    FastAPI,
    File,
    Form,
    HTTPException,
    Path,
    Query,
    UploadFile,
    status,
)
from fastapi.responses import JSONResponse

from .config import settings
from .csvutil import CsvValidationError, parse_thickness_csv
from .db import close_pool, create_pool
from .service import (
    ConflictError,
    RevisionRequestError,
    accept_submission,
    get_batch_detail,
    get_released_data,
    list_device_batches,
    simulate_revision,
)
from .storage import persist_original
from .worker import DecisionWorker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("fab.api")
MAX_UPLOAD_BYTES = 8 * 1024 * 1024


@asynccontextmanager
async def lifespan(app: FastAPI):
    pool = await create_pool()
    worker = DecisionWorker(pool, settings.instance_id)
    worker.start()
    app.state.worker = worker
    try:
        yield
    finally:
        await worker.stop()
        await close_pool()


app = FastAPI(
    title="晶圆厚度批次放行 API / Wafer Thickness Batch Release API",
    version="1.0.0",
    description=(
        "厚度明细入湖前的漂移批次隔离服务。\n\n"
        "同设备按持久化受理顺序判定：前 3 批放行；其后批中位数与最近 3 个"
        "已放行批中位数均值相差超过 5nm 则隔离，隔离批不进入参考窗口。"
        "双实例后台处理，同设备严格 FIFO、不阻塞其他设备。"
    ),
    lifespan=lifespan,
)


@app.exception_handler(CsvValidationError)
async def _csv_error_handler(_request, exc: CsvValidationError):
    return JSONResponse(status_code=400, content={"error": str(exc)})


@app.exception_handler(ConflictError)
async def _conflict_handler(_request, exc: ConflictError):
    return JSONResponse(
        status_code=409,
        content={"error": str(exc), "existing": exc.existing},
    )


@app.exception_handler(RevisionRequestError)
async def _revision_error_handler(_request, exc: RevisionRequestError):
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": str(exc), **exc.details},
    )


@app.get("/health")
async def health():
    from .db import get_pool
    try:
        async with get_pool().acquire() as conn:
            await conn.fetchval("SELECT 1")
        db_ok = True
    except Exception:  # pragma: no cover
        db_ok = False
    return {"status": "ok" if db_ok else "degraded",
            "instance_id": settings.instance_id, "database": db_ok}


@app.post("/batches", status_code=status.HTTP_202_ACCEPTED,
          summary="提交一批晶圆厚度 CSV（原件持久化后受理）")
async def submit_batch(
    batch_no: str = Form(..., description="全局唯一批号"),
    device_id: str = Form(..., description="设备号"),
    file: UploadFile = File(..., description="CSV：晶圆号,厚度nm"),
):
    batch_no = batch_no.strip()
    device_id = device_id.strip()
    if not batch_no or not device_id:
        raise HTTPException(400, "batch_no 与 device_id 不能为空 / fields required")

    raw = await file.read()
    if len(raw) > MAX_UPLOAD_BYTES:
        raise HTTPException(413, "CSV 超过 8MB 上限 / file too large")

    parsed = parse_thickness_csv(raw)          # 400 on any structural violation
    artifact = persist_original(raw)           # durable original before acceptance
    acceptance = await accept_submission(
        get_pool_safe(),
        batch_no=batch_no, device_id=device_id,
        raw=raw, parsed=parsed, artifact=artifact,
    )
    return {
        "batch_no": acceptance.batch_no,
        "device_id": acceptance.device_id,
        "accept_seq": acceptance.accept_seq,
        "batch_id": acceptance.batch_id,
        "status": acceptance.status,
        "reused_retry": acceptance.reused,
        "wafer_count": parsed.count,
        "sha256": artifact.sha256,
        "message": ("同原件重试，复用既有受理 / identical retry reused"
                    if acceptance.reused
                    else "已受理，等待后台判定 / accepted, pending decision"),
    }


@app.get("/batches/{batch_no}", summary="查询判定结果及参考批号")
async def query_batch(batch_no: str):
    detail = await get_batch_detail(get_pool_safe(), batch_no)
    if detail is None:
        raise HTTPException(404, f"批号不存在 / batch not found: {batch_no}")
    return detail


@app.get("/devices/{device_id}/batches", summary="按设备列出受理顺序与判定")
async def device_batches(device_id: str):
    rows = await list_device_batches(get_pool_safe(), device_id)
    return {"device_id": device_id, "count": len(rows), "batches": rows}


@app.post(
    "/devices/{device_id}/batches/{batch_no}/revision-simulation",
    summary="厚度修订影响试算（只读，不改变任何已提交判定）",
    description=(
        "上传与原批晶圆号完全一致、仅厚度可变的替代 CSV，按同一已提交判定"
        "快照从目标批起重放判定规则：前 3 批直接放行，其后与最近 3 个"
        "（试算后）已放行批中位数均值比较，偏差 > 5nm 隔离。\n\n"
        "返回结论发生变化的批号及其实际/试算参考批号与偏差，以及快照截止"
        "批号。若目标批之后已判定批超过 50 个则拒绝试算并返回 409，绝不返回"
        "截断结果。试算只读：不改原件、不改实际判定、不影响供数。"
    ),
    response_model=None,
)
async def revision_simulation(
    device_id: str = Path(..., description="设备号"),
    batch_no: str = Path(..., description="该设备下已判定的目标批号"),
    file: UploadFile = File(..., description="替代 CSV：晶圆号,厚度nm"),
):
    device_id = device_id.strip()
    batch_no = batch_no.strip()
    if not device_id or not batch_no:
        raise HTTPException(400, "device_id 与 batch_no 不能为空 / required")

    raw = await file.read()
    if len(raw) > MAX_UPLOAD_BYTES:
        raise HTTPException(413, "CSV 超过 8MB 上限 / file too large")
    parsed = parse_thickness_csv(raw)  # 400 on structural violations

    # Note: the replacement bytes are deliberately NOT persisted; the
    # simulation must not create any durable artifact.
    return await simulate_revision(
        get_pool_safe(), device_id=device_id, batch_no=batch_no, parsed=parsed)


@app.get("/data/released", summary="数据湖供数接口（仅放行明细，隔离批不可见）")
async def released_data(
    device_id: Optional[str] = Query(None),
    batch_no: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    total, items = await get_released_data(
        get_pool_safe(), device_id=device_id, batch_no=batch_no,
        limit=limit, offset=offset)
    return {"total_released": total, "limit": limit, "offset": offset, "items": items}


def get_pool_safe():
    from .db import get_pool
    return get_pool()


@app.post("/internal/shutdown", include_in_schema=False)
async def internal_shutdown(token: str = Query(...)):
    """Graceful shutdown hook for the crash-takeover demonstration.

    Disabled unless ``SHUTDOWN_TOKEN`` is configured; the caller must present
    the same token.  In production simply leave the variable unset.
    """
    import os
    import signal
    expected = os.environ.get("SHUTDOWN_TOKEN", "")
    if not expected or token != expected:
        raise HTTPException(404, "not found")
    worker = getattr(app.state, "worker", None)
    if worker is not None:
        await worker.stop()
    await close_pool()
    try:
        os.kill(os.getpid(), signal.SIGTERM)
    except PermissionError:  # pragma: no cover - sandboxed demo environments
        os._exit(0)
    return {"status": "shutting down"}
