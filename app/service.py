"""Submission/acceptance service and read models."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import asyncpg

from .config import settings
from .csvutil import ParsedCsv, median_nm
from .revision import compute_revision, validate_replacement_wafer_set
from .storage import StoredArtifact

# Unique constraint backing per-device accept_seq (see schema.py).  Concurrent
# inserts of the same next sequence collide here and are retried, which keeps
# every intake transaction short -- no connection is ever held while blocked
# on a lock, so the pool cannot be starved by same-device contention.
DEVICE_SEQ_CONSTRAINT = "batches_device_accept_uniq"
BATCH_NO_CONSTRAINT = "batches_batch_no_uniq"


class ConflictError(Exception):
    """HTTP 409: duplicate batch number with conflicting content/device."""

    def __init__(self, message: str, existing: Dict[str, Any]):
        super().__init__(message)
        self.existing = existing


@dataclass
class Acceptance:
    batch_id: int
    batch_no: str
    device_id: str
    accept_seq: int
    status: str
    reused: bool


async def accept_submission(
    pool: asyncpg.pool.Pool,
    *,
    batch_no: str,
    device_id: str,
    raw: bytes,
    parsed: ParsedCsv,
    artifact: StoredArtifact,
) -> Acceptance:
    """Persist a new batch or resolve an idempotent retry.

    The original file is already on disk (see :mod:`app.storage`) before this
    runs.  Same ``batch_no`` + same device + identical bytes reuses the
    existing row; any other combination of an existing number is rejected.
    """
    for attempt in range(50):
        try:
            return await _accept_once(
                pool, batch_no=batch_no, device_id=device_id,
                parsed=parsed, artifact=artifact)
        except asyncpg.UniqueViolationError as exc:
            if exc.constraint_name == DEVICE_SEQ_CONSTRAINT:
                # Another same-device submission won this sequence number;
                # re-read the max and try again. Tiny bounded backoff.
                await asyncio.sleep(min(0.05, 0.005 * (attempt + 1)))
                continue
            if exc.constraint_name == BATCH_NO_CONSTRAINT:
                # Race: two brand-new submissions with the same batch_no.
                # Re-read and turn it into the normal 409 decision.
                async with pool.acquire() as conn:
                    existing = await conn.fetchrow(
                        "SELECT id, batch_no, device_id, sha256, accept_seq, "
                        "status FROM batches WHERE batch_no = $1", batch_no)
                if existing is not None:
                    return _reuse_or_conflict(batch_no, device_id, artifact, existing)
            raise
    raise RuntimeError(f"could not allocate accept_seq for {batch_no} after retries")


async def _accept_once(pool, *, batch_no, device_id, parsed, artifact) -> Acceptance:
    async with pool.acquire() as conn:
        async with conn.transaction():
            existing = await conn.fetchrow(
                "SELECT id, batch_no, device_id, sha256, accept_seq, status "
                "FROM batches WHERE batch_no = $1",
                batch_no,
            )
            if existing is not None:
                return _reuse_or_conflict(batch_no, device_id, artifact, existing)

            next_seq = await conn.fetchval(
                "SELECT COALESCE(MAX(accept_seq), 0) + 1 FROM batches "
                "WHERE device_id = $1",
                device_id,
            )
            batch_id = await conn.fetchval(
                """
                INSERT INTO batches
                    (batch_no, device_id, sha256, size_bytes, storage_path,
                     accept_seq, status)
                VALUES ($1, $2, $3, $4, $5, $6, 'PENDING')
                RETURNING id
                """,
                batch_no, device_id, artifact.sha256, artifact.size_bytes,
                artifact.storage_path, next_seq,
            )
            await conn.executemany(
                "INSERT INTO wafer_measurements(batch_id, wafer_no, thickness_nm) "
                "VALUES ($1, $2, $3)",
                [(batch_id, w, t) for w, t in parsed.rows],
            )
            await conn.execute(
                "INSERT INTO device_state(device_id) VALUES ($1) "
                "ON CONFLICT (device_id) DO NOTHING",
                device_id,
            )
            return Acceptance(
                batch_id=batch_id, batch_no=batch_no, device_id=device_id,
                accept_seq=next_seq, status="PENDING", reused=False,
            )


def _reuse_or_conflict(batch_no: str, device_id: str,
                       artifact: StoredArtifact,
                       existing: asyncpg.Record) -> Acceptance:
    """Idempotent reuse on identical re-submission, else HTTP 409."""
    if existing["device_id"] != device_id:
        raise ConflictError(
            f"批号 {batch_no} 已被其他设备占用 / batch number already "
            "registered for another device", _brief(existing))
    if existing["sha256"] != artifact.sha256:
        raise ConflictError(
            f"批号 {batch_no} 已存在但 CSV 内容不同（sha256 不一致），"
            "拒绝重复提交 / same batch number with different content",
            _brief(existing))
    # Identical retry: re-use the original acceptance.
    return Acceptance(
        batch_id=existing["id"], batch_no=batch_no, device_id=device_id,
        accept_seq=existing["accept_seq"], status=existing["status"],
        reused=True,
    )


def _brief(row: asyncpg.Record) -> Dict[str, Any]:
    return {
        "batch_no": row["batch_no"],
        "device_id": row["device_id"],
        "accept_seq": row["accept_seq"],
        "status": row["status"],
    }


class RevisionRequestError(Exception):
    """A simulation request that cannot be answered with a sound snapshot."""

    def __init__(self, message: str, status_code: int = 409,
                 details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.status_code = status_code
        self.details = details or {}


_SNAPSHOT_SQL = """
    SELECT id, batch_no, device_id, accept_seq, status, median_nm,
           ref_mean_nm, deviation_nm, reference_ids
      FROM batches
     WHERE device_id = $1
     ORDER BY accept_seq
"""


async def simulate_revision(
    pool: asyncpg.pool.Pool,
    *,
    device_id: str,
    batch_no: str,
    parsed: ParsedCsv,
) -> Dict[str, Any]:
    """Replay the committed decision chain with a revised target thickness.

    Everything is read inside one ``REPEATABLE READ`` read-only transaction, so
    the report describes a single point-in-time snapshot: decisions committed
    concurrently after that snapshot are invisible, and the cutoff batch number
    in the response pins down exactly how far the snapshot reaches.  Nothing is
    written -- not the original CSV, not any decision row, not the data-lake
    read model.
    """
    async with pool.acquire() as conn:
        tx = conn.transaction(isolation="repeatable_read", readonly=True)
        async with tx:
            rows = await conn.fetch(_SNAPSHOT_SQL, device_id)
            target = next((r for r in rows if r["batch_no"] == batch_no), None)
            if target is None:
                raise RevisionRequestError(
                    f"设备 {device_id} 下无此批号 / batch not found for device: "
                    f"{batch_no}", status_code=404)
            if target["status"] == "PENDING":
                raise RevisionRequestError(
                    f"批号 {batch_no} 尚未判定，不能作为试算基准 / "
                    "target batch has no committed decision yet", status_code=409)

            decided = [r for r in rows if r["status"] != "PENDING"]
            # The worker decides strictly in accept_seq order, so committed
            # decisions must form the contiguous prefix 1..cutoff.  Anything
            # else means the snapshot is not a valid replay base.
            if [int(r["accept_seq"]) for r in decided] != \
                    list(range(1, len(decided) + 1)):
                raise RevisionRequestError(
                    "设备判定序列不连续，无法构造一致快照 / "
                    "decision sequence is not a contiguous prefix",
                    status_code=409)
            if any(r["median_nm"] is None for r in decided):
                raise RevisionRequestError(
                    "快照中存在缺少中位数的已判定批 / snapshot contains a "
                    "decided batch without median", status_code=409)

            cutoff = decided[-1]
            followups = int(cutoff["accept_seq"]) - int(target["accept_seq"])
            limit = settings.revision_max_followup_batches
            if followups > limit:
                # Explicit refusal: never return a truncated report.
                raise RevisionRequestError(
                    f"目标批之后有 {followups} 个已判定批，超过上限 {limit}，"
                    "拒绝试算 / too many decided batches after the target; "
                    "simulation refused rather than truncated",
                    status_code=409,
                    details={"decided_after_target": followups,
                             "max_decided_after_target": limit,
                             "snapshot_cutoff_batch_no": cutoff["batch_no"]},
                )

            original_wafers = await conn.fetch(
                "SELECT wafer_no, thickness_nm FROM wafer_measurements "
                "WHERE batch_id = $1", target["id"])
            try:
                validate_replacement_wafer_set(
                    [w["wafer_no"] for w in original_wafers], parsed.rows)
            except ValueError as exc:  # WaferSetMismatch
                raise RevisionRequestError(
                    str(exc), status_code=400,
                    details={"missing_wafer_nos": getattr(exc, "missing", []),
                             "extra_wafer_nos": getattr(exc, "extra", [])},
                ) from exc

            snapshot = [
                {
                    "id": r["id"],
                    "batch_no": r["batch_no"],
                    "accept_seq": r["accept_seq"],
                    "status": r["status"],
                    "median_nm": float(r["median_nm"]),
                    "ref_mean_nm": (float(r["ref_mean_nm"])
                                    if r["ref_mean_nm"] is not None else None),
                    "deviation_nm": (float(r["deviation_nm"])
                                     if r["deviation_nm"] is not None else None),
                    "reference_ids": [int(i) for i in (r["reference_ids"] or [])],
                }
                for r in decided
            ]

            revised_median = median_nm([t for _, t in parsed.rows])
            result = compute_revision(
                snapshot,
                target_seq=int(target["accept_seq"]),
                revised_median_nm=revised_median,
                baseline_count=settings.baseline_count,
                reference_count=settings.reference_count,
                threshold_nm=settings.drift_threshold_nm,
            )

            return {
                "device_id": device_id,
                "target_batch_no": batch_no,
                "target_accept_seq": int(target["accept_seq"]),
                "original_median_nm": float(target["median_nm"]),
                "revised_median_nm": revised_median,
                "threshold_nm": settings.drift_threshold_nm,
                "baseline_count": settings.baseline_count,
                "reference_count": settings.reference_count,
                "snapshot_cutoff_batch_no": cutoff["batch_no"],
                "snapshot_cutoff_accept_seq": int(cutoff["accept_seq"]),
                "decided_after_target": followups,
                "evaluated_count": result["evaluated_count"],
                "target_result": result["target"],
                "changed_count": result["changed_count"],
                "changed_batches": result["changed_batches"],
                # The endpoint is read-only by construction; stated explicitly
                # so operators can see that nothing was mutated.
                "applied": False,
            }


async def get_batch_detail(pool: asyncpg.pool.Pool,
                           batch_no: str) -> Optional[Dict[str, Any]]:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM batches WHERE batch_no = $1", batch_no)
        if row is None:
            return None
        ref_ids: List[int] = list(row["reference_ids"] or [])
        ref_batch_nos: List[str] = []
        if ref_ids:
            ref_rows = await conn.fetch(
                "SELECT id, batch_no FROM batches WHERE id = ANY($1::bigint[]) "
                "ORDER BY array_position($2::bigint[], id)",
                ref_ids, ref_ids,
            )
            ref_batch_nos = [r["batch_no"] for r in ref_rows]
        return {
            "batch_no": row["batch_no"],
            "device_id": row["device_id"],
            "status": row["status"],
            "accept_seq": row["accept_seq"],
            "wafer_count": await conn.fetchval(
                "SELECT count(*) FROM wafer_measurements WHERE batch_id=$1",
                row["id"]),
            "median_nm": row["median_nm"],
            "reference_mean_nm": row["ref_mean_nm"],
            "deviation_nm": row["deviation_nm"],
            "reference_batch_ids": ref_ids,
            "reference_batch_nos": ref_batch_nos,
            "sha256": row["sha256"],
            "original_storage_path": row["storage_path"],
            "attempts": row["attempts"],
            "decided_by": row["decided_by"],
            "created_at": row["created_at"].isoformat() if row["created_at"] else None,
            "decided_at": row["decided_at"].isoformat() if row["decided_at"] else None,
        }


async def list_device_batches(pool: asyncpg.pool.Pool,
                              device_id: str) -> List[Dict[str, Any]]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT batch_no, accept_seq, status, median_nm, deviation_nm, "
            "decided_at, attempts FROM batches WHERE device_id=$1 "
            "ORDER BY accept_seq",
            device_id,
        )
        return [
            {
                "batch_no": r["batch_no"],
                "accept_seq": r["accept_seq"],
                "status": r["status"],
                "median_nm": r["median_nm"],
                "deviation_nm": r["deviation_nm"],
                "attempts": r["attempts"],
                "decided_at": r["decided_at"].isoformat() if r["decided_at"] else None,
            }
            for r in rows
        ]


async def get_released_data(pool: asyncpg.pool.Pool, *,
                            device_id: Optional[str],
                            batch_no: Optional[str],
                            limit: int,
                            offset: int) -> Tuple[int, List[Dict[str, Any]]]:
    """Read model for the data lake: *only* released batches are served."""
    where = ["status = 'RELEASED'"]
    params: List[Any] = []
    if device_id:
        params.append(device_id)
        where.append(f"device_id = ${len(params)}")
    if batch_no:
        params.append(batch_no)
        where.append(f"batch_no = ${len(params)}")
    where_sql = " WHERE " + " AND ".join(where)

    async with pool.acquire() as conn:
        total = await conn.fetchval(
            f"SELECT count(*) FROM batches{where_sql}", *params)
        params.append(limit)
        params.append(offset)
        rows = await conn.fetch(
            f"SELECT id, batch_no, device_id, accept_seq, median_nm, decided_at "
            f"FROM batches{where_sql} ORDER BY id "
            f"LIMIT ${len(params) - 1} OFFSET ${len(params)}",
            *params,
        )
        out: List[Dict[str, Any]] = []
        for r in rows:
            wafers = await conn.fetch(
                "SELECT wafer_no, thickness_nm FROM wafer_measurements "
                "WHERE batch_id=$1 ORDER BY wafer_no", r["id"])
            out.append({
                "batch_no": r["batch_no"],
                "device_id": r["device_id"],
                "accept_seq": r["accept_seq"],
                "median_nm": r["median_nm"],
                "decided_at": r["decided_at"].isoformat() if r["decided_at"] else None,
                "wafers": [
                    {"wafer_no": w["wafer_no"], "thickness_nm": w["thickness_nm"]}
                    for w in wafers
                ],
            })
        return total, out
