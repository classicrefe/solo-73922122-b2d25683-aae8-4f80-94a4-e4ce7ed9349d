"""Tests for the thickness-revision impact simulation endpoint.

Covers:
* rule replay from a committed snapshot, including verdict changes that
  propagate through the reference window (a flipped batch changes the
  reference set of later batches);
* wafer-number identity enforcement (only thickness may change);
* explicit refusal when more than 50 decided batches follow the target --
  never a truncated report;
* read-only behaviour: originals, committed decisions and the data-lake read
  model are untouched;
* snapshot isolation: a decision committed while the simulation transaction
  is open does not leak into the report, and the reported cutoff pins the
  snapshot.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import subprocess
import sys
import time

import pytest

from app.config import settings
from app.csvutil import parse_thickness_csv
from app.db import close_pool, create_pool
from app.service import RevisionRequestError, simulate_revision
from tests.conftest import make_csv, submit, uniform_csv, wait_status

DSN = os.environ.get("DATABASE_DSN") or open("/tmp/pg_dsn.txt").read().strip()


# --------------------------------------------------------------- HTTP helpers
async def _simulate(ac, device, batch_no, rows, header=True):
    return await ac.post(
        f"/devices/{device}/batches/{batch_no}/revision-simulation",
        files={"file": ("rev.csv", make_csv(rows, header=header), "text/csv")},
    )


async def _setup_device(ac, device, medians):
    """Create one batch per median (5 identical wafers -> median == value)."""
    names = []
    for i, median in enumerate(medians, start=1):
        batch_no = f"{device}-B{i}"
        r = await submit(ac, batch_no, device, uniform_csv(f"{device}b{i}", 5, median))
        assert r.status_code == 202, r.text
        names.append(batch_no)

    # Wait until every batch has a committed verdict.
    deadline = time.time() + 15
    statuses = []
    while time.time() < deadline:
        statuses = [(await ac.get(f"/batches/{b}")).json()["status"] for b in names]
        if all(s != "PENDING" for s in statuses):
            return names, statuses
        await asyncio.sleep(0.1)
    raise AssertionError(f"batches not decided: {list(zip(names, statuses))}")


# --------------------------------------------------------- rule replay tests
@pytest.mark.asyncio
async def test_revision_flip_propagates_through_reference_window(client):
    """Revising B4 from 506 to 505 flips B4 Q->R and B5 Q->R via new refs."""
    ac = client
    names, actual = await _setup_device(ac, "EQ-R", [500, 500, 500, 506, 506])
    assert actual == ["RELEASED", "RELEASED", "RELEASED",
                      "QUARANTINED", "QUARANTINED"]

    # Replacement CSV: same five wafer numbers, thickness 505.
    r = await _simulate(ac, "EQ-R", "EQ-R-B4",
                        [(f"EQ-Rb4-W{i:03d}", 505) for i in range(1, 6)])
    assert r.status_code == 200, r.text
    body = r.json()

    assert body["applied"] is False
    assert body["target_batch_no"] == "EQ-R-B4"
    assert body["original_median_nm"] == pytest.approx(506.0)
    assert body["revised_median_nm"] == pytest.approx(505.0)
    assert body["snapshot_cutoff_batch_no"] == "EQ-R-B5"
    assert body["snapshot_cutoff_accept_seq"] == 5
    assert body["decided_after_target"] == 1
    assert body["evaluated_count"] == 2

    target = body["target_result"]
    assert target["actual_status"] == "QUARANTINED"
    assert target["simulated_status"] == "RELEASED"
    assert target["actual_reference_batch_nos"] == ["EQ-R-B1", "EQ-R-B2", "EQ-R-B3"]
    assert target["simulated_reference_batch_nos"] == \
        ["EQ-R-B1", "EQ-R-B2", "EQ-R-B3"]
    assert target["actual_deviation_nm"] == pytest.approx(6.0)
    assert target["simulated_deviation_nm"] == pytest.approx(5.0)

    assert body["changed_count"] == 2
    changed = body["changed_batches"]
    assert [c["batch_no"] for c in changed] == ["EQ-R-B4", "EQ-R-B5"]

    # B5 changed *because* B4 is now released: its simulated reference set is
    # the revised rolling window, not the committed one.
    b5 = changed[1]
    assert b5["actual_status"] == "QUARANTINED"
    assert b5["simulated_status"] == "RELEASED"
    assert b5["actual_reference_batch_nos"] == ["EQ-R-B1", "EQ-R-B2", "EQ-R-B3"]
    assert b5["simulated_reference_batch_nos"] == ["EQ-R-B2", "EQ-R-B3", "EQ-R-B4"]
    assert b5["actual_reference_mean_nm"] == pytest.approx(500.0)
    assert b5["simulated_reference_mean_nm"] == pytest.approx(501.6666667)
    assert b5["actual_deviation_nm"] == pytest.approx(6.0)
    assert b5["simulated_deviation_nm"] == pytest.approx(4.3333333)

    # Committed decisions were not touched.
    for batch_no, status in zip(names, actual):
        d = (await ac.get(f"/batches/{batch_no}")).json()
        assert d["status"] == status


@pytest.mark.asyncio
async def test_revision_can_turn_release_into_quarantine(client):
    """A downward revision changes a later batch's reference mean -> Q."""
    ac = client
    await _setup_device(ac, "EQ-T", [500, 500, 500, 500, 504])
    r = await _simulate(ac, "EQ-T", "EQ-T-B4",
                        [(f"EQ-Tb4-W{i:03d}", 496) for i in range(1, 6)])
    assert r.status_code == 200, r.text
    body = r.json()

    assert body["target_result"]["actual_status"] == "RELEASED"
    assert body["target_result"]["simulated_status"] == "RELEASED"
    # 496 vs mean(500,500,500) = 4 nm -> still released.
    assert body["target_result"]["simulated_deviation_nm"] == pytest.approx(4.0)

    assert [c["batch_no"] for c in body["changed_batches"]] == ["EQ-T-B5"]
    b5 = body["changed_batches"][0]
    assert b5["actual_status"] == "RELEASED"
    assert b5["simulated_status"] == "QUARANTINED"
    assert b5["simulated_reference_batch_nos"] == ["EQ-T-B2", "EQ-T-B3", "EQ-T-B4"]
    assert b5["simulated_deviation_nm"] == pytest.approx(16.0 / 3.0)
    # And the actual decision remains released.
    d = (await ac.get("/batches/EQ-T-B5")).json()
    assert d["status"] == "RELEASED" and d["deviation_nm"] == pytest.approx(4.0)


@pytest.mark.asyncio
async def test_revision_baseline_target_stays_released(client):
    ac = client
    await _setup_device(ac, "EQ-U", [500, 500, 500, 504])
    r = await _simulate(ac, "EQ-U", "EQ-U-B1",
                        [(f"EQ-Ub1-W{i:03d}", 900) for i in range(1, 6)])
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["target_result"]["simulated_status"] == "RELEASED"
    assert body["target_result"]["simulated_reference_batch_nos"] == []
    # B4's actual reference set includes B1, so the 900 revision changes its
    # simulated mean and deviation -> quarantine.
    assert body["changed_count"] == 1
    b4 = body["changed_batches"][0]
    assert b4["batch_no"] == "EQ-U-B4"
    assert b4["simulated_status"] == "QUARANTINED"
    assert b4["simulated_reference_batch_nos"] == ["EQ-U-B1", "EQ-U-B2", "EQ-U-B3"]
    assert b4["simulated_reference_mean_nm"] == pytest.approx(1900.0 / 3.0)


# ------------------------------------------------- validation / refusal cases
@pytest.mark.asyncio
async def test_revision_rejects_changed_wafer_numbers(client):
    ac = client
    await _setup_device(ac, "EQ-V", [500, 500, 500, 504])

    # Missing one wafer number.
    rows = [(f"EQ-Vb4-W{i:03d}", 504) for i in range(1, 6)]
    r = await _simulate(ac, "EQ-V", "EQ-V-B4", rows[:-1])
    assert r.status_code == 400, r.text
    assert r.json()["missing_wafer_nos"] == ["EQ-Vb4-W005"]

    # Extra wafer number.
    r = await _simulate(ac, "EQ-V", "EQ-V-B4", rows + [("EXTRA", 504)])
    assert r.status_code == 400, r.text
    assert r.json()["extra_wafer_nos"] == ["EXTRA"]

    # Renamed wafer number (missing + extra).
    renamed = [(f"X{i}", 504) for i in range(1, 6)]
    r = await _simulate(ac, "EQ-V", "EQ-V-B4", renamed)
    assert r.status_code == 400, r.text
    assert set(r.json()["missing_wafer_nos"]) == {w for w, _ in rows}
    assert set(r.json()["extra_wafer_nos"]) == {w for w, _ in renamed}

    # Structural CSV errors still return 400.
    r = await ac.post(
        "/devices/EQ-V/batches/EQ-V-B4/revision-simulation",
        files={"file": ("rev.csv", b"wafer_no,thickness_nm\nW1,0\n", "text/csv")})
    assert r.status_code == 400


@pytest.mark.asyncio
async def test_revision_unknown_or_foreign_batch_returns_404(client):
    ac = client
    await _setup_device(ac, "EQ-W", [500, 500, 500, 504])
    r = await _simulate(ac, "EQ-W", "EQ-W-NOPE",
                        [(f"EQ-Wb4-W{i:03d}", 504) for i in range(1, 6)])
    assert r.status_code == 404

    # The batch exists, but under a different device: not addressable here.
    await submit(ac, "OTHER-B1", "EQ-OTHER", uniform_csv("ob1", 5, 500))
    await wait_status(ac, "OTHER-B1", "RELEASED")
    r = await _simulate(ac, "EQ-W", "OTHER-B1",
                        [(f"EQ-Wb4-W{i:03d}", 504) for i in range(1, 6)])
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_revision_is_read_only_for_original_decision_and_data(client):
    ac = client
    await _setup_device(ac, "EQ-X2", [500, 500, 500, 506])
    before_detail = (await ac.get("/batches/EQ-X2-B4")).json()
    before_original = open(before_detail["original_storage_path"], "rb").read()
    before_sha = hashlib.sha256(before_original).hexdigest()
    before_data = (await ac.get("/data/released")).json()

    replacement = make_csv([(f"EQ-X2b4-W{i:03d}", 505) for i in range(1, 6)])
    r = await _simulate(ac, "EQ-X2", "EQ-X2-B4",
                        [(f"EQ-X2b4-W{i:03d}", 505) for i in range(1, 6)])
    assert r.status_code == 200 and r.json()["changed_count"] == 1

    after_detail = (await ac.get("/batches/EQ-X2-B4")).json()
    assert after_detail == before_detail
    after_original = open(after_detail["original_storage_path"], "rb").read()
    assert after_original == before_original
    assert hashlib.sha256(after_original).hexdigest() == before_sha
    assert (await ac.get("/data/released")).json() == before_data

    # The replacement bytes were never persisted (content-addressed store).
    replacement_sha = hashlib.sha256(replacement).hexdigest()
    stored = os.path.join(settings.storage_dir, replacement_sha[:2],
                          replacement_sha[2:4], f"{replacement_sha}.csv")
    assert not os.path.exists(stored)


# ------------------------------------------------- service-level snapshot tests
def _direct_sql(query: str) -> list:
    """Synchronous SQL helper (usable from inside a sync monkeypatch)."""
    code = (
        "import asyncio, asyncpg, os, json\n"
        "async def m():\n"
        "    c = await asyncpg.connect(os.environ['DATABASE_DSN'])\n"
        "    await c.set_type_codec('jsonb', encoder=json.dumps, "
        "decoder=json.loads, schema='pg_catalog')\n"
        f"    r = await c.fetch({query!r})\n"
        "    print(json.dumps([dict(x) for x in r], default=str))\n"
        "    await c.close()\n"
        "asyncio.run(m())\n")
    proc = subprocess.run(
        [sys.executable, "-I", "-c", code], capture_output=True,
        env={**os.environ, "DATABASE_DSN": DSN})
    assert proc.returncode == 0, proc.stderr.decode()
    return json.loads(proc.stdout.decode())


def _reset_tables():
    _direct_sql("TRUNCATE wafer_measurements, batches, device_state")


def _insert_decided(device, seq, batch_no, median, status="RELEASED"):
    rows = _direct_sql(
        "INSERT INTO batches(batch_no, device_id, sha256, size_bytes, "
        "storage_path, accept_seq, status, median_nm, ref_mean_nm, "
        "deviation_nm, reference_ids, decided_at, decided_by) VALUES "
        f"('{batch_no}', '{device}', 'x', 1, '/tmp/x', {seq}, '{status}', "
        f"{median}, NULL, NULL, '[]'::jsonb, now(), 'test') RETURNING id")
    batch_id = rows[0]["id"]
    _direct_sql(
        "INSERT INTO wafer_measurements(batch_id, wafer_no, thickness_nm) "
        f"SELECT {batch_id}, 'W' || g, {int(median)} FROM generate_series(1, 5) g")
    return batch_id


def _parsed(median, prefix="W"):
    raw = make_csv([(f"{prefix}{i}", median) for i in range(1, 6)])
    return parse_thickness_csv(raw)


@pytest.mark.asyncio
async def test_decisions_committed_after_snapshot_do_not_leak(monkeypatch):
    """A commit mid-simulation is invisible; cutoff pins the snapshot."""
    _reset_tables()
    _insert_decided("EQ-SNAP", 1, "SNAP-1", 500)
    _insert_decided("EQ-SNAP", 2, "SNAP-2", 500)
    _insert_decided("EQ-SNAP", 3, "SNAP-3", 500)
    _insert_decided("EQ-SNAP", 4, "SNAP-4", 506, "QUARANTINED")
    # Fix reference_ids for SNAP-4 (ids are 1..4 in a fresh table).
    _direct_sql("UPDATE batches SET reference_ids='[1,2,3]'::jsonb "
                      "WHERE batch_no='SNAP-4'")

    import app.service as service_module
    real_validate = service_module.validate_replacement_wafer_set
    calls = {"n": 0}

    def _validate_then_commit(original, replacement):
        real_validate(original, replacement)
        if calls["n"] == 0:
            calls["n"] += 1
            # A different session commits a brand-new decision while the
            # simulation's REPEATABLE READ transaction is still open.
            next_id = _direct_sql(
                "INSERT INTO batches(batch_no, device_id, sha256, size_bytes, "
                "storage_path, accept_seq, status, median_nm, ref_mean_nm, "
                "deviation_nm, reference_ids, decided_at, decided_by) VALUES "
                "('SNAP-5', 'EQ-SNAP', 'x', 1, '/tmp/x', 5, 'QUARANTINED', "
                "700, 500, 200, '[2,3,4]'::jsonb, now(), 'concurrent') "
                "RETURNING id")
            _direct_sql(
                "INSERT INTO wafer_measurements(batch_id, wafer_no, thickness_nm) "
                f"SELECT {next_id[0]['id']}, 'W' || g, 700 "
                "FROM generate_series(1, 5) g")

    monkeypatch.setattr(service_module, "validate_replacement_wafer_set",
                        _validate_then_commit)

    pool = await create_pool()
    try:
        body = await simulate_revision(
            pool, device_id="EQ-SNAP", batch_no="SNAP-4",
            parsed=_parsed(505))
        # Snapshot ends at SNAP-4 even though SNAP-5 committed mid-flight.
        assert body["snapshot_cutoff_batch_no"] == "SNAP-4"
        assert body["snapshot_cutoff_accept_seq"] == 4
        assert body["evaluated_count"] == 1
        assert body["decided_after_target"] == 0
        assert [c["batch_no"] for c in body["changed_batches"]] == ["SNAP-4"]

        # A second simulation sees the new committed decision.
        body2 = await simulate_revision(
            pool, device_id="EQ-SNAP", batch_no="SNAP-4",
            parsed=_parsed(505))
        assert body2["snapshot_cutoff_batch_no"] == "SNAP-5"
        assert body2["decided_after_target"] == 1
        assert body2["evaluated_count"] == 2
        # SNAP-5 is inside the new snapshot and is replayed (its committed
        # verdict is not trusted) even though its verdict stays QUARANTINED.
        assert [c["batch_no"] for c in body2["changed_batches"]] == ["SNAP-4"]
        assert body2["target_result"]["simulated_status"] == "RELEASED"
        assert body2["snapshot_cutoff_batch_no"] == "SNAP-5"
    finally:
        await close_pool()


@pytest.mark.asyncio
async def test_pending_target_is_rejected():
    _reset_tables()
    _insert_decided("EQ-PEND", 1, "PEND-1", 500)
    _direct_sql(
        "INSERT INTO batches(batch_no, device_id, sha256, size_bytes, "
        "storage_path, accept_seq, status) VALUES "
        "('PEND-2', 'EQ-PEND', 'x', 1, '/tmp/x', 2, 'PENDING')")

    pool = await create_pool()
    try:
        with pytest.raises(RevisionRequestError) as excinfo:
            await simulate_revision(
                pool, device_id="EQ-PEND", batch_no="PEND-2",
                parsed=_parsed(500))
        assert excinfo.value.status_code == 409
    finally:
        await close_pool()


@pytest.mark.asyncio
async def test_fifty_followup_batches_allowed_but_fifty_one_refused():
    _reset_tables()
    # Batches 1..51 with the target at seq 1 -> exactly 50 follow-ups.
    values = ",".join(
        f"('LIM-{i}', 'EQ-LIM', 'x', 1, '/tmp/x', {i}, 'RELEASED', 500, "
        "NULL, NULL, '[]'::jsonb, now(), 'test')"
        for i in range(1, 52))
    _direct_sql(
        "INSERT INTO batches(batch_no, device_id, sha256, size_bytes, "
        "storage_path, accept_seq, status, median_nm, ref_mean_nm, "
        "deviation_nm, reference_ids, decided_at, decided_by) VALUES " + values)
    _direct_sql(
        "INSERT INTO wafer_measurements(batch_id, wafer_no, thickness_nm) "
        "SELECT b.id, 'W' || g, 500 FROM batches b, generate_series(1,5) g "
        "WHERE b.batch_no='LIM-1'")

    pool = await create_pool()
    try:
        body = await simulate_revision(
            pool, device_id="EQ-LIM", batch_no="LIM-1", parsed=_parsed(500))
        assert body["decided_after_target"] == 50
        assert body["snapshot_cutoff_batch_no"] == "LIM-51"

        # Add a 52nd decided batch: 51 follow-ups must be refused.
        _direct_sql(
            "INSERT INTO batches(batch_no, device_id, sha256, size_bytes, "
            "storage_path, accept_seq, status, median_nm, ref_mean_nm, "
            "deviation_nm, reference_ids, decided_at, decided_by) VALUES "
            "('LIM-52', 'EQ-LIM', 'x', 1, '/tmp/x', 52, 'RELEASED', 500, "
            "NULL, NULL, '[]'::jsonb, now(), 'test')")
        with pytest.raises(RevisionRequestError) as excinfo:
            await simulate_revision(
                pool, device_id="EQ-LIM", batch_no="LIM-1", parsed=_parsed(500))
        assert excinfo.value.status_code == 409
        assert excinfo.value.details["decided_after_target"] == 51
        assert excinfo.value.details["snapshot_cutoff_batch_no"] == "LIM-52"
    finally:
        await close_pool()
