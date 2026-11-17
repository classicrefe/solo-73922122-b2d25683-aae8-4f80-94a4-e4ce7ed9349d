"""Pure-function tests for the decision replay (no database required)."""
from __future__ import annotations

import pytest

from app.revision import WaferSetMismatch, compute_revision, validate_replacement_wafer_set


def _row(seq, batch_no, status, median, refs=(), ref_mean=None, dev=None):
    return {
        "id": seq,
        "batch_no": batch_no,
        "accept_seq": seq,
        "status": status,
        "median_nm": median,
        "ref_mean_nm": ref_mean,
        "deviation_nm": dev,
        "reference_ids": list(refs),
    }


def _snapshot(medians, statuses):
    snap = []
    for i, (median, status) in enumerate(zip(medians, statuses), start=1):
        refs = list(range(max(1, i - 3), i)) if i > 3 else []
        snap.append(_row(i, f"B{i}", status, median))
        snap[-1]["reference_ids"] = [r for r in refs
                                     if i > 3 and snap[r - 1]["status"] == "RELEASED"][-3:]
    return snap


def test_threshold_is_strictly_greater_than():
    snap = _snapshot([500, 500, 500, 506], ["RELEASED"] * 3 + ["QUARANTINED"])
    out = compute_revision(snap, target_seq=4, revised_median_nm=505.0,
                           baseline_count=3, reference_count=3, threshold_nm=5.0)
    assert out["target"]["simulated_status"] == "RELEASED"
    assert out["target"]["simulated_deviation_nm"] == pytest.approx(5.0)
    # 505.0001 would be more than 5nm from the 500 mean.
    out2 = compute_revision(snap, target_seq=4, revised_median_nm=505.0001,
                            baseline_count=3, reference_count=3, threshold_nm=5.0)
    assert out2["target"]["simulated_status"] == "QUARANTINED"


def test_quarantined_batches_before_target_never_enter_window():
    # B4 was quarantined; it must not be part of B5's reference window.
    snap = [
        _row(1, "B1", "RELEASED", 500),
        _row(2, "B2", "RELEASED", 500),
        _row(3, "B3", "RELEASED", 500),
        _row(4, "B4", "QUARANTINED", 600, refs=[1, 2, 3], ref_mean=500, dev=100),
        _row(5, "B5", "RELEASED", 501, refs=[1, 2, 3], ref_mean=500, dev=1),
    ]
    out = compute_revision(snap, target_seq=5, revised_median_nm=501.0,
                           baseline_count=3, reference_count=3, threshold_nm=5.0)
    assert out["target"]["simulated_reference_batch_nos"] == ["B1", "B2", "B3"]


def test_cascade_through_reference_window():
    # Committed history: B4 (506) quarantined, B5 (504) released against
    # B1..B3, B6 (510) quarantined against B2,B3,B5.
    snap = [
        _row(1, "B1", "RELEASED", 500),
        _row(2, "B2", "RELEASED", 500),
        _row(3, "B3", "RELEASED", 500),
        _row(4, "B4", "QUARANTINED", 506, refs=[1, 2, 3], ref_mean=500, dev=6),
        _row(5, "B5", "RELEASED", 504, refs=[1, 2, 3], ref_mean=500, dev=4),
        _row(6, "B6", "QUARANTINED", 510, refs=[2, 3, 5], ref_mean=501.33, dev=8.67),
    ]
    # Revising B4 to 495 flips it to RELEASED.  B4 then enters B5's reference
    # window, which lowers B5's mean and flips B5 to QUARANTINED instead.
    out = compute_revision(snap, target_seq=4, revised_median_nm=495.0,
                           baseline_count=3, reference_count=3, threshold_nm=5.0)
    changed = {c["batch_no"]: c for c in out["changed_batches"]}
    assert set(changed) == {"B4", "B5"}
    assert changed["B4"]["simulated_status"] == "RELEASED"
    assert changed["B5"]["simulated_status"] == "QUARANTINED"

    # B5 now uses the *simulated* window (B4 included), not its committed one.
    assert changed["B5"]["actual_reference_batch_nos"] == ["B1", "B2", "B3"]
    assert changed["B5"]["simulated_reference_batch_nos"] == ["B2", "B3", "B4"]
    assert changed["B5"]["simulated_reference_mean_nm"] == pytest.approx(
        (500 + 500 + 495) / 3)
    assert changed["B5"]["simulated_deviation_nm"] == pytest.approx(
        abs(504 - (500 + 500 + 495) / 3))

    # B6 keeps its committed verdict but is replayed against the new window:
    # B5 is now quarantined, so B6 sees B2,B3,B4 (mean 498.33, dev 11.67).
    assert "B6" not in changed
    assert out["evaluated_count"] == 3


def test_prefix_changes_are_not_reported():
    snap = _snapshot([500, 500, 500, 500], ["RELEASED"] * 4)
    out = compute_revision(snap, target_seq=4, revised_median_nm=500.0,
                           baseline_count=3, reference_count=3, threshold_nm=5.0)
    assert out["changed_batches"] == []
    assert out["evaluated_count"] == 1


def test_wafer_set_validation():
    validate_replacement_wafer_set(["W1", "W2"], [("W2", 1), ("W1", 2)])
    with pytest.raises(WaferSetMismatch) as excinfo:
        validate_replacement_wafer_set(["W1", "W2"], [("W1", 1)])
    assert excinfo.value.missing == ["W2"]
    with pytest.raises(WaferSetMismatch) as excinfo:
        validate_replacement_wafer_set(["W1"], [("W1", 1), ("W9", 2)])
    assert excinfo.value.extra == ["W9"]
