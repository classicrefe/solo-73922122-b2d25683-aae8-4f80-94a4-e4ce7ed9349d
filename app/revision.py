"""Pure decision-replay logic for the thickness-revision impact simulation.

The simulation never touches the database or the filesystem.  It receives the
committed decisions of one device as an immutable snapshot and replays the
original decision rule from the revised target batch onwards:

* the first ``baseline_count`` batches are released unconditionally;
* every later batch is compared with the mean of the medians of the most
  recent ``reference_count`` *simulated* releases;
* a deviation strictly greater than ``threshold_nm`` is quarantined.

Because the replay uses the previous batch's *simulated* verdict when building
the next reference window, a flipped verdict propagates to later batches
exactly like it would have during real processing.  Only the target batch's
median is revised; every other batch keeps its committed median.

Batches strictly before the target are taken as committed facts: their
verdicts seed the reference window but are never reported as changed.
"""
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Mapping, Sequence, Tuple

Status = str


class WaferSetMismatch(ValueError):
    """The replacement CSV does not cover exactly the target batch's wafers."""

    def __init__(self, missing: Iterable[str], extra: Iterable[str]):
        self.missing = sorted(missing)
        self.extra = sorted(extra)
        parts = []
        if self.missing:
            parts.append(f"缺少晶圆号 {self.missing}")
        if self.extra:
            parts.append(f"多出晶圆号 {self.extra}")
        super().__init__(
            "替代 CSV 的晶圆号必须与原批完全一致（仅厚度可改）/ "
            "replacement CSV wafer numbers must match the original batch: "
            + "; ".join(parts))


def validate_replacement_wafer_set(
    original_wafer_nos: Iterable[str],
    replacement_rows: Sequence[Tuple[str, int]],
) -> None:
    """Require the same set of wafer numbers (order is irrelevant)."""
    original = set(original_wafer_nos)
    replacement = {wafer for wafer, _ in replacement_rows}
    missing = original - replacement
    extra = replacement - original
    if missing or extra:
        raise WaferSetMismatch(missing, extra)


def _ref_nos(ref_ids: Sequence[int], by_id: Mapping[int, Mapping[str, Any]]
             ) -> List[str]:
    return [by_id[i]["batch_no"] for i in ref_ids if i in by_id]


def compute_revision(
    decided: Sequence[Mapping[str, Any]],
    *,
    target_seq: int,
    revised_median_nm: float,
    baseline_count: int,
    reference_count: int,
    threshold_nm: float,
) -> Dict[str, Any]:
    """Replay decisions from ``target_seq`` using the revised target median.

    ``decided`` must hold every committed decision of the device in
    ``accept_seq`` order (a contiguous 1..N prefix).  Items are the snapshot
    rows assembled by :func:`app.service.simulate_revision`.
    """
    by_id: Dict[int, Mapping[str, Any]] = {int(d["id"]): d for d in decided}
    target = next((d for d in decided if int(d["accept_seq"]) == target_seq), None)
    if target is None:
        raise ValueError(f"target seq {target_seq} not present in snapshot")

    # Simulated median per batch id: committed values except for the target.
    sim_median: Dict[int, float] = {
        int(d["id"]): float(d["median_nm"]) for d in decided
    }
    target_id = int(target["id"])
    sim_median[target_id] = float(revised_median_nm)

    # Rolling window of *simulated* released batch ids, most recent last.
    window: List[int] = []
    changed: List[Dict[str, Any]] = []
    target_view: Dict[str, Any] = {}

    for d in decided:  # already ordered by accept_seq
        seq = int(d["accept_seq"])
        batch_id = int(d["id"])

        if seq < target_seq:
            # Committed prefix: facts, not part of the simulation.
            sim_status: Status = d["status"]
            sim_ref_ids: List[int] = [int(i) for i in (d["reference_ids"] or [])]
            sim_ref_mean = d["ref_mean_nm"]
            sim_deviation = d["deviation_nm"]
            sim_median_value = float(d["median_nm"])
        else:
            sim_median_value = sim_median[batch_id]
            if seq <= baseline_count:
                sim_status, sim_ref_ids = "RELEASED", []
                sim_ref_mean = sim_deviation = None
            else:
                sim_ref_ids = window[-reference_count:]
                if not sim_ref_ids:
                    # Defensive, mirrors the worker's "no-reference" release.
                    sim_status, sim_ref_mean, sim_deviation = "RELEASED", None, None
                else:
                    sim_ref_mean = (
                        sum(sim_median[i] for i in sim_ref_ids) / len(sim_ref_ids))
                    sim_deviation = abs(sim_median_value - sim_ref_mean)
                    sim_status = (
                        "QUARANTINED" if sim_deviation > threshold_nm else "RELEASED"
                    )

        if sim_status == "RELEASED":
            window = (window + [batch_id])[-reference_count:]

        actual_ref_ids = [int(i) for i in (d["reference_ids"] or [])]
        view = {
            "batch_no": d["batch_no"],
            "accept_seq": seq,
            "actual_status": d["status"],
            "simulated_status": sim_status,
            "actual_median_nm": d["median_nm"],
            "simulated_median_nm": sim_median_value,
            "actual_reference_batch_nos": _ref_nos(actual_ref_ids, by_id),
            "simulated_reference_batch_nos": _ref_nos(sim_ref_ids, by_id),
            "actual_reference_mean_nm": d["ref_mean_nm"],
            "simulated_reference_mean_nm": sim_ref_mean,
            "actual_deviation_nm": d["deviation_nm"],
            "simulated_deviation_nm": sim_deviation,
        }
        if seq == target_seq:
            target_view = view
        if seq >= target_seq and sim_status != d["status"]:
            changed.append(view)

    if not target_view:  # pragma: no cover - guarded by the caller
        raise ValueError(f"target seq {target_seq} not present in snapshot")

    return {
        "target": target_view,
        "evaluated_count": len(decided) - (target_seq - 1),
        "changed_count": len(changed),
        "changed_batches": changed,
    }
