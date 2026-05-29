from __future__ import annotations

from collections.abc import Iterable
from typing import Any


# TODO: artifact merging should become package-owned via merge strategies.
# fasthep-flow should not contain histogram-specific logic long term.
def merge_hists(items: Iterable[Any]) -> Any:
    it = iter(items)
    acc = next(it)
    for h in it:
        acc = acc + h
    return acc


# TODO: artifact merging should become package-owned via merge strategies.
# fasthep-flow should not contain cutflow-specific logic long term.
def merge_cutflows(items: Iterable[dict[str, Any]]) -> dict[str, Any]:
    # Intermediate cutflow products are merged by stable cut node id. The
    # persisted artifact is converted to the canonical graph representation
    # later, once the plan selection topology is available.
    out_by_name: dict[str, dict[str, Any]] = {}
    for cf in items:
        for row in cf.get("cuts", []):
            name = row["name"]
            tgt = out_by_name.setdefault(name, _empty_cutflow_row(row))
            for field in ("n_in", "n_out"):
                tgt[field] += int(row.get(field, row.get("n", 0)))
            for field in ("sumw_in", "sumw_out"):
                tgt[field] += float(row.get(field, row.get("sumw", row.get("n", 0))))
            for field in ("sumw2_in", "sumw2_out"):
                tgt[field] += float(row.get(field, row.get("sumw2", row.get("n", 0))))
            tgt["n"] = tgt["n_out"]
            tgt["sumw"] = tgt["sumw_out"]
            tgt["sumw2"] = tgt["sumw2_out"]
    return {"cuts": list(out_by_name.values())}


def _empty_cutflow_row(row: dict[str, Any]) -> dict[str, Any]:
    merged = {
        "name": row["name"],
        "n": 0,
        "sumw": 0.0,
        "sumw2": 0.0,
        "n_in": 0,
        "n_out": 0,
        "sumw_in": 0.0,
        "sumw_out": 0.0,
        "sumw2_in": 0.0,
        "sumw2_out": 0.0,
    }
    for field in ("selection", "index", "label", "expr", "kind"):
        if field in row:
            merged[field] = row[field]
    return merged
