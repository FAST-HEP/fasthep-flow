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
    # Assume structure: {"cuts":[{"name":..., "n":..., "sumw":..., "sumw2":...}, ...]}
    # Merge by cut name and field sum
    out_by_name = {}
    for cf in items:
        for row in cf.get("cuts", []):
            name = row["name"]
            tgt = out_by_name.setdefault(
                name, {"name": name, "n": 0, "sumw": 0.0, "sumw2": 0.0})
            tgt["n"] += int(row.get("n", 0))
            tgt["sumw"] += float(row.get("sumw", row.get("n", 0)))
            tgt["sumw2"] += float(row.get("sumw2", row.get("n", 0)))
    return {"cuts": list(out_by_name.values())}
