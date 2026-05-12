from typing import Any, Dict, Iterable
import boost_histogram as bh


def merge_hists(items: Iterable[bh.Histogram]) -> bh.Histogram:
    it = iter(items)
    acc = next(it)
    for h in it:
        acc = acc + h
    return acc


def merge_cutflows(items: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
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
