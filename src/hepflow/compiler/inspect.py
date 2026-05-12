from __future__ import annotations

from typing import Any, Dict, Iterable, List, Tuple
import uproot


def _tree_specs_for_primary(ir: Dict[str, Any]) -> List[Tuple[str, str]]:
    """
    Return list of (stream_id, tree_path) that define the event-bearing trees used
    for nevents inference / partitioning. For zip_join primary, return all input trees.
    """
    streams = ir.get("streams") or {}
    primary = ir.get("primary_stream") or "events"
    if primary not in streams:
        raise ValueError(f"primary_stream '{primary}' not found in ir.streams")

    s = streams[primary]
    if s["kind"] == "root_tree":
        return [(primary, s["tree"])]

    if s["kind"] == "zip_join":
        out = []
        for inp in s.get("inputs") or []:
            sid = inp["stream"]
            sub = streams.get(sid)
            if not sub or sub.get("kind") != "root_tree":
                raise ValueError(
                    f"zip_join input '{sid}' must be a root_tree stream")
            out.append((sid, sub["tree"]))
        return out

    raise ValueError(
        f"Unsupported primary stream kind for entry counting: {s.get('kind')}")


def _num_entries(paths: Iterable[str]) -> dict[str, dict[str, int]]:
    """
    Wrapper around uproot.models.TTree.num_entries that returns
        {file_path: {tree_path: entries}}
    for each input file: tree path.
    """
    # uproot returns an iterator of a tuple: (file_path, tree_path, entries)
    raw = uproot.models.TTree.num_entries(paths)
    out: dict[str, dict[str, int]] = {}
    for root_file, tree, entries in raw:
        out.setdefault(root_file, {})[tree] = int(entries)

    return out


# TODO: split out into smaller functions that do not depend on `norm` - pass datasets + streams explicitly
# datasets, streams, primary_stream
def inspect_dataset_entries(norm: Dict[str, Any], ir: Dict[str, Any]) -> Dict[str, Any]:
    """
    Inspect all datasets and return an entry-count report suitable for:
      - filling missing dataset nevents
      - sanity checks (missing trees, zip_join mismatches)
      - writing build/dataset_entries.json

    Output:
    {
      "primary_stream": "...",
      "trees": [{"stream":"scatters","tree":"Scatters"}, ...],
      "datasets": {
        "<ds>": {
          "files": {
            "<file>": {"entries": 123, "by_tree": {"scatters":123, "truth":123}}
          },
          "total_entries": 123
        }
      }
    }
    """
    tree_specs = _tree_specs_for_primary(ir)  # [(stream_id, tree)]
    out: Dict[str, Any] = {
        "primary_stream": ir.get("primary_stream"),
        "trees": [{"stream": sid, "tree": tree} for sid, tree in tree_specs],
        "datasets": {},
    }

    # ---- Single batched query across all datasets/files/trees ----
    all_paths: List[str] = []
    for ds in norm["data"]["datasets"]:
        for f in ds["files"]:
            for _, tree in tree_specs:
                all_paths.append(f"{f}:{tree}")

    counts = _num_entries(all_paths)

    # ---- Per-dataset validation and aggregation ----
    for ds in norm["data"]["datasets"]:
        name = ds["name"]
        files = ds["files"]

        # Build per-file report and validate
        files_obj: Dict[str, Any] = {}
        first_tree = tree_specs[0][1]  # the tree path of first spec

        for f in files:
            if f not in counts:
                raise ValueError(
                    f"Missing entry info for dataset='{name}' file='{f}'")

            # Ensure all requested trees exist for this file
            missing = [tree for _, tree in tree_specs if tree not in counts[f]]
            if missing:
                raise ValueError(
                    f"Missing trees for dataset='{name}' file='{f}': {missing}"
                )

            by_tree = {sid: counts[f][tree] for sid, tree in tree_specs}
            entries = counts[f][first_tree]

            files_obj[f] = {
                "entries": int(entries),
                "by_tree": {sid: int(v) for sid, v in by_tree.items()},
            }

            # zip_join alignment check per file (if multiple trees)
            if len(tree_specs) > 1:
                vals = [(sid, tree, counts[f][tree])
                        for sid, tree in tree_specs]
                only_counts = [v[-1] for v in vals]
                if len(set(only_counts)) != 1:
                    raise ValueError(
                        f"zip_join entry mismatch dataset='{name}' file='{f}': {vals}"
                    )

        total_entries = int(sum(v["entries"] for v in files_obj.values()))
        out["datasets"][name] = {"files": files_obj,
                                 "total_entries": total_entries}

    return out


def fill_missing_nevents_from_inspection(norm: Dict[str, Any], inspection: Dict[str, Any]) -> None:
    for ds in norm["data"]["datasets"]:
        if ds.get("nevents") is None:
            name = ds["name"]
            total = inspection["datasets"][name]["total_entries"]
            ds["nevents"] = str(total)
