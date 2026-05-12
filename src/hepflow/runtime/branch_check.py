# hepflow/runtime/branch_check.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from hepflow.runtime.records import get_field_by_branch
from hepflow.runtime.stream_readers import read_root_tree


@dataclass(frozen=True)
class BranchCheckResult:
    stream_id: str
    tree: str
    ok: List[str]
    missing: List[dict[str, Any]]  # {branch, error}


def check_required_inputs_for_file(
    *,
    plan: dict[str, Any],
    file_path: str,
    streams: Optional[list[str]] = None,
    start: int = 0,
    stop: int = 1,
) -> list[BranchCheckResult]:
    """
    For each stream in plan.required_inputs (optionally filtered), try to read the
    required branches from the referenced ROOT tree, then resolve each branch using
    get_field_by_branch().

    - Uses a tiny entry range by default for speed.
    - Returns per-stream results with missing details.
    """
    required_inputs = plan.get("required_inputs") or {}
    stream_defs = plan.get("streams") or {}

    # Determine which streams to check
    to_check = list(required_inputs.keys())
    if streams is not None:
        wanted = set(streams)
        to_check = [sid for sid in to_check if sid in wanted]

    out: list[BranchCheckResult] = []

    for sid in to_check:
        ri = required_inputs.get(sid) or {}
        sdef = stream_defs.get(sid) or {}

        kind = str(ri.get("kind") or sdef.get("kind") or "")
        if kind != "root_tree":
            # Only validate physical trees here; virtual streams (zip_join) aren’t read directly.
            continue

        tree = str(ri.get("tree") or sdef.get("tree") or "")
        branches = list(ri.get("branches") or [])
        if not tree:
            out.append(
                BranchCheckResult(
                    stream_id=sid,
                    tree=tree,
                    ok=[],
                    missing=[{"branch": "<tree>",
                              "error": "missing tree name"}],
                )
            )
            continue

        # Read only required branches from uproot
        raw = read_root_tree(file_path, tree, branches, start, stop)

        ok: list[str] = []
        missing: list[dict[str, Any]] = []

        for br in branches:
            try:
                _ = get_field_by_branch(raw, str(br))
                ok.append(str(br))
            except Exception as e:
                missing.append(
                    {"branch": str(br), "error": f"{type(e).__name__}: {e}"})

        out.append(BranchCheckResult(stream_id=sid,
                   tree=tree, ok=ok, missing=missing))

    return out
