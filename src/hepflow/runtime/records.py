from __future__ import annotations

from typing import Any


def _is_opaque_branch_name(branch: str) -> bool:
    """
    Branch names like 'ss./ss.nSingleScatters' contain './' and should be treated as *opaque*.
    We should NOT try to split them as paths.
    """
    return "./" in branch or "../" in branch


def branch_to_segments(branch: str) -> list[str] | None:
    """
    Convert a branch reference into nested record segments.

    Supports:
      - ROOT subbranches using '/'   e.g. 'L1Upgrade/jetEt' -> ['L1Upgrade','jetEt']
      - dotted objects              e.g. 'Muon.pt'         -> ['Muon','pt']
      - (legacy) join prefixes '.'  e.g. 'l1.L1Upgrade/jetEt' -> ['l1','L1Upgrade','jetEt']

    Returns None if the branch should be treated as opaque (must match a leaf name exactly),
    e.g. LZ 'ss./ss.nSingleScatters'.
    """
    b = str(branch).strip()
    if not b:
        return None

    # Treat odd legacy names as opaque (e.g. LZ 'ss./ss.nSingleScatters')
    if "./" in b or "../" in b:
        return None
    if _is_opaque_branch_name(b):
        return None

    dot_parts = [p for p in b.split(".") if p]
    segs: list[str] = []
    for p in dot_parts:
        segs.extend([x for x in p.split("/") if x])
    return segs or None


def get_field_by_branch(rec: Any, branch: str) -> Any:
    """
    Retrieve a field from a nested awkward record by either:
      1) exact leaf name match (fast path)
      2) path traversal using branch_to_segments()

    Raises KeyError with helpful diagnostics.
    """
    if not hasattr(rec, "fields"):
        raise KeyError(f"Not a record array; cannot access branch={branch!r}")

    # 1) exact match (covers opaque names and already-flat fields)
    if branch in rec.fields:
        return rec[branch]

    # 2) path traversal
    segs = branch_to_segments(branch)
    if segs is None:
        # opaque branch that isn't present directly
        raise KeyError(
            f"Branch {branch!r} not found as direct field. "
            f"Available top-level fields: {list(rec.fields)[:50]}"
        )

    cur: Any = rec
    for s in segs:
        if not hasattr(cur, "fields") or s not in cur.fields:
            avail = list(getattr(cur, "fields", []))
            raise KeyError(
                f"Missing segment {s!r} while resolving branch {branch!r}. "
                f"Available at this level: {avail[:50]}"
            )
        cur = cur[s]
    return cur
