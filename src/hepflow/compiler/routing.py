from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, Mapping, Optional, Sequence, Set, Tuple


class RoutingError(ValueError):
    pass


def resolve_branch_ref(
    *,
    streams: Dict[str, Dict[str, Any]],
    stream_id: str,
    branch: str,
) -> Tuple[str, str]:
    """
    Resolve (stream_id, branch) to a *leaf* root_tree stream + branch.

    Supports "virtual" zip_join streams where branch is prefixed by join input prefix:
      stream_id="events" (zip_join with inputs [{stream:"l1", prefix:"l1"}, ...])
      branch="l1.L1Upgrade.jetEt"
      -> ("l1", "L1Upgrade.jetEt")

    If stream_id already refers to root_tree: returns as-is.
    """
    if stream_id not in streams:
        raise RoutingError(f"Unknown stream_id {stream_id!r}")

    s = streams[stream_id]
    kind = s.get("kind")

    if kind == "root_tree":
        return stream_id, branch

    if kind == "zip_join":
        if "." not in branch:
            raise RoutingError(
                f"Branch {branch!r} for zip_join stream {stream_id!r} must be prefixed "
                f"with one of the join input prefixes."
            )
        pref, rest = branch.split(".", 1)

        for inp in (s.get("inputs") or []):
            if inp.get("prefix") == pref:
                leaf_stream = inp.get("stream")
                if not leaf_stream:
                    raise RoutingError(
                        f"zip_join input missing stream for prefix {pref!r}")
                # recurse in case of nested virtual streams later
                return resolve_branch_ref(streams=streams, stream_id=leaf_stream, branch=rest)

        raise RoutingError(
            f"Prefix {pref!r} not found among inputs of zip_join {stream_id!r}"
        )

    # Future: other virtual kinds (union, concat, etc.)
    raise RoutingError(f"Unsupported stream kind {kind!r} for routing")


def is_virtual_stream(streams: Mapping[str, Mapping[str, Any]], stream_id: str) -> bool:
    return (streams.get(stream_id) or {}).get("kind") == "zip_join"


def is_leaf_stream(streams: Mapping[str, Mapping[str, Any]], stream_id: str) -> bool:
    return (streams.get(stream_id) or {}).get("kind") == "root_tree"


def _join_inputs(
    streams: Mapping[str, Mapping[str, Any]],
    join_id: str,
) -> Sequence[Mapping[str, Any]]:
    s = streams.get(join_id) or {}
    if s.get("kind") != "zip_join":
        return ()
    inputs = s.get("inputs") or []
    if not isinstance(inputs, list):
        return ()
    # expected entries like {"stream": "l1", "prefix": "l1"}
    return tuple(x for x in inputs if isinstance(x, dict))


def route_join_branch(
    *,
    streams: Mapping[str, Mapping[str, Any]],
    join_id: str,
    branch: str,
    on_missing_prefix: str = "error",  # "error" | "unresolved"
) -> Tuple[Optional[str], Optional[str]]:
    """
    Given a virtual join stream `join_id` and a branch like "reco.Jet.eta",
    route it to the leaf stream ("reco", "Jet.eta") based on join inputs prefixes.

    Returns (leaf_stream_id, leaf_branch) or (None, None) if unresolved and on_missing_prefix="unresolved".
    """
    inputs = _join_inputs(streams, join_id)
    if not inputs:
        if on_missing_prefix == "unresolved":
            return None, None
        raise RoutingError(
            f"join '{join_id}' has no inputs; cannot route branch {branch!r}")

    if not isinstance(branch, str) or not branch:
        if on_missing_prefix == "unresolved":
            return None, None
        raise RoutingError(f"branch must be non-empty string; got {branch!r}")

    # We require prefix-based addressing inside joins: "<prefix>.<rest>"
    if "." not in branch:
        if on_missing_prefix == "unresolved":
            return None, None
        raise RoutingError(
            f"join '{join_id}' branch {branch!r} is missing prefix. "
            "Expected '<prefix>.<branch>' (e.g. 'reco.Jet.eta')."
        )

    pref, rest = branch.split(".", 1)
    for inp in inputs:
        p = inp.get("prefix")
        sid = inp.get("stream")
        if p == pref and isinstance(sid, str) and sid:
            if not is_leaf_stream(streams, sid):
                if on_missing_prefix == "unresolved":
                    return None, None
                raise RoutingError(
                    f"join '{join_id}' input stream '{sid}' is not a leaf root_tree stream; got kind={(streams.get(sid) or {}).get('kind')!r}"
                )
            return sid, rest

    # prefix did not match any join input
    if on_missing_prefix == "unresolved":
        return None, None
    known = [str(i.get("prefix"))
             for i in inputs if i.get("prefix") is not None]
    raise RoutingError(
        f"join '{join_id}' could not route branch {branch!r}: "
        f"prefix {pref!r} not found in join prefixes {known}"
    )


def route_required_branches_by_stream(
    *,
    streams: Mapping[str, Mapping[str, Any]],
    required_branches_by_stream: Mapping[str, Set[str]],
    on_missing_prefix: str = "error",  # "error" | "unresolved"
) -> Tuple[Dict[str, Set[str]], Set[Tuple[str, str]]]:
    """
    Route required branches from potentially-virtual stream ids onto leaf root_tree streams.

    Input:
      required_branches_by_stream may contain keys that are zip_join stream ids,
      with branches containing join prefixes (e.g. "reco.Jet.eta").

    Output:
      (leaf_required, unresolved_pairs)
      where unresolved_pairs contains (stream_id, branch) pairs that could not be routed.
    """
    leaf_required: Dict[str, Set[str]] = defaultdict(set)
    unresolved: Set[Tuple[str, str]] = set()

    for sid, branches in required_branches_by_stream.items():
        kind = (streams.get(sid) or {}).get("kind")

        # leaf stream: keep as-is
        if kind == "root_tree":
            for b in branches:
                if isinstance(b, str) and b:
                    leaf_required[sid].add(b)
            continue

        # virtual join: route each branch using prefix mapping
        if kind == "zip_join":
            for b in branches:
                try:
                    leaf_sid, leaf_b = route_join_branch(
                        streams=streams,
                        join_id=sid,
                        branch=b,
                        on_missing_prefix=on_missing_prefix,
                    )
                except RoutingError:
                    if on_missing_prefix == "unresolved":
                        unresolved.add((sid, str(b)))
                        continue
                    raise
                if leaf_sid and leaf_b:
                    leaf_required[leaf_sid].add(leaf_b)
                else:
                    unresolved.add((sid, str(b)))
            continue

        # unknown kind: cannot route
        for b in branches:
            unresolved.add((sid, str(b)))

    return dict(leaf_required), unresolved


def _join_prefix_map(streams: Dict[str, Dict[str, Any]], join_id: str) -> Dict[str, str]:
    """
    For a join stream like:
      streams["events"] = {"kind":"zip_join","inputs":[{"stream":"l1","prefix":"l1"}, ...]}
    return {"l1": "l1", "reco": "reco"} mapping prefix->physical_stream_id.
    """
    s = streams.get(join_id) or {}
    if s.get("kind") != "zip_join":
        return {}
    out: Dict[str, str] = {}
    for inp in s.get("inputs") or []:
        pref = inp.get("prefix")
        sid = inp.get("stream")
        if isinstance(pref, str) and isinstance(sid, str):
            out[pref] = sid
    return out


def rewrite_fieldmap_for_joins(
    *,
    fieldmap: Dict[str, Dict[str, str]],
    streams: Dict[str, Dict[str, Any]],
) -> Dict[str, Dict[str, str]]:
    """
    Rewrite fieldmap entries that reference a join stream:
      {stream: "events", branch: "l1.L1Upgrade/jetEt"}
    into:
      {stream: "l1", branch: "L1Upgrade/jetEt"}

    Rules:
    - Only applies if fieldmap entry.stream is a zip_join stream.
    - Requires branch to start with "<prefix>." where prefix is one of join inputs' prefixes.
    - Leaves non-join fieldmap entries unchanged.
    """
    out: Dict[str, Dict[str, str]] = {}

    for alias, spec in (fieldmap or {}).items():
        if not isinstance(spec, dict):
            continue
        s = spec.get("stream")
        b = spec.get("branch")
        if not (isinstance(s, str) and isinstance(b, str)):
            out[alias] = dict(spec)
            continue

        # only rewrite if stream is a join
        if (streams.get(s) or {}).get("kind") != "zip_join":
            out[alias] = dict(spec)
            continue

        pref_map = _join_prefix_map(streams, s)

        # require "<prefix>.<rest>"
        if "." not in b:
            raise ValueError(
                f"fieldmap.{alias}: join-scoped branch must be '<prefix>.<branch>', got {b!r}"
            )
        prefix, rest = b.split(".", 1)
        if prefix not in pref_map:
            raise ValueError(
                f"fieldmap.{alias}: unknown join prefix {prefix!r} for join stream {s!r}. "
                f"Known: {sorted(pref_map.keys())}"
            )

        out[alias] = {"stream": pref_map[prefix], "branch": rest}

    return out
