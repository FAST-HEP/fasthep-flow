from __future__ import annotations

from typing import Any

from hepflow.model.defaults import DEFAULT_PRIMARY_STREAM_ID
from hepflow.runtime.records import get_field_by_branch


def _awkward():
    import awkward as ak  # noqa: PLC0415

    return ak


def _uproot():
    import uproot  # noqa: PLC0415

    return uproot


def _is_opaque_for_uproot_arrays(expr: str) -> bool:
    """
    uproot's TTree.arrays(expressions=[...]) uses a Python-expression parser.
    Names like 'ss./ss.xyCorrectedS2Area_phd' are invalid syntax and must be read as raw branches.
    """
    s = str(expr)
    # conservative; covers your LZ case well
    return ("./" in s) or ("/" in s and "." in s)


def read_root_tree(
    file_path: str, tree: str, branches: list[str], start: int, stop: int
):
    ak = _awkward()
    with _uproot().open(file_path) as f:
        t = f[tree]

        # Split into "safe for arrays" and "must be raw-branch read"
        safe: list[str] = []
        opaque: list[str] = []
        for b in branches:
            (opaque if _is_opaque_for_uproot_arrays(b) else safe).append(b)

        out: dict[str, Any] = {}

        # Fast path for normal names
        if safe:
            arrs = t.arrays(safe, entry_start=start, entry_stop=stop, library="ak")
            # uproot returns a record; merge fields into out
            for k in arrs.fields:
                out[k] = arrs[k]

        # Robust path for weird names (bypass expression parsing)
        for b in opaque:
            try:
                # TBranchElement supports .array in uproot
                out[b] = t[b].array(entry_start=start, entry_stop=stop, library="ak")
            except Exception:
                # fallback: some uproot versions prefer arrays([b]) even if expression parsing fails
                # but this should still work via TBranch access for LZ-style names.
                out[b] = t[b].array(entry_start=start, entry_stop=stop, library="ak")

        return ak.zip(out, depth_limit=1)


def _flatten_record_fields(
    rec: Any, *, prefix: str = "", sep: str = "."
) -> dict[str, Any]:
    """
    Flatten an awkward RecordArray (possibly nested) into a flat dict:
      {"A.B": ..., "A.C": ...}
    This normalizes uproot's nested representation for dotted branch names.
    """
    out: dict[str, Any] = {}

    # RecordArray: has .fields and supports rec[field]
    if hasattr(rec, "fields"):
        for k in rec.fields:
            key = f"{prefix}{sep}{k}" if prefix else str(k)
            v = rec[k]
            # If v itself is a record-like, recurse
            if hasattr(v, "fields"):
                out.update(_flatten_record_fields(v, prefix=key, sep=sep))
            else:
                out[key] = v
        return out

    # Not record-like; nothing to flatten
    if prefix:
        out[prefix] = rec
    return out


def flatten_record(rec: Any, *, sep: str = ".") -> Any:
    """
    Convert nested awkward records into a flat record with keys containing sep.
    If rec is not record-like, returns it unchanged.
    """
    if not hasattr(rec, "fields"):
        return rec
    flat = _flatten_record_fields(rec, sep=sep)
    return _awkward().zip(flat, depth_limit=1)


def prefix_record(rec: Any, prefix: str):
    # rec is a *flat* awkward record array / mapping
    return _awkward().zip({f"{prefix}.{k}": rec[k] for k in rec.fields}, depth_limit=1)


def _lift_join_aliases(plan: dict[str, Any], *, stream_id: str, merged: Any) -> Any:
    """
    For a zip_join stream (e.g. 'events'), lift aliases defined on its input streams
    up to the merged/top-level record.

    Assumes merged has fields like 'l1', 'reco', where each is already a record
    containing the per-stream alias fields (because read_stream() projects aliases
    in the root_tree case).
    """
    streams = plan.get("streams") or {}
    s = streams.get(stream_id) or {}
    if s.get("kind") != "zip_join":
        return merged

    fieldmap = plan.get("fieldmap") or {}

    alias_cols: dict[str, Any] = {}
    collisions: set[str] = set()

    for inp in s.get("inputs") or []:
        sid = inp["stream"]  # e.g. "l1"
        pref = inp["prefix"]  # e.g. "l1" (namespace key in merged)

        # aliases defined for that physical stream
        for alias, spec in fieldmap.items():
            if spec.get("stream") != sid:
                continue

            if alias in alias_cols:
                collisions.add(alias)
                continue

            # Because root_tree read_stream already projected aliases, they exist as:
            # merged[pref][alias]
            try:
                alias_cols[alias] = merged[pref][alias]
            except Exception as e:
                raise ValueError(
                    f"Could not lift alias {alias!r} from join stream {stream_id!r}: "
                    f"expected merged[{pref!r}][{alias!r}] to exist. "
                    f"(input stream={sid!r})"
                ) from e

    if collisions:
        raise ValueError(
            f"Alias name collision while lifting aliases into join stream '{stream_id}': "
            f"{sorted(collisions)}. Please rename aliases to be unique across join inputs."
        )

    if not alias_cols:
        return merged

    # keep namespaces + add lifted alias columns
    base_cols = {k: merged[k] for k in merged.fields}
    overlap = set(base_cols) & set(alias_cols)
    if overlap:
        raise ValueError(
            f"Lifted aliases would overwrite existing join fields in '{stream_id}': {sorted(overlap)}"
        )

    return _awkward().zip({**base_cols, **alias_cols}, depth_limit=1)


def _project_aliases(plan, stream_id, raw):
    fieldmap = plan.get("fieldmap") or {}
    used_aliases = (plan.get("used_field_aliases") or {}).get(stream_id) or []
    aliases = {
        alias: spec["branch"]
        for alias, spec in fieldmap.items()
        if spec.get("stream") == stream_id and alias in used_aliases
    }
    if not aliases:
        return raw

    base_cols = {k: raw[k] for k in raw.fields}

    alias_cols = {}
    missing = []
    for alias, branch in aliases.items():
        try:
            alias_cols[alias] = get_field_by_branch(raw, branch)
        except KeyError:
            missing.append({"alias": alias, "branch": branch})

    if missing:
        raise ValueError(
            f"Missing branches while projecting aliases for stream '{stream_id}'. "
            f"Missing: {missing[:20]}{' ...' if len(missing) > 20 else ''}"
        )

    # Guard against accidental overwrite (rare but worth being explicit)
    overlap = set(base_cols) & set(alias_cols)
    if overlap:
        raise ValueError(
            f"Alias projection would overwrite existing fields in stream '{stream_id}': {sorted(overlap)}"
        )

    return _awkward().zip({**base_cols, **alias_cols}, depth_limit=1)


def read_stream(plan: dict, stream_id: str, file_path: str, start: int, stop: int):
    streams = plan["streams"]
    s = streams[stream_id]
    required_inputs = plan.get("required_inputs", {})

    if s["kind"] == "root_tree":
        tree = s["tree"]
        ri = required_inputs.get(stream_id) or {}
        branches = ri.get("branches", [])
        raw = read_root_tree(file_path, tree, branches, start, stop)
        # IMPORTANT: do NOT flatten
        return _project_aliases(plan, stream_id, raw)

    if s["kind"] == "zip_join":
        inputs = s["inputs"]
        on_mismatch = s.get("on_mismatch", "error")

        ak = _awkward()
        parts: dict[str, Any] = {}
        lens: list[int] = []

        for inp in inputs:
            sid = inp["stream"]
            pref = inp["prefix"]
            sub = read_stream(plan, sid, file_path, start, stop)

            # length should be the number of events
            lens.append(len(sub))
            parts[pref] = sub

        if len(set(lens)) != 1:
            if on_mismatch == "error":
                raise ValueError(
                    f"zip_join length mismatch: {lens} for streams {[i['stream'] for i in inputs]}"
                )
            # warn/skip behavior can come later; for now you can implement 'warn' as error too
            raise ValueError(
                f"zip_join on_mismatch='{on_mismatch}' unsupported (expected equal lengths)"
            )
        merged = ak.zip(parts, depth_limit=1)
        return _lift_join_aliases(plan, stream_id=stream_id, merged=merged)

    raise ValueError(f"Unknown stream kind: {s['kind']}")


def get_stream_array(data: dict[str, Any], stream_name: str) -> Any:
    if stream_name in data:
        return data[stream_name]
    if DEFAULT_PRIMARY_STREAM_ID in data:
        return data[DEFAULT_PRIMARY_STREAM_ID]
    if data:
        return next(iter(data.values()))
    raise KeyError(
        f"Stream '{stream_name}' not found in data, and no default primary stream available"
    )
