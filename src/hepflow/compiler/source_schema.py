from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import uproot

from hepflow.model.issues import FlowIssue, IssueLevel
from hepflow.model.source_schema import (
    BranchSchema,
    ObjectSchema,
    make_file_schema,
    make_schema_signature,
)
from hepflow.utils import read_json, write_json

_CYCLE_RE = re.compile(r"^(?P<base>.*?);(?P<cycle>\d+)$")


def split_root_cycle(path: str) -> tuple[str, int | None]:
    m = _CYCLE_RE.match(path)
    if not m:
        return path, None
    return m.group("base"), int(m.group("cycle"))


def strip_root_cycle(path: str) -> str:
    base, _ = split_root_cycle(path)
    return base


def inspect_root_file_schema(
    *,
    dataset: str,
    file_index: int,
    file_path: str,
) -> dict[str, Any]:
    """
    Inspect a ROOT file and return a file-centric schema dict.

    v1 behavior:
    - only TTree objects are recorded
    - ROOT cycle numbers are normalized away in ObjectSchema.path
    - original ROOT path is preserved in ObjectSchema.path_raw
    - if multiple cycles of the same object exist, the highest cycle is kept
    """
    # canonical_path -> (cycle_value, raw_path, class_name)
    # cycle_value: None treated as -1 for comparison
    best_objects: dict[str, tuple[int, str, str]] = {}

    with uproot.open(file_path) as f:
        classnames = f.classnames(recursive=True)

        # First pass: choose best raw object per canonical path
        for raw_path, clsname in sorted(classnames.items()):
            if clsname != "TTree":
                continue

            canon_path, cycle = split_root_cycle(raw_path)
            cycle_value = cycle if cycle is not None else -1

            prev = best_objects.get(canon_path)
            if prev is None or cycle_value > prev[0]:
                best_objects[canon_path] = (cycle_value, str(raw_path), str(clsname))

        objects: list[ObjectSchema] = []

        # Second pass: inspect selected objects
        for canon_path in sorted(best_objects.keys()):
            _, raw_path, clsname = best_objects[canon_path]

            tree = f[raw_path]
            branches: list[BranchSchema] = []

            # tree.keys() may return branch/subbranch names, which is fine for our purposes
            for i, name in enumerate(tree.keys()):
                br = tree[name]
                try:
                    typename = str(getattr(br, "typename", "") or "")
                except Exception:
                    typename = ""

                branches.append(
                    BranchSchema(
                        index=i,
                        name=str(name),
                        typename=typename,
                    )
                )

            sig = make_schema_signature(
                object_path=canon_path,
                object_type="TTree",
                branches=branches,
            )

            objects.append(
                ObjectSchema(
                    path=canon_path,
                    path_raw=raw_path,
                    type="TTree",
                    entries=int(tree.num_entries),
                    schema_signature=sig,
                    branches=branches,
                )
            )

    return make_file_schema(
        dataset=dataset,
        file_index=file_index,
        file_path=file_path,
        objects=objects,
    ).to_dict()


def write_dataset_source_schema(
    *,
    dataset: str,
    file_index: int,
    file_path: str,
    work_dir: str,
) -> tuple[str, dict[str, Any], bool]:
    work_path = Path(work_dir)
    out_dir = work_path / "source_schema"
    out_dir.mkdir(parents=True, exist_ok=True)

    filename = f"source_schema_{dataset}_file_{file_index:04d}.json"
    abs_path = out_dir / filename
    rel_path = abs_path.relative_to(work_path)

    if abs_path.exists():
        schema = read_json(abs_path)
        return str(rel_path), schema, True

    schema = inspect_root_file_schema(
        dataset=dataset,
        file_index=file_index,
        file_path=file_path,
    )
    write_json(schema, abs_path)
    return str(rel_path), schema, False


def _load_schema_json(path: str | Path) -> dict[str, Any]:
    obj = read_json(path)
    if not isinstance(obj, dict):
        raise TypeError(f"Source schema file must contain a JSON object: {path}")
    return obj


def _objects_by_path(schema: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for obj in schema.get("objects", []) or []:
        if not isinstance(obj, dict):
            continue
        p = obj.get("path")
        if isinstance(p, str) and p:
            out[p] = obj
    return out


def _branch_names(obj: dict[str, Any]) -> set[str]:
    out: set[str] = set()
    for b in obj.get("branches", []) or []:
        if isinstance(b, dict):
            name = b.get("name")
            if isinstance(name, str) and name:
                out.add(name)
    return out


def validate_source_schema_against_plan(
    plan: dict[str, Any],
    *,
    work_dir: str,
) -> list[FlowIssue]:
    """
    Validate plan sources / required_inputs / fieldmap against per-dataset source schema JSON.

    Expected plan structure:
      reports:
        source_schema:
          <dataset>:
            path: source_schema/source_schema_<dataset>_file_0000.json
            file: /abs/path/to/file.root
    """
    issues: list[FlowIssue] = []

    reports = plan.get("reports") or {}
    schema_report = reports.get("source_schema") or {}
    datasets = plan.get("datasets") or {}
    streams = plan.get("streams") or {}
    required_inputs = plan.get("required_inputs") or {}
    fieldmap = plan.get("fieldmap") or {}

    # Precompute field usage by stream from required_inputs
    # alias is "used" if its branch is in required_inputs[stream].branches
    used_aliases_by_stream: dict[str, set[str]] = {}
    for alias, spec in fieldmap.items():
        if not isinstance(spec, dict):
            continue
        stream = spec.get("stream")
        branch = spec.get("branch")
        if not isinstance(stream, str) or not isinstance(branch, str):
            continue
        req_branches = set((required_inputs.get(stream) or {}).get("branches", []))
        if branch in req_branches:
            used_aliases_by_stream.setdefault(stream, set()).add(str(alias))

    # Validate each dataset against its inspected schema
    for dataset_name in datasets:
        ds_report = schema_report.get(dataset_name)
        if not isinstance(ds_report, dict):
            issues.append(
                FlowIssue(
                    level=IssueLevel.WARN,
                    code="SOURCE_SCHEMA_MISSING",
                    message=f"No source schema report found for dataset '{dataset_name}'",
                    meta={"dataset": dataset_name},
                )
            )
            continue

        rel_path = ds_report.get("path")
        inspected_file = ds_report.get("file")
        if not isinstance(rel_path, str) or not rel_path:
            issues.append(
                FlowIssue(
                    level=IssueLevel.WARN,
                    code="SOURCE_SCHEMA_PATH_MISSING",
                    message=f"Source schema report for dataset '{dataset_name}' has no valid path",
                    meta={"dataset": dataset_name, "report": ds_report},
                )
            )
            continue

        schema_path = Path(rel_path)
        abs_schema_path = schema_path if schema_path.is_absolute() else Path(work_dir) / schema_path
        try:
            schema = _load_schema_json(abs_schema_path)
        except Exception as e:
            issues.append(
                FlowIssue(
                    level=IssueLevel.ERROR,
                    code="SOURCE_SCHEMA_LOAD_FAILED",
                    message=f"Could not load source schema for dataset '{dataset_name}'",
                    meta={
                        "dataset": dataset_name,
                        "path": str(abs_schema_path),
                        "error": f"{type(e).__name__}: {e}",
                    },
                )
            )
            continue

        obj_by_path = _objects_by_path(schema)

        # 1) validate source trees exist
        for stream_id, stream_spec in streams.items():
            if not isinstance(stream_spec, dict):
                continue
            if stream_spec.get("kind") != "root_tree":
                # only validate physical root_tree streams here
                continue

            tree_path = stream_spec.get("tree")
            if not isinstance(tree_path, str) or not tree_path:
                continue

            if tree_path not in obj_by_path:
                issues.append(
                    FlowIssue(
                        level=IssueLevel.ERROR,
                        code="SOURCE_TREE_NOT_FOUND",
                        message=(
                            f"Configured source tree '{tree_path}' for stream '{stream_id}' "
                            f"was not found in inspected file for dataset '{dataset_name}'"
                        ),
                        meta={
                            "dataset": dataset_name,
                            "stream": stream_id,
                            "tree": tree_path,
                            "schema_path": abs_schema_path,
                            "file": inspected_file,
                            "available_objects": sorted(obj_by_path.keys()),
                        },
                    )
                )
                continue

            obj = obj_by_path[tree_path]
            branches = _branch_names(obj)

            # 2) validate required_inputs branches exist
            ri = required_inputs.get(stream_id) or {}
            required_branches: list[Any] = list(ri.get("branches") or [])
            missing_required = [b for b in required_branches if b not in branches]
            if missing_required:
                issues.append(
                    FlowIssue(
                        level=IssueLevel.ERROR,
                        code="REQUIRED_BRANCH_MISSING",
                        message=(
                            f"Some required input branches for stream '{stream_id}' are not present "
                            f"in tree '{tree_path}' for dataset '{dataset_name}'"
                        ),
                        meta={
                            "dataset": dataset_name,
                            "stream": stream_id,
                            "tree": tree_path,
                            "missing": sorted(missing_required),
                            "schema_path": abs_schema_path,
                            "file": inspected_file,
                        },
                    )
                )

            # 3) validate field aliases against actual branches
            aliases_for_stream: dict[str, str] = {}
            for alias, spec in fieldmap.items():
                if not isinstance(spec, dict):
                    continue
                s = spec.get("stream")
                b = spec.get("branch")
                if s == stream_id and isinstance(alias, str) and isinstance(b, str):
                    aliases_for_stream[alias] = b

            if not aliases_for_stream:
                continue

            # 3a) alias overwrites real branch name
            overwriting_aliases = sorted(
                [a for a in aliases_for_stream if a in branches]
            )
            if overwriting_aliases:
                issues.append(
                    FlowIssue(
                        level=IssueLevel.ERROR,
                        code="FIELD_ALIAS_OVERWRITES_BRANCH",
                        message=(
                            f"Some field aliases for stream '{stream_id}' would overwrite real branch names "
                            f"in tree '{tree_path}' for dataset '{dataset_name}'"
                        ),
                        meta={
                            "dataset": dataset_name,
                            "stream": stream_id,
                            "tree": tree_path,
                            "aliases": overwriting_aliases,
                            "schema_path": abs_schema_path,
                            "file": inspected_file,
                        },
                    )
                )

            # 3b) alias branch missing if alias is used
            used_aliases = used_aliases_by_stream.get(stream_id, set())
            missing_used_aliases: list[dict[str, str]] = []
            unused_aliases: list[str] = []

            for alias, branch in aliases_for_stream.items():
                if alias in used_aliases:
                    if branch not in branches:
                        missing_used_aliases.append({"alias": alias, "branch": branch})
                else:
                    unused_aliases.append(alias)

            if missing_used_aliases:
                issues.append(
                    FlowIssue(
                        level=IssueLevel.ERROR,
                        code="FIELD_ALIAS_BRANCH_MISSING",
                        message=(
                            f"Some used field aliases for stream '{stream_id}' point to branches that are not present "
                            f"in tree '{tree_path}' for dataset '{dataset_name}'"
                        ),
                        meta={
                            "dataset": dataset_name,
                            "stream": stream_id,
                            "tree": tree_path,
                            "missing": missing_used_aliases,
                            "schema_path": abs_schema_path,
                            "file": inspected_file,
                        },
                    )
                )

    return issues
