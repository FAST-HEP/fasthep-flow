"""Resolve the input paths of ``execution.resources`` entries at compile time.

A resource names its ``kind``; the registry section ``resource_kinds`` lists the fields of
that kind that hold input paths. Those fields are rewritten to absolute paths and every
declaration is returned as a :class:`ResolvedInput` for the inputs manifest.
"""

from __future__ import annotations

import warnings
from collections.abc import Mapping
from copy import deepcopy
from pathlib import Path
from typing import Any

from hepflow.compiler.input_paths import ResolvedInput, resolve_path_value


def resolve_resource_paths(
    resources: Mapping[str, Any],
    *,
    registry: Mapping[str, Any],
    root: Path,
    entry_dir: Path | None,
) -> tuple[dict[str, Any], list[ResolvedInput]]:
    """Return ``resources`` with declared path fields made absolute, plus the entries."""
    kinds = dict(registry.get("resource_kinds") or {})
    out: dict[str, Any] = {}
    entries: list[ResolvedInput] = []
    for resource_id, resource in resources.items():
        if not isinstance(resource, Mapping) or "kind" not in resource:
            out[resource_id] = deepcopy(resource)
            continue
        kind = resource["kind"]
        declaration = kinds.get(kind)
        if declaration is None:
            warnings.warn(
                f"resource {resource_id!r}: kind {kind!r} declares no path fields "
                "(registry.resource_kinds), so its paths are not resolved",
                UserWarning,
                stacklevel=2,
            )
            out[resource_id] = deepcopy(resource)
            continue
        item = deepcopy(dict(resource))
        for field in _path_fields(kind, declaration):
            if item.get(field) is None:
                continue
            item[field], resolved_entries = resolve_path_value(
                item[field],
                role=f"resource:{resource_id}.{field}",
                root=root,
                entry_dir=entry_dir,
                label=f"resource {resource_id!r}: field {field!r}",
            )
            entries.extend(resolved_entries)
        out[resource_id] = item
    return out, entries


def _path_fields(kind: str, declaration: Any) -> list[str]:
    fields = (
        declaration.get("path_fields") if isinstance(declaration, Mapping) else None
    )
    if not isinstance(fields, list) or not all(isinstance(f, str) for f in fields):
        raise ValueError(
            f"registry.resource_kinds[{kind!r}].path_fields must be a list of strings"
        )
    return fields
