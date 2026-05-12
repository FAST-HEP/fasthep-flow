from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

from hepflow.utils import read_yaml


@dataclass(frozen=True)
class IncludeResult:
    doc: dict[str, Any]
    files: tuple[str, ...]


def _as_abs(path: str, *, base_dir: str) -> str:
    if os.path.isabs(path):
        return os.path.normpath(path)
    return os.path.normpath(os.path.abspath(os.path.join(base_dir, path)))


def _deep_merge(a: Any, b: Any) -> Any:
    """
    Merge b over a.
    - dict: deep merge
    - list: concatenate
    - otherwise: overwrite
    """
    if isinstance(a, dict) and isinstance(b, dict):
        out = dict(a)
        for k, bv in b.items():
            if k in out:
                out[k] = _deep_merge(out[k], bv)
            else:
                out[k] = bv
        return out

    if isinstance(a, list) and isinstance(b, list):
        return list(a) + list(b)

    # type mismatch or scalar: overwrite
    return b


def load_author_with_includes(
    root_path: str,
    max_depth: int = 50,
) -> IncludeResult:
    """
    Recursively resolve `include:` in an author.yaml-like document.

    Conventions:
    - include can be a string path or a list of paths
    - included docs are merged "below" the including doc:
        included first, then local overrides (merge order: include -> local)
    - relative include paths are resolved relative to the file that contains them
    - cycle detection is strict (ValueError)
    """
    root_abs = os.path.normpath(os.path.abspath(root_path))

    visiting: list[str] = []
    visited: set[str] = set()
    files_in_order: list[str] = []

    def _load_one(path_abs: str, depth: int) -> dict[str, Any]:
        if depth > max_depth:
            raise ValueError(
                f"include recursion exceeded max_depth={max_depth}. "
                f"Stack: {visiting}"
            )

        if path_abs in visiting:
            cycle = visiting[visiting.index(path_abs):] + [path_abs]
            raise ValueError(f"include cycle detected: {cycle}")

        visiting.append(path_abs)

        raw = read_yaml(path_abs)
        if raw is None:
            raw = {}
        if not isinstance(raw, dict):
            raise TypeError(
                f"Included YAML must be a mapping/dict: {path_abs}")

        base_dir = os.path.dirname(path_abs)

        inc = raw.get("include", [])
        if isinstance(inc, str):
            inc_list = [inc]
        elif isinstance(inc, list):
            inc_list = inc
        elif inc is None:
            inc_list = []
        else:
            raise TypeError(
                f"'include' must be string or list of strings in {path_abs}"
            )

        merged: dict[str, Any] = {}

        # Merge included docs first
        for rel in inc_list:
            if not isinstance(rel, str) or not rel.strip():
                raise TypeError(
                    f"include entries must be non-empty strings in {path_abs}")
            child_abs = _as_abs(rel, base_dir=base_dir)
            child_doc = _load_one(child_abs, depth + 1)
            merged = _deep_merge(merged, child_doc)

        # Then merge local doc (minus include)
        local = dict(raw)
        local.pop("include", None)
        merged = _deep_merge(merged, local)

        visiting.pop()

        if path_abs not in visited:
            visited.add(path_abs)
            files_in_order.append(path_abs)

        return merged

    doc = _load_one(root_abs, 0)
    return IncludeResult(doc=doc, files=tuple(files_in_order))
