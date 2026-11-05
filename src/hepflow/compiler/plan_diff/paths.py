"""Path-valued plan fields and their lexical display relative to a project root.

Which fields hold paths is an exact structural-path registry; key names are never guessed.
Display normalisation is lexical only: it never resolves symlinks or touches the filesystem.
"""

from __future__ import annotations

import posixpath
import re
from dataclasses import dataclass
from typing import Any

#: Exact key paths of path-valued fields; ``*`` matches one mapping key.
PATH_FIELDS: tuple[tuple[str, ...], ...] = (
    ("context", "workflow_path"),
    ("context", "project_root"),
    ("provenance", "root"),
    ("provenance", "inputs", "declared"),
    ("provenance", "inputs", "resolved"),
    ("provenance", "registry_layers", "path"),
    ("provenance", "execution_layers", "path"),
    ("provenance", "execution_hook_layers", "path"),
    ("nodes", "meta", "compile_hooks", "*", "resolved_path"),
)

#: Fields that record the root itself: shown as written, not relative to themselves.
ROOT_FIELDS: frozenset[tuple[str, ...]] = frozenset(
    {("context", "project_root"), ("provenance", "root")}
)

_URI = re.compile(r"^[A-Za-z][A-Za-z0-9+.\-]*://")


@dataclass(frozen=True)
class PathDisplay:
    value: str
    label: str  # relative | external | uri | no_root | root

    def to_dict(self) -> dict[str, Any]:
        return {"value": self.value, "label": self.label}


def is_path_field(key_path: tuple[str, ...]) -> bool:
    return any(
        len(pattern) == len(key_path)
        and all(p in ("*", k) for p, k in zip(pattern, key_path, strict=True))
        for pattern in PATH_FIELDS
    )


def display_path(value: str, root: str | None) -> PathDisplay:
    """Show ``value`` relative to ``root`` when it lies under it, else unchanged with a label."""
    if _URI.match(value):
        return PathDisplay(value, "uri")
    if not root:
        return PathDisplay(value, "no_root")
    if not posixpath.isabs(value):
        return PathDisplay(value, "relative")
    base = posixpath.normpath(root)
    path = posixpath.normpath(value)
    if path == base:
        return PathDisplay(".", "relative")
    prefix = base if base.endswith("/") else base + "/"
    if path.startswith(prefix):
        return PathDisplay(path[len(prefix) :], "relative")
    return PathDisplay(path, "external")
