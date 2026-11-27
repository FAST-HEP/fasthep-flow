"""Shared test builders: a workflow with a few changes, and files on disk.

Start from the ``toy_workflow`` fixture (``tests/fixtures/workflows/toy_workflow.yaml`` plus the
toy registry) and state only what a test changes.
"""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from pathlib import Path
from typing import Any

import yaml


def modified(base: Mapping[str, Any], **changes: Any) -> dict[str, Any]:
    """A deep copy of ``base`` with ``changes`` applied; ``base`` is not touched.

    A dict value is merged into the existing dict, any other value replaces it, and
    ``None`` removes the key.
    """
    result = deepcopy(dict(base))
    _apply(result, changes)
    return result


def _apply(target: dict[str, Any], changes: Mapping[str, Any]) -> None:
    for key, value in changes.items():
        if value is None:
            target.pop(key, None)
        elif isinstance(value, Mapping) and isinstance(target.get(key), dict):
            _apply(target[key], value)
        else:
            target[key] = deepcopy(value)


def write_yaml(path: Path, data: Any) -> Path:
    """``data`` as YAML at ``path``, keeping key order."""
    path.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")
    return path


def write_file(path: Path, content: str | bytes = b"") -> Path:
    """``content`` at ``path``, creating its parent directories."""
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(content, bytes):
        path.write_bytes(content)
    else:
        path.write_text(content, encoding="utf-8")
    return path


def write_files(root: Path, files: Mapping[str, str | bytes]) -> Path:
    """A tree of files below ``root``: ``{"pkg/__init__.py": "", ...}``."""
    for relative, content in files.items():
        write_file(root / relative, content)
    return root
