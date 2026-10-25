"""The compile-level facts that several compile steps need.

A :class:`CompileContext` is created once where a compile starts and handed to the steps that
need it (profile resolvers, plan builder, parameter compile hooks), instead of copying
``workflow_path`` and ``project_root`` through each function. It is derived from keys that
already exist in ``normalized.yaml`` and ``plan.context`` (``workflow_path``, ``project_root``),
so it adds nothing to those files.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from hepflow.compiler.input_paths import resolve_project_root


@dataclass(frozen=True)
class CompileContext:
    """The workflow file being compiled and the project root its relative paths resolve against."""

    workflow_path: Path
    project_root: Path

    @property
    def entry_dir(self) -> Path:
        """The directory of the entry workflow file."""
        return self.workflow_path.parent

    @classmethod
    def for_workflow(
        cls, workflow_path: str | Path, root: str | Path | None = None
    ) -> CompileContext:
        """Build the context at the start of a compile.

        ``root`` defaults to the current directory and must be an existing directory.
        """
        return cls(Path(workflow_path), resolve_project_root(root))

    @classmethod
    def from_workflow_path(
        cls, workflow_path: str | Path, project_root: Path | None = None
    ) -> CompileContext:
        """Build a context without checking the paths; the root defaults to the entry directory."""
        path = Path(workflow_path)
        return cls(path, project_root if project_root is not None else path.parent)

    @classmethod
    def from_normalized(cls, normalized: Mapping[str, Any]) -> CompileContext | None:
        """Read the context from a normalized workflow (``None`` without a ``workflow_path``).

        A file written before the project root existed falls back to the entry directory.
        """
        return cls._from_mapping(normalized)

    @classmethod
    def from_plan_context(cls, context: Mapping[str, Any]) -> CompileContext | None:
        """Read the context from ``plan.context`` (same rules as ``from_normalized``)."""
        return cls._from_mapping(context)

    @classmethod
    def _from_mapping(cls, data: Mapping[str, Any]) -> CompileContext | None:
        workflow_path = data.get("workflow_path")
        if not isinstance(workflow_path, str) or not workflow_path.strip():
            return None
        project_root = data.get("project_root")
        root = (
            Path(project_root)
            if isinstance(project_root, str) and project_root.strip()
            else None
        )
        return cls.from_workflow_path(workflow_path, root)

    def to_normalized(self) -> dict[str, str]:
        """The keys under which the context is stored in ``normalized.yaml`` and ``plan.context``."""
        return {
            "workflow_path": str(self.workflow_path),
            "project_root": str(self.project_root),
        }
