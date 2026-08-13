from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from hepflow.build_layout import BuildPaths


@dataclass(slots=True)
class CompileHookContext:
    normalized: dict[str, Any]
    plan_context: dict[str, Any]
    build_paths: BuildPaths
    artifacts: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class ParamCompileHookContext:
    input_stream_fields: tuple[str, ...] = ()
    workflow_dir: str | None = None


@dataclass(slots=True, frozen=True)
class CompileHookResult:
    value: Any
    provenance: dict[str, Any]
