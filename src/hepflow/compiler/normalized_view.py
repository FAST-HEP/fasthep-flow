"""A typed, read-only view of a normalized workflow.

``normalized.yaml`` and the dict that holds it are the persisted form: it is written section by
section during normalisation, copied per variation and per dataset, and handed to hooks. The
compiler steps that only *read* it use :class:`NormalizedView` instead of repeating
``normalized.get("data") or {}`` with string keys. The view is built once where a step starts,
checks the top-level types, and does not change the file or the dict.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class NormalizedView:
    """The sections of a normalized workflow that the compiler reads."""

    datasets: tuple[dict[str, Any], ...] = ()
    stages: tuple[Any, ...] = ()
    analysis_globals: dict[str, Any] | None = None
    registry: dict[str, Any] | None = None
    provenance: dict[str, Any] | None = None
    execution: Any = None  # normalized by normalize_global_execution, which owns its checks
    execution_hooks: tuple[Any, ...] = ()
    reports: tuple[Any, ...] = ()
    systematics: Mapping[str, Any] | None = None
    systematics_malformed: bool = False
    variation: Mapping[str, Any] | None = None
    workflow_path: str | None = None
    project_root: str | None = None

    @classmethod
    def from_mapping(cls, normalized: Mapping[str, Any]) -> NormalizedView:
        """Read the sections of ``normalized``; a section of the wrong type is a ``ValueError``."""
        data = _mapping(normalized.get("data"), "data")
        analysis = _mapping(normalized.get("analysis"), "analysis")
        systematics = normalized.get("systematics")
        variation = normalized.get("variation")
        return cls(
            datasets=tuple(dict(d) for d in _list(data.get("datasets"), "data.datasets")),
            stages=tuple(_list(analysis.get("stages"), "analysis.stages")),
            analysis_globals=dict(_mapping(analysis.get("globals"), "analysis.globals")),
            registry=dict(_mapping(normalized.get("registry"), "registry")),
            provenance=dict(_mapping(normalized.get("provenance"), "provenance")),
            execution=normalized.get("execution"),
            execution_hooks=tuple(_list(normalized.get("execution_hooks"), "execution_hooks")),
            reports=tuple(_list(normalized.get("reports"), "reports")),
            systematics=systematics if isinstance(systematics, Mapping) else None,
            systematics_malformed=systematics is not None
            and not isinstance(systematics, Mapping),
            variation=variation if isinstance(variation, Mapping) else None,
            workflow_path=_text(normalized.get("workflow_path")),
            project_root=_text(normalized.get("project_root")),
        )

    @property
    def dataset_names(self) -> list[str]:
        return [str(record.get("name")) for record in self.datasets]


def _mapping(value: Any, where: str) -> Mapping[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ValueError(f"{where} must be a mapping")
    return value


def _list(value: Any, where: str) -> list[Any]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError(f"{where} must be a list")
    return value


def _text(value: Any) -> str | None:
    return value if isinstance(value, str) and value.strip() else None
