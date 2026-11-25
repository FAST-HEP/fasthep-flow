from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from hepflow.model import ResolvedResource
from hepflow.runtime.provenance import ProvenanceRecorder, ensure_runtime_provenance


class ComponentContext(dict[str, Any]):
    """Public component-workflow runtime context.

    The context remains dict-compatible for existing components, while exposing
    the stable attribute surface component workflows should prefer.
    """

    @property
    def resources(self) -> Mapping[str, ResolvedResource]:
        resources = self.setdefault("resources", {})
        if not isinstance(resources, Mapping):
            raise ValueError("Component context resources must be a mapping")
        return resources  # type: ignore[return-value]

    @property
    def provenance(self) -> ProvenanceRecorder:
        return ensure_runtime_provenance(self)

    @property
    def dataset(self) -> Mapping[str, Any]:
        dataset = self.get("dataset") or {}
        if not isinstance(dataset, Mapping):
            return {}
        return dataset

    def artifact_path(self, name: str | Path) -> Path:
        """
        Path of ``name`` below this sink's ``artifact_root``.

        A sink treats its root as its working directory: it writes there or in
        subfolders. A name that is absolute or leaves the root raises ``ValueError``.
        A relative root stays relative (it is read from the process working directory).
        """
        root = self.get("artifact_root")
        if not root:
            raise ValueError("artifact_root is not set in the component context")
        candidate = Path(name)
        if candidate.is_absolute():
            raise ValueError(
                f"{str(name)!r} is absolute; give a path below artifact_root"
            )
        base = Path(os.path.normpath(str(root)))
        resolved = Path(os.path.normpath(base / candidate))
        if resolved != base and base not in resolved.parents:
            raise ValueError(f"{str(name)!r} leaves artifact_root {str(root)!r}")
        return resolved

    @property
    def node_id(self) -> str:
        return str(self.get("node_id") or "")

    @property
    def partition_id(self) -> str | None:
        partition = self.get("partition")
        if isinstance(partition, Mapping):
            value = partition.get("id") or partition.get("part")
            return str(value) if value else None
        return None


def component_context(ctx: Mapping[str, Any] | None = None) -> ComponentContext:
    if isinstance(ctx, ComponentContext):
        return ctx
    return ComponentContext(dict(ctx or {}))


__all__ = [
    "ComponentContext",
    "component_context",
]
