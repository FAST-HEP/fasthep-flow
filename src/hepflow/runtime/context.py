from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from hepflow.model import ResolvedResource
from hepflow.runtime.provenance import ProvenanceRecorder, ensure_runtime_provenance


class ComponentContext(dict[str, Any]):
    """Public component-author runtime context.

    The context remains dict-compatible for existing components, while exposing
    the stable attribute surface component authors should prefer.
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
