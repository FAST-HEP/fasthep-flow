from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class ResolvedRuntimeResource:
    """Runtime-only resource value plus serialisable resolution metadata."""

    id: str
    kind: str
    value: Any
    requested_era: str | None = None
    selected_era: str | None = None
    path: str | None = None
    correction: str | None = None
    fallback: bool = False
    reason: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_record(self) -> dict[str, Any]:
        selected = _drop_none(
            {
                "era": self.selected_era,
                "path": self.path,
                "correction": self.correction,
                "fallback": self.fallback,
                "reason": self.reason,
            }
        )
        return _drop_none(
            {
                "kind": self.kind,
                "requested_era": self.requested_era,
                "selected": selected or None,
                "metadata": dict(self.metadata) if self.metadata else None,
            }
        )


class RuntimeProvenanceRecorder:
    """Collect per-operation runtime provenance for the current execution."""

    def __init__(self) -> None:
        self._active: dict[str, Any] | None = None
        self._operations: list[dict[str, Any]] = []
        self._resources: dict[str, ResolvedRuntimeResource] = {}

    @contextmanager
    def operation_context(
        self,
        *,
        node_id: str,
        impl: str,
        role: str,
        dataset: str | None = None,
        partition: dict[str, Any] | None = None,
    ) -> Iterator[None]:
        previous = self._active
        partition_id = _partition_id(partition)
        self._active = _drop_none(
            {
                "id": _execution_id(node_id, partition_id),
                "node_id": node_id,
                "impl": impl,
                "role": role,
                "dataset": dataset,
                "partition": partition_id,
            }
        )
        try:
            yield
        finally:
            self._active = previous

    def record_operation(
        self,
        *,
        inputs: dict[str, Any] | None = None,
        outputs: dict[str, Any] | None = None,
    ) -> None:
        active = dict(self._active or {})
        if not active:
            active = {"id": "unknown", "node_id": "unknown"}
        record = {
            **active,
            "operation": {
                "inputs": _normalise_io(inputs),
                "outputs": _normalise_io(outputs),
            },
        }
        self._operations.append(_drop_empty(record))

    def record_resource(self, resource: ResolvedRuntimeResource) -> None:
        self._resources[resource.id] = resource

    def record_operation_record(self, record: dict[str, Any]) -> None:
        self._operations.append(dict(record))

    def record_resource_record(
        self,
        resource_id: str,
        record: dict[str, Any],
    ) -> None:
        selected = dict(record.get("selected") or {})
        self._resources[resource_id] = ResolvedRuntimeResource(
            id=resource_id,
            kind=str(record.get("kind") or ""),
            value=None,
            requested_era=_optional_str(record.get("requested_era")),
            selected_era=_optional_str(selected.get("era")),
            path=_optional_str(selected.get("path")),
            correction=_optional_str(selected.get("correction")),
            fallback=bool(selected.get("fallback", False)),
            reason=_optional_str(selected.get("reason")),
            metadata=dict(record.get("metadata") or {}),
        )

    def merge(self, other: RuntimeProvenanceRecorder) -> None:
        for operation in other.operation_records():
            self._operations.append(operation)
        for resource_id, resource in other.resource_records().items():
            self._resources[resource_id] = resource

    def operation_records(self) -> list[dict[str, Any]]:
        return [dict(item) for item in self._operations]

    def resource_records(self) -> dict[str, ResolvedRuntimeResource]:
        return dict(self._resources)

    def serialise_resources(self) -> dict[str, dict[str, Any]]:
        return {
            resource_id: resource.to_record()
            for resource_id, resource in sorted(self._resources.items())
        }

    def serialise_executions(self) -> dict[str, dict[str, Any]]:
        executions: dict[str, dict[str, Any]] = {}
        for record in self._operations:
            execution_id = str(record.get("id") or record.get("node_id") or "unknown")
            entry = executions.setdefault(
                execution_id,
                {
                    key: record[key]
                    for key in ("id", "node_id", "impl", "role", "dataset", "partition")
                    if key in record
                },
            )
            operations = entry.setdefault("operations", [])
            if isinstance(operations, list):
                operations.append(dict(record.get("operation") or {}))
        return executions


def ensure_runtime_provenance(ctx: dict[str, Any]) -> RuntimeProvenanceRecorder:
    recorder = ctx.get("provenance")
    if isinstance(recorder, RuntimeProvenanceRecorder):
        return recorder
    recorder = RuntimeProvenanceRecorder()
    ctx["provenance"] = recorder
    return recorder


def runtime_provenance_from_ctx(
    ctx: dict[str, Any] | None,
) -> RuntimeProvenanceRecorder | None:
    recorder = (ctx or {}).get("provenance")
    return recorder if isinstance(recorder, RuntimeProvenanceRecorder) else None


def _normalise_io(value: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    record: dict[str, Any] = {}
    symbols = _string_list(value.get("symbols"))
    resources = _string_list(value.get("resources"))
    if symbols:
        record["symbols"] = symbols
    if resources:
        record["resources"] = resources
    return record


def _string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, list | tuple | set):
        return [str(item) for item in value if item is not None]
    return [str(value)]


def _partition_id(partition: dict[str, Any] | None) -> str | None:
    if not isinstance(partition, dict):
        return None
    value = partition.get("id") or partition.get("part")
    return str(value) if value else None


def _execution_id(node_id: str, partition_id: str | None) -> str:
    return f"{node_id}::{partition_id}" if partition_id else node_id


def _drop_none(value: dict[str, Any]) -> dict[str, Any]:
    return {key: item for key, item in value.items() if item is not None}


def _drop_empty(value: dict[str, Any]) -> dict[str, Any]:
    return {
        key: item
        for key, item in value.items()
        if item is not None and item != {} and item != []
    }


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


__all__ = [
    "ResolvedRuntimeResource",
    "RuntimeProvenanceRecorder",
    "ensure_runtime_provenance",
    "runtime_provenance_from_ctx",
]
