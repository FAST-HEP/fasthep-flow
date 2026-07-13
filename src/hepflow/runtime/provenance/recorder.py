from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from hepflow.model import ResolvedResource
from hepflow.runtime.provenance.model import ExecutionRecord, OperationRecord
from hepflow.runtime.provenance.store import ProvenanceStore


class ProvenanceRecorder:
    """Runtime-facing recorder attached to an execution context."""

    def __init__(self, store: ProvenanceStore | None = None) -> None:
        self._active: dict[str, Any] | None = None
        self._store = store or ProvenanceStore()

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
        execution = ExecutionRecord.from_obj(active)
        operation = OperationRecord.from_obj(
            {
                "inputs": inputs,
                "outputs": outputs,
            }
        )
        self._store.register_operation_record(execution, operation)

    def record_resource(self, resource: ResolvedResource) -> None:
        self._store.register_resource(resource)

    def record_operation_record(self, record: dict[str, Any]) -> None:
        execution = ExecutionRecord.from_obj(record)
        operation = OperationRecord.from_obj(record.get("operation"))
        self._store.register_operation_record(execution, operation)

    def record_resource_record(
        self,
        resource_id: str,
        record: dict[str, Any],
    ) -> None:
        self._store.register_resource_record(resource_id, record)

    def merge(self, other: ProvenanceRecorder) -> None:
        self._store.merge(other.store)

    @property
    def store(self) -> ProvenanceStore:
        return self._store

    def operation_records(self) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for execution in self._store.executions().values():
            execution_meta = {
                key: value
                for key, value in execution.to_record().items()
                if key != "operations"
            }
            for operation in execution.operations:
                records.append(
                    {
                        **execution_meta,
                        "operation": operation.to_record(),
                    }
                )
        return records

    def serialise_resources(self) -> dict[str, dict[str, Any]]:
        return self._store.serialise_resources()

    def serialise_executions(self) -> dict[str, dict[str, Any]]:
        return self._store.serialise_executions()

    def validate(self) -> None:
        self._store.validate()


def ensure_runtime_provenance(ctx: dict[str, Any]) -> ProvenanceRecorder:
    recorder = ctx.get("provenance")
    if isinstance(recorder, ProvenanceRecorder):
        return recorder
    recorder = ProvenanceRecorder()
    ctx["provenance"] = recorder
    return recorder


def runtime_provenance_from_ctx(
    ctx: dict[str, Any] | None,
) -> ProvenanceRecorder | None:
    recorder = (ctx or {}).get("provenance")
    return recorder if isinstance(recorder, ProvenanceRecorder) else None


def _partition_id(partition: dict[str, Any] | None) -> str | None:
    if not isinstance(partition, dict):
        return None
    value = partition.get("id") or partition.get("part")
    return str(value) if value else None


def _execution_id(node_id: str, partition_id: str | None) -> str:
    return f"{node_id}::{partition_id}" if partition_id else node_id


def _drop_none(value: dict[str, Any]) -> dict[str, Any]:
    return {key: item for key, item in value.items() if item is not None}


__all__ = [
    "ProvenanceRecorder",
    "ensure_runtime_provenance",
    "runtime_provenance_from_ctx",
]
