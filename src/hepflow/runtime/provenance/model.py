from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class OperationIORecord:
    symbols: list[str] = field(default_factory=list)
    resources: list[str] = field(default_factory=list)

    @classmethod
    def from_obj(cls, value: dict[str, Any] | None) -> OperationIORecord:
        if not isinstance(value, dict):
            return cls()
        return cls(
            symbols=_string_list(value.get("symbols")),
            resources=_string_list(value.get("resources")),
        )

    def to_record(self) -> dict[str, Any]:
        record: dict[str, Any] = {}
        if self.symbols:
            record["symbols"] = list(self.symbols)
        if self.resources:
            record["resources"] = list(self.resources)
        return record


@dataclass(frozen=True)
class OperationRecord:
    inputs: OperationIORecord = field(default_factory=OperationIORecord)
    outputs: OperationIORecord = field(default_factory=OperationIORecord)

    @classmethod
    def from_obj(cls, value: dict[str, Any] | None) -> OperationRecord:
        if not isinstance(value, dict):
            return cls()
        return cls(
            inputs=OperationIORecord.from_obj(value.get("inputs")),
            outputs=OperationIORecord.from_obj(value.get("outputs")),
        )

    def to_record(self) -> dict[str, Any]:
        return _drop_empty(
            {
                "inputs": self.inputs.to_record(),
                "outputs": self.outputs.to_record(),
            }
        )


@dataclass(frozen=True)
class ExecutionRecord:
    id: str
    node_id: str
    impl: str | None = None
    role: str | None = None
    dataset: str | None = None
    partition: str | None = None
    operations: list[OperationRecord] = field(default_factory=list)

    @classmethod
    def from_obj(cls, value: dict[str, Any]) -> ExecutionRecord:
        return cls(
            id=str(value.get("id") or value.get("node_id") or "unknown"),
            node_id=str(value.get("node_id") or "unknown"),
            impl=_optional_str(value.get("impl")),
            role=_optional_str(value.get("role")),
            dataset=_optional_str(value.get("dataset")),
            partition=_optional_str(value.get("partition")),
            operations=[
                OperationRecord.from_obj(item)
                for item in list(value.get("operations") or [])
                if isinstance(item, dict)
            ],
        )

    def to_record(self) -> dict[str, Any]:
        return _drop_empty(
            {
                "id": self.id,
                "node_id": self.node_id,
                "impl": self.impl,
                "role": self.role,
                "dataset": self.dataset,
                "partition": self.partition,
                "operations": [item.to_record() for item in self.operations],
            }
        )


@dataclass(frozen=True)
class ResolvedResourceRecord:
    id: str
    kind: str
    requested_era: str | None = None
    selected_era: str | None = None
    path: str | None = None
    correction: str | None = None
    fallback: bool = False
    reason: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_obj(cls, resource_id: str, value: dict[str, Any]) -> ResolvedResourceRecord:
        selected = dict(value.get("selected") or {})
        return cls(
            id=resource_id,
            kind=str(value.get("kind") or ""),
            requested_era=_optional_str(value.get("requested_era")),
            selected_era=_optional_str(selected.get("era")),
            path=_optional_str(selected.get("path")),
            correction=_optional_str(selected.get("correction")),
            fallback=bool(selected.get("fallback", False)),
            reason=_optional_str(selected.get("reason")),
            metadata=dict(value.get("metadata") or {}),
        )

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


@dataclass(frozen=True)
class ProvenanceDocument:
    version: str
    run_id: str
    workflow: dict[str, Any]
    software: dict[str, Any]
    execution: dict[str, Any]
    partitions: list[dict[str, Any]]
    node_executions: list[dict[str, Any]]
    resources: dict[str, ResolvedResourceRecord] = field(default_factory=dict)
    executions: dict[str, ExecutionRecord] = field(default_factory=dict)

    @classmethod
    def from_obj(cls, value: dict[str, Any]) -> ProvenanceDocument:
        return cls(
            version=str(value.get("version") or ""),
            run_id=str(value.get("run_id") or ""),
            workflow=dict(value.get("workflow") or {}),
            software=dict(value.get("software") or {}),
            execution=dict(value.get("execution") or {}),
            partitions=[
                dict(item)
                for item in list(value.get("partitions") or [])
                if isinstance(item, dict)
            ],
            node_executions=[
                dict(item)
                for item in list(value.get("node_executions") or [])
                if isinstance(item, dict)
            ],
            resources={
                str(resource_id): ResolvedResourceRecord.from_obj(
                    str(resource_id),
                    dict(resource),
                )
                for resource_id, resource in dict(value.get("resources") or {}).items()
                if isinstance(resource, dict)
            },
            executions={
                str(execution_id): ExecutionRecord.from_obj(dict(execution))
                for execution_id, execution in dict(value.get("executions") or {}).items()
                if isinstance(execution, dict)
            },
        )

    def to_record(self) -> dict[str, Any]:
        record: dict[str, Any] = {
            "version": self.version,
            "run_id": self.run_id,
            "workflow": dict(self.workflow),
            "software": dict(self.software),
            "execution": dict(self.execution),
            "partitions": [dict(item) for item in self.partitions],
            "node_executions": [dict(item) for item in self.node_executions],
        }
        if self.resources:
            record["resources"] = {
                resource_id: resource.to_record()
                for resource_id, resource in sorted(self.resources.items())
            }
        if self.executions:
            record["executions"] = {
                execution_id: execution.to_record()
                for execution_id, execution in self.executions.items()
            }
        return record


def _string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, list | tuple | set):
        return [str(item) for item in value if item is not None]
    return [str(value)]


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


def _drop_none(value: dict[str, Any]) -> dict[str, Any]:
    return {key: item for key, item in value.items() if item is not None}


def _drop_empty(value: dict[str, Any]) -> dict[str, Any]:
    return {
        key: item
        for key, item in value.items()
        if item is not None and item != {} and item != []
    }


__all__ = [
    "ExecutionRecord",
    "OperationIORecord",
    "OperationRecord",
    "ProvenanceDocument",
    "ResolvedResourceRecord",
]
