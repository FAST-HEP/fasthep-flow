from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

Scope = Literal["partition", "dataset", "global"]
MaterializeMode = Literal["never", "if_requested", "always"]


@dataclass(slots=True, frozen=True)
class PlanInputRef:
    node_id: str
    output_name: str
    input_name: str

    def to_dict(self) -> dict[str, str]:
        return {
            "node_id": self.node_id,
            "output_name": self.output_name,
            "input_name": self.input_name,
        }


@dataclass(slots=True)
class PartitionSpec:
    """
    Backend-neutral partitioning intent.

    mode:
      - none: unpartitioned/global
      - dataset: one unit per dataset
      - dataset_chunks: dataset split into entry chunks
    """

    mode: Literal["none", "dataset", "dataset_chunks"] = "none"
    chunk_size: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "chunk_size": self.chunk_size,
        }


@dataclass(slots=True)
class ExecutionNode:
    """Backend-neutral execution unit description."""

    id: str
    graph_node_id: str
    role: Literal["source", "transform", "observer", "sink"]
    impl: str

    inputs: list[PlanInputRef] = field(default_factory=list)
    params: dict[str, Any] = field(default_factory=dict)
    outputs: dict[str, str] = field(default_factory=dict)

    input_scope: Scope = "global"
    output_scope: Scope = "global"
    partitioning: PartitionSpec = field(default_factory=PartitionSpec)

    materialize: MaterializeMode = "never"

    meta: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "graph_node_id": self.graph_node_id,
            "role": self.role,
            "impl": self.impl,
            "inputs": [item.to_dict() for item in self.inputs],
            "params": self.params,
            "outputs": self.outputs,
            "input_scope": self.input_scope,
            "output_scope": self.output_scope,
            "partitioning": self.partitioning.to_dict(),
            "materialize": self.materialize,
            "meta": self.meta,
        }


@dataclass(slots=True)
class ExecutionPlan:
    nodes: list[ExecutionNode] = field(default_factory=list)
    node_index: dict[str, ExecutionNode] = field(default_factory=dict)
    partitions: list[ExecutionPartition] = field(default_factory=list)
    context: dict[str, Any] = field(default_factory=dict)
    registry: dict[str, Any] = field(default_factory=dict)
    provenance: dict[str, Any] = field(default_factory=dict)
    execution: dict[str, Any] = field(
        default_factory=lambda: {
            "backend": "local",
            "strategy": "default",
            "profiles": [],
            "resources": {},
            "pools": {},
            "environment": {},
            "config": {},
        }
    )
    execution_hooks: list[dict[str, Any]] = field(default_factory=list)
    reports: list[dict[str, Any]] = field(default_factory=list)
    data_flow: dict[str, Any] = field(default_factory=dict)

    def add_node(self, node: ExecutionNode) -> None:
        if node.id in self.node_index:
            raise ValueError(f"Duplicate execution node id: {node.id}")
        self.nodes.append(node)
        self.node_index[node.id] = node

    def get_node(self, node_id: str) -> ExecutionNode:
        try:
            return self.node_index[node_id]
        except KeyError as exc:
            raise KeyError(f"Unknown execution node id: {node_id}") from exc

    def to_dict(self) -> dict[str, Any]:
        return {
            "context": self.context,
            "nodes": [node.to_dict() for node in self.nodes],
            "partitions": [partition.to_dict() for partition in self.partitions],
            "registry": self.registry,
            "provenance": self.provenance,
            "execution": self.execution,
            "execution_hooks": self.execution_hooks,
            "reports": self.reports,
            "data_flow": self.data_flow,
        }


def resolve_plan_ref(ref: str, plan: ExecutionPlan) -> Any:
    parts = str(ref).split(".")
    if not parts or parts[0] != "context":
        raise ValueError(f"Unsupported plan ref {ref!r}; only context.* is supported")

    current: Any = plan.context
    for part in parts[1:]:
        if not isinstance(current, dict):
            raise KeyError(
                f"Plan ref {ref!r} cannot be resolved through "
                f"non-mapping segment {part!r}"
            )
        try:
            current = current[part]
        except KeyError as exc:
            raise KeyError(f"Plan ref {ref!r} missing segment {part!r}") from exc
    return current


@dataclass(slots=True, frozen=True)
class ExecutionPartition:
    id: str
    dataset: str
    file: str
    source: str
    part: str
    start: int | None = None
    stop: int | None = None

    def to_context(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "dataset": self.dataset,
            "file": self.file,
            "source": self.source,
            "part": self.part,
            "start": self.start,
            "stop": self.stop,
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "dataset": self.dataset,
            "file": self.file,
            "source": self.source,
            "part": self.part,
            "start": self.start,
            "stop": self.stop,
        }
