from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from hepflow.model.lifecycle import normalize_lifecycle_event
from hepflow.model.plan import ExecutionNode, ExecutionPartition, ExecutionPlan
from hepflow.model.products import (
    BoundaryOutputSpec,
    BoundaryProduct,
    ProductBoundaryPolicy,
)
from hepflow.registry.runtime import RuntimeRegistry

_SCOPE_RANK = {
    "partition": 0,
    "dataset": 1,
    "global": 2,
}


def plan_partition_boundary(
    plan: ExecutionPlan,
    *,
    runtime_registry: RuntimeRegistry,
) -> list[BoundaryOutputSpec]:
    consumers = _consumers_by_output(plan)
    specs: list[BoundaryOutputSpec] = []

    for node in plan.nodes:
        for output_name, kind in node.outputs.items():
            policy = _boundary_policy(str(kind), runtime_registry)
            reasons: list[str] = []
            if policy.retain:
                reasons.append("retained_product")
            for consumer in consumers.get((node.id, output_name), []):
                if _consumer_crosses_partition_boundary(consumer):
                    reasons.append(f"{consumer.input_scope}_consumer:{consumer.id}")
            if reasons:
                specs.append(
                    BoundaryOutputSpec(
                        node_id=node.id,
                        output_name=output_name,
                        kind=str(kind),
                        policy=policy,
                        reasons=tuple(dict.fromkeys(reasons)),
                    )
                )

    return specs


def extract_boundary_products(
    plan: ExecutionPlan,
    value_store: Mapping[tuple[str, str], Any],
    *,
    partition: ExecutionPartition | Mapping[str, Any] | None,
    boundary: list[BoundaryOutputSpec],
    runtime_registry: RuntimeRegistry,
) -> list[BoundaryProduct]:
    del plan
    partition_id, dataset = _partition_identity(partition)
    products: list[BoundaryProduct] = []

    for spec in boundary:
        value_key = spec.key()
        if value_key not in value_store:
            continue
        policy = _boundary_policy(spec.kind, runtime_registry)
        if policy.representation == "materialize":
            handler = runtime_registry.product_handlers.get(spec.kind)
            if handler is None or handler.boundary_materialize is None:
                raise NotImplementedError(
                    f"Boundary materialization for product kind {spec.kind!r} "
                    "is not implemented."
                )
            value = handler.boundary_materialize(
                value_store[value_key],
                node_id=spec.node_id,
                output_name=spec.output_name,
                partition=partition,
            )
        else:
            value = value_store[value_key]

        products.append(
            BoundaryProduct(
                node_id=spec.node_id,
                output_name=spec.output_name,
                kind=spec.kind,
                dataset=dataset,
                partition_id=partition_id,
                representation=policy.representation,
                value=value,
                metadata={"reasons": list(spec.reasons)},
            )
        )

    return products


def format_partition_boundary(boundary: list[BoundaryOutputSpec]) -> str:
    lines = ["partition boundary:"]
    for spec in boundary:
        reasons = ", ".join(spec.reasons)
        lines.append(
            f"  {spec.node_id}.{spec.output_name:<16} "
            f"{spec.kind:<10} {spec.policy.representation:<10} {reasons}"
        )
    return "\n".join(lines)


def _boundary_policy(
    kind: str,
    runtime_registry: RuntimeRegistry,
) -> ProductBoundaryPolicy:
    handler = runtime_registry.product_handlers.get(kind)
    if handler is None:
        return ProductBoundaryPolicy()
    return handler.boundary


def _consumers_by_output(
    plan: ExecutionPlan,
) -> dict[tuple[str, str], list[ExecutionNode]]:
    consumers: dict[tuple[str, str], list[ExecutionNode]] = {}
    for node in plan.nodes:
        for ref in node.inputs:
            consumers.setdefault((ref.node_id, ref.output_name), []).append(node)
    return consumers


def _consumer_crosses_partition_boundary(node: ExecutionNode) -> bool:
    if _scope_rank(node.input_scope) > _SCOPE_RANK["partition"]:
        return True
    if node.role == "sink":
        when = _sink_when(node)
        return when in {"dataset_end", "run_end"}
    return False


def _scope_rank(scope: str) -> int:
    try:
        return _SCOPE_RANK[scope]
    except KeyError as exc:
        raise ValueError(f"Unknown execution scope {scope!r}") from exc


def _sink_when(node: ExecutionNode) -> str:
    default = "run_end" if str(node.impl).startswith("hep.render.") else "partition_end"
    return normalize_lifecycle_event(dict(node.params or {}).get("when") or default)


def _partition_identity(
    partition: ExecutionPartition | Mapping[str, Any] | None,
) -> tuple[str | None, str | None]:
    if partition is None:
        return None, None
    if isinstance(partition, ExecutionPartition):
        return partition.id, partition.dataset
    partition_id = partition.get("id")
    dataset = partition.get("dataset")
    return (
        str(partition_id) if partition_id is not None else None,
        str(dataset) if dataset is not None else None,
    )
