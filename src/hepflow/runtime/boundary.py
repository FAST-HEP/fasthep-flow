from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from hepflow.model.lifecycle import normalize_lifecycle_event
from hepflow.model.plan import ExecutionNode, ExecutionPartition, ExecutionPlan
from hepflow.model.products import (
    BoundaryOutputSpec,
    BoundaryProduct,
    ProductBoundaryPolicy,
    ProductHandlerEntry,
)
from hepflow.registry.runtime import RuntimeRegistry

_SCOPE_RANK = {
    "partition": 0,
    "dataset": 1,
    "global": 2,
}


@dataclass(slots=True)
class PartitionBoundaryResult:
    partition: ExecutionPartition
    products: list[BoundaryProduct]

    def value_store(self) -> dict[tuple[str, str], Any]:
        return boundary_products_to_value_store(self.products)


@dataclass(slots=True, frozen=True)
class ProductDescriptor:
    node_id: str
    output_name: str
    kind: str
    representation: str
    value_type: str

    @classmethod
    def from_boundary_product(cls, product: BoundaryProduct) -> ProductDescriptor:
        return cls(
            node_id=product.node_id,
            output_name=product.output_name,
            kind=product.kind,
            representation=product.representation,
            value_type=type(product.value).__name__,
        )

    def to_dict(self) -> dict[str, str]:
        return {
            "node": self.node_id,
            "port": self.output_name,
            "kind": self.kind,
            "representation": self.representation,
            "type": self.value_type,
        }


@dataclass(slots=True, frozen=True)
class PartitionExecutionSummary:
    partition: ExecutionPartition
    products: list[ProductDescriptor]

    @classmethod
    def from_boundary_result(
        cls,
        result: PartitionBoundaryResult,
    ) -> PartitionExecutionSummary:
        return cls(
            partition=result.partition,
            products=[
                ProductDescriptor.from_boundary_product(product)
                for product in result.products
            ],
        )


class ProductAccumulator:
    def __init__(
        self,
        plan: ExecutionPlan,
        *,
        runtime_registry: RuntimeRegistry,
        dataset_name: str | None = None,
    ) -> None:
        self.plan = plan
        self.runtime_registry = runtime_registry
        self.dataset_name = dataset_name
        self._combined: dict[tuple[str, str], Any] = {}
        self._collected: dict[tuple[str, str], list[Any]] = {}
        self._collected_products: list[BoundaryProduct] = []
        self._partitions_by_id: dict[str, ExecutionPartition] = {}

    @property
    def combined_count(self) -> int:
        return len(self._combined)

    @property
    def collected_count(self) -> int:
        return sum(len(values) for values in self._collected.values())

    def add_result(self, result: PartitionBoundaryResult) -> None:
        self._add_products(result.products, partition=result.partition)

    def add_value_store(self, value_store: Mapping[tuple[str, str], Any]) -> None:
        self._add_values(list(value_store.items()), collected_products=[])

    def finalize(self) -> dict[tuple[str, str], Any]:
        store = dict(self._combined)
        for key, values in self._collected.items():
            store[key] = reduce_product_values(
                self.plan,
                self.runtime_registry,
                key=key,
                values=values,
                dataset_name=self.dataset_name,
            )
        return store

    def collected_partition_results(self) -> list[PartitionBoundaryResult]:
        grouped: dict[str, list[BoundaryProduct]] = {}
        for product in self._collected_products:
            if product.partition_id is None:
                continue
            grouped.setdefault(product.partition_id, []).append(product)
        return [
            PartitionBoundaryResult(
                partition=self._partitions_by_id[partition_id],
                products=products,
            )
            for partition_id, products in grouped.items()
            if partition_id in self._partitions_by_id
        ]

    def _add_products(
        self,
        products: list[BoundaryProduct],
        *,
        partition: ExecutionPartition,
    ) -> None:
        self._add_values(
            [(product.key(), product.value) for product in products],
            collected_products=products,
            partition=partition,
        )

    def _add_values(
        self,
        values: list[tuple[tuple[str, str], Any]],
        *,
        collected_products: list[BoundaryProduct],
        partition: ExecutionPartition | None = None,
    ) -> None:
        next_combined = dict(self._combined)
        next_collected = {key: list(items) for key, items in self._collected.items()}
        next_collected_products = list(self._collected_products)
        next_partitions_by_id = dict(self._partitions_by_id)
        if partition is not None:
            next_partitions_by_id[partition.id] = partition

        products_by_key = {product.key(): product for product in collected_products}
        for key, value in values:
            handler = _handler_for_key(self.plan, self.runtime_registry, key)
            if handler is not None and handler.combine is not None:
                node_id, output_name = key
                node = self.plan.get_node(node_id)
                if key in next_combined:
                    next_combined[key] = handler.combine(
                        next_combined[key],
                        value,
                        node=node,
                        output_name=output_name,
                        dataset_name=self.dataset_name,
                    )
                else:
                    next_combined[key] = value
                continue

            next_collected.setdefault(key, []).append(value)
            product = products_by_key.get(key)
            if product is not None:
                next_collected_products.append(product)

        self._combined = next_combined
        self._collected = next_collected
        self._collected_products = next_collected_products
        self._partitions_by_id = next_partitions_by_id


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


def boundary_products_to_value_store(
    products: list[BoundaryProduct],
) -> dict[tuple[str, str], Any]:
    return {
        (product.node_id, product.output_name): product.value
        for product in products
    }


def partition_boundary_results_to_value_stores(
    results: list[PartitionBoundaryResult],
) -> list[dict[tuple[str, str], Any]]:
    return [result.value_store() for result in results]


def reduce_product_values(
    plan: ExecutionPlan,
    runtime_registry: RuntimeRegistry,
    *,
    key: tuple[str, str],
    values: list[Any],
    dataset_name: str | None,
) -> Any:
    node_id, output_name = key
    try:
        node = plan.get_node(node_id)
        output_kind = node.outputs.get(output_name)
    except KeyError:
        node = None
        output_kind = None

    if node is not None:
        handler = runtime_registry.product_handlers.get(str(output_kind))
        if handler is not None and handler.merge is not None:
            return handler.merge(
                values,
                node=node,
                output_name=output_name,
                dataset_name=dataset_name,
            )

    if output_kind in runtime_registry.product_handlers:
        return values[0] if len(values) == 1 else list(values)

    if output_kind == "report":
        return list(values)

    return values[0] if len(values) == 1 else list(values)


def format_partition_boundary(boundary: list[BoundaryOutputSpec]) -> str:
    lines = ["partition boundary:"]
    for spec in boundary:
        reasons = ", ".join(spec.reasons)
        lines.append(
            f"  {spec.node_id}.{spec.output_name:<16} "
            f"{spec.kind:<10} {spec.policy.representation:<10} {reasons}"
        )
    return "\n".join(lines)


def format_partition_boundary_result(result: PartitionBoundaryResult) -> str:
    lines = [f"partition {result.partition.id} retained:"]
    for product in result.products:
        lines.append(
            f"  {product.node_id}.{product.output_name:<16} "
            f"{product.kind:<10} {product.representation}"
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


def _handler_for_key(
    plan: ExecutionPlan,
    runtime_registry: RuntimeRegistry,
    key: tuple[str, str],
) -> ProductHandlerEntry | None:
    node_id, output_name = key
    try:
        node = plan.get_node(node_id)
    except KeyError:
        return None
    return runtime_registry.product_handlers.get(str(node.outputs.get(output_name)))


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
