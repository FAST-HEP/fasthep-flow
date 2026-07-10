from __future__ import annotations

from typing import Any

from hepflow.model.applicability import node_applies_to_dataset
from hepflow.model.plan import ExecutionNode, ExecutionPlan, PlanInputRef


def node_applies_to_plan_dataset(
    node: ExecutionNode,
    *,
    dataset: dict[str, Any] | None,
) -> bool:
    return node_applies_to_dataset(
        node.meta.get("applies_to"),
        dataset=dataset,
    )


def node_applies_to_context(node: ExecutionNode, *, ctx: dict[str, Any]) -> bool:
    dataset = ctx.get("dataset")
    return node_applies_to_plan_dataset(
        node,
        dataset=dataset if isinstance(dataset, dict) else None,
    )


def active_plan_nodes_for_dataset(
    plan: ExecutionPlan,
    *,
    dataset: dict[str, Any] | None,
) -> list[ExecutionNode]:
    return [
        node
        for node in plan.nodes
        if node_applies_to_plan_dataset(node, dataset=dataset)
    ]


def active_plan_nodes_for_context(
    plan: ExecutionPlan,
    *,
    ctx: dict[str, Any],
) -> list[ExecutionNode]:
    dataset = ctx.get("dataset")
    return active_plan_nodes_for_dataset(
        plan,
        dataset=dataset if isinstance(dataset, dict) else None,
    )


def resolve_active_input_ref(
    plan: ExecutionPlan,
    ref: PlanInputRef,
    *,
    dataset: dict[str, Any] | None,
) -> PlanInputRef:
    upstream = plan.get_node(ref.node_id)
    if node_applies_to_plan_dataset(upstream, dataset=dataset):
        return ref
    return _bypass_inactive_node(
        plan,
        ref,
        dataset=dataset,
        seen={ref.node_id},
    )


def validate_plan_applicability(plan: ExecutionPlan) -> None:
    datasets = dict(plan.context.get("datasets") or {})
    if not datasets:
        _validate_dataset(plan, dataset=None, label="default")
        return
    for name, dataset in datasets.items():
        _validate_dataset(plan, dataset=dict(dataset or {}), label=str(name))


def _validate_dataset(
    plan: ExecutionPlan,
    *,
    dataset: dict[str, Any] | None,
    label: str,
) -> None:
    for node in active_plan_nodes_for_dataset(plan, dataset=dataset):
        for ref in node.inputs:
            try:
                resolve_active_input_ref(plan, ref, dataset=dataset)
            except ValueError as exc:
                raise ValueError(
                    f"Unsupported applies_to graph for dataset {label!r} "
                    f"at node {node.id!r}: {exc}"
                ) from exc


def _bypass_inactive_node(
    plan: ExecutionPlan,
    ref: PlanInputRef,
    *,
    dataset: dict[str, Any] | None,
    seen: set[str],
) -> PlanInputRef:
    inactive = plan.get_node(ref.node_id)
    if inactive.role != "transform":
        raise ValueError(
            f"inactive node {inactive.id!r} cannot be removed transparently "
            f"because its role is {inactive.role!r}"
        )
    if len(inactive.inputs) != 1:
        raise ValueError(
            f"inactive node {inactive.id!r} cannot be removed transparently "
            "because it has multiple inputs"
        )
    if len(inactive.outputs) != 1 or ref.output_name not in inactive.outputs:
        raise ValueError(
            f"inactive node {inactive.id!r} cannot be removed transparently "
            "because the requested output is not its only output"
        )
    if inactive.outputs.get(ref.output_name) != "event_stream":
        raise ValueError(
            f"inactive node {inactive.id!r} cannot be removed transparently "
            "because its output is not an event_stream"
        )

    upstream_ref = inactive.inputs[0]
    upstream = plan.get_node(upstream_ref.node_id)
    if upstream.outputs.get(upstream_ref.output_name) != "event_stream":
        raise ValueError(
            f"inactive node {inactive.id!r} cannot be removed transparently "
            "because its input is not an event_stream"
        )
    if upstream.id in seen:
        raise ValueError(f"cycle detected while bypassing inactive node {upstream.id!r}")

    if node_applies_to_plan_dataset(upstream, dataset=dataset):
        return PlanInputRef(
            node_id=upstream_ref.node_id,
            output_name=upstream_ref.output_name,
            input_name=ref.input_name,
        )

    bypassed = _bypass_inactive_node(
        plan,
        upstream_ref,
        dataset=dataset,
        seen={*seen, upstream.id},
    )
    return PlanInputRef(
        node_id=bypassed.node_id,
        output_name=bypassed.output_name,
        input_name=ref.input_name,
    )


__all__ = [
    "active_plan_nodes_for_context",
    "active_plan_nodes_for_dataset",
    "node_applies_to_context",
    "node_applies_to_plan_dataset",
    "resolve_active_input_ref",
    "validate_plan_applicability",
]
