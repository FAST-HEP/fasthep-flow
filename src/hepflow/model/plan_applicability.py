from __future__ import annotations

from typing import Any

from hepflow.model.applicability import node_applies_to_dataset
from hepflow.model.component_spec import RuntimeComponentSpec
from hepflow.model.plan import ExecutionNode, ExecutionPlan, PlanInputRef
from hepflow.registry.loaders import load_object


def inactive_inputs_behavior_for_node(
    plan: ExecutionPlan,
    node: ExecutionNode,
) -> str:
    spec = _component_spec_for_node(plan, node)
    if spec is None:
        return "error"
    raw = spec.input.get("inactive_inputs") if isinstance(spec.input, dict) else None
    if raw is None:
        return "error"
    behavior = str(raw)
    if behavior in {"error", "omit"}:
        return behavior
    raise ValueError(
        f"Unsupported inactive_inputs behavior for {node.id!r}: {behavior!r}"
    )


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
        inactive_inputs = inactive_inputs_behavior_for_node(plan, node)
        event_stream_inputs = _event_stream_input_count(plan, node)
        if (
            node.role == "sink"
            and str(node.params.get("when") or "") == "run_end"
            and event_stream_inputs > 1
            and inactive_inputs == "omit"
        ):
            continue
        for ref in node.inputs:
            upstream = plan.get_node(ref.node_id)
            if not node_applies_to_plan_dataset(upstream, dataset=dataset):
                if inactive_inputs == "omit":
                    continue
                if event_stream_inputs > 1:
                    raise ValueError(
                        f"node {node.id!r} has inactive required input "
                        f"{ref.node_id!r}; declare input.inactive_inputs: omit "
                        "to allow contextual omission"
                    )
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
    "inactive_inputs_behavior_for_node",
    "node_applies_to_context",
    "node_applies_to_plan_dataset",
    "resolve_active_input_ref",
    "validate_plan_applicability",
]


def _component_spec_for_node(
    plan: ExecutionPlan,
    node: ExecutionNode,
) -> RuntimeComponentSpec | None:
    category = {
        "transform": "transforms",
        "sink": "sinks",
    }.get(node.role)
    if category is None:
        return None
    entries = (plan.registry or {}).get(category) or {}
    entry = entries.get(node.impl)
    if not isinstance(entry, dict):
        return None
    spec_ref = entry.get("spec")
    if not isinstance(spec_ref, str):
        return None
    return RuntimeComponentSpec.from_obj(load_object(spec_ref))


def _event_stream_input_count(plan: ExecutionPlan, node: ExecutionNode) -> int:
    return sum(
        1
        for ref in node.inputs
        if ref.input_name != "dependency"
        and plan.get_node(ref.node_id).outputs.get(ref.output_name) == "event_stream"
    )
