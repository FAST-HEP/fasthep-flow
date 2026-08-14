from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import Any

from hepflow.compiler.data_flow import apply_data_flow_to_sources, infer_data_flow
from hepflow.model.plan import ExecutionNode, ExecutionPlan, PlanInputRef


@dataclass(frozen=True, slots=True)
class InlineVariationBranch:
    anchor_node_id: str
    variation: Mapping[str, Any]
    parameter_patch: Mapping[str, Any] = dataclass_field(default_factory=dict)
    stop_before: frozenset[str] = frozenset()
    clone_observers: bool = False
    clone_sinks: bool = False
    id_suffix: str | None = None


@dataclass(frozen=True, slots=True)
class InlineVariationResult:
    variation: dict[str, Any]
    cloned_nodes: dict[str, str]
    export_fields: dict[str, str] = dataclass_field(default_factory=dict)


def add_inline_variation_branch(
    plan: ExecutionPlan,
    branch: InlineVariationBranch,
    *,
    update_data_flow: bool = True,
) -> InlineVariationResult:
    """Clone one inline event-stream variation branch inside an execution plan."""
    if not isinstance(branch.variation, Mapping) or not branch.variation:
        raise ValueError("Inline variation requires non-empty variation metadata")

    anchor = plan.get_node(branch.anchor_node_id)
    if not _is_cloneable_event_stream_node(anchor, branch=branch):
        raise ValueError(
            f"Inline variation anchor {anchor.id!r} must be an event-stream transform"
        )
    requested_variation = dict(branch.variation)
    if _node_variation(anchor) is not None:
        raise ValueError(
            f"Inline variation anchor {anchor.id!r} already has variation metadata"
        )

    clone_order = _inline_clone_order(plan, branch=branch)
    if not clone_order:
        raise ValueError(f"Inline variation anchor {branch.anchor_node_id!r} was not cloned")

    suffix = branch.id_suffix or _variation_suffix(requested_variation)
    clone_ids = {node_id: f"{node_id}@{suffix}" for node_id in clone_order}
    collisions = sorted(clone_id for clone_id in clone_ids.values() if clone_id in plan.node_index)
    if collisions:
        raise ValueError(f"Inline variation clone id already exists: {collisions[0]!r}")

    cloned_nodes: dict[str, str] = {}
    for node_id in clone_order:
        original = plan.get_node(node_id)
        _validate_variation_compatible(
            plan,
            original,
            requested_variation=requested_variation,
            cloned_producers=cloned_nodes,
        )
        clone_id = clone_ids[node_id]
        cloned_nodes[node_id] = clone_id
        plan.add_node(
            _clone_node(
                original,
                clone_id=clone_id,
                cloned_producers=cloned_nodes,
                variation=requested_variation,
                parameter_patch=branch.parameter_patch if node_id == anchor.id else {},
            )
        )

    if update_data_flow:
        plan.data_flow = infer_data_flow(plan, registry_cfg=plan.registry)
        apply_data_flow_to_sources(plan)

    return InlineVariationResult(
        variation=requested_variation,
        cloned_nodes=cloned_nodes,
    )


def apply_inline_variation_branches(
    plan: ExecutionPlan,
    normalized: Mapping[str, Any],
) -> list[InlineVariationResult]:
    systematics = normalized.get("systematics")
    if not isinstance(systematics, Mapping):
        return []
    variations = systematics.get("variations") or []
    if not isinstance(variations, list):
        return []

    results: list[InlineVariationResult] = []
    for raw_variation in variations:
        if not isinstance(raw_variation, Mapping):
            continue
        if str(raw_variation.get("mode") or "plan") != "inline":
            continue
        result = add_inline_variation_branch(
            plan,
            InlineVariationBranch(
                anchor_node_id=_stage_node_id(raw_variation.get("anchor")),
                variation=_inline_variation_metadata(raw_variation),
                parameter_patch=dict(raw_variation.get("patch") or {}),
                stop_before=frozenset(
                    _stage_node_id(item)
                    for item in list(raw_variation.get("stop_before") or [])
                ),
            ),
            update_data_flow=False,
        )
        object.__setattr__(result, "export_fields", dict(raw_variation.get("export") or {}))
        results.append(result)

    if results:
        plan.data_flow = infer_data_flow(plan, registry_cfg=plan.registry)
        _insert_variation_collection_boundaries(plan, results)
        apply_data_flow_to_sources(plan)
        _retopologize_plan(plan)
    return results


def _stage_node_id(raw: Any) -> str:
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError("Inline variation requires non-empty anchor stage")
    value = raw.strip()
    return value if "." in value else f"stage.{value}"


def _inline_variation_metadata(raw_variation: Mapping[str, Any]) -> dict[str, Any]:
    metadata = {
        key: deepcopy(value)
        for key, value in raw_variation.items()
        if key not in {"anchor", "export", "patch", "params", "stage", "stop_before"}
        and not _empty_metadata_value(value)
    }
    metadata["mode"] = "inline"
    return metadata


def _empty_metadata_value(value: Any) -> bool:
    if value in (None, [], {}):
        return True
    if isinstance(value, Mapping):
        return all(_empty_metadata_value(item) for item in value.values())
    return False


def _insert_variation_collection_boundaries(
    plan: ExecutionPlan,
    results: list[InlineVariationResult],
) -> None:
    exported = [result for result in results if result.export_fields]
    if not exported:
        return

    for sink in list(plan.nodes):
        if sink.role != "sink":
            continue
        target_ref = _target_stream_ref(sink)
        if target_ref is None:
            continue

        collection_inputs = [
            PlanInputRef(
                node_id=target_ref.node_id,
                output_name=target_ref.output_name,
                input_name="nominal",
            )
        ]
        export_index = 0
        for result in exported:
            varied_node_id = result.cloned_nodes.get(target_ref.node_id)
            if varied_node_id is None:
                continue
            _require_compatible_lineage(
                plan,
                nominal_node_id=target_ref.node_id,
                varied_node_id=varied_node_id,
                variation=result.variation,
            )
            export_node_id = _unique_node_id(
                plan,
                f"export.{target_ref.node_id}@{_variation_suffix(result.variation)}",
            )
            plan.add_node(
                ExecutionNode(
                    id=export_node_id,
                    graph_node_id=export_node_id,
                    role="transform",
                    impl="hep.project_fields",
                    inputs=[
                        PlanInputRef(
                            node_id=varied_node_id,
                            output_name="stream",
                            input_name="stream",
                        )
                    ],
                    params={
                        "stream_id": str(result.variation.get("name") or "variation"),
                        "aliases": dict(result.export_fields),
                    },
                    outputs={"stream": "event_stream"},
                    input_scope=sink.input_scope,
                    output_scope=sink.input_scope,
                    partitioning=deepcopy(sink.partitioning),
                    meta={
                        "inserted_by": "inline_variations",
                        "variation": deepcopy(result.variation),
                        "export_of": varied_node_id,
                    },
                )
            )
            export_index += 1
            collection_inputs.append(
                PlanInputRef(
                    node_id=export_node_id,
                    output_name="stream",
                    input_name=f"variation_{export_index}",
                )
            )

        if len(collection_inputs) == 1:
            continue

        collection_node_id = _unique_node_id(plan, f"collect.{sink.id}")
        plan.add_node(
            ExecutionNode(
                id=collection_node_id,
                graph_node_id=collection_node_id,
                role="transform",
                impl="hep.merge_fields",
                inputs=collection_inputs,
                params={"on_conflict": "error"},
                outputs={"stream": "event_stream"},
                input_scope=sink.input_scope,
                output_scope=sink.input_scope,
                partitioning=deepcopy(sink.partitioning),
                meta={
                    "inserted_by": "inline_variations",
                    "collection_for": sink.id,
                },
            )
        )
        _replace_input_ref(
            sink,
            old_ref=target_ref,
            new_ref=PlanInputRef(
                node_id=collection_node_id,
                output_name="stream",
                input_name=target_ref.input_name,
            ),
        )
    plan.data_flow = infer_data_flow(plan, registry_cfg=plan.registry)


def _target_stream_ref(node: ExecutionNode) -> PlanInputRef | None:
    for ref in node.inputs:
        if ref.output_name == "stream":
            return ref
    return None


def _replace_input_ref(
    node: ExecutionNode,
    *,
    old_ref: PlanInputRef,
    new_ref: PlanInputRef,
) -> None:
    node.inputs = [new_ref if ref == old_ref else ref for ref in node.inputs]


def _require_compatible_lineage(
    plan: ExecutionPlan,
    *,
    nominal_node_id: str,
    varied_node_id: str,
    variation: dict[str, Any],
) -> None:
    lineage = dict((plan.data_flow or {}).get("_stream_lineage") or {})
    nominal = dict(lineage.get(f"{nominal_node_id}:stream") or {}).get("identity")
    varied = dict(lineage.get(f"{varied_node_id}:stream") or {}).get("identity")
    if nominal is not None and varied is not None and nominal == varied:
        return
    raise ValueError(
        f"Inline variation {_variation_name(variation)!r} cannot export fields from "
        f"{varied_node_id!r} into {nominal_node_id!r}: incompatible stream lineage"
    )


def _unique_node_id(plan: ExecutionPlan, base: str) -> str:
    if base not in plan.node_index:
        return base
    index = 1
    while f"{base}.{index}" in plan.node_index:
        index += 1
    return f"{base}.{index}"


def _retopologize_plan(plan: ExecutionPlan) -> None:
    remaining = {node.id: node for node in plan.nodes}
    ordered: list[ExecutionNode] = []
    while remaining:
        ready = [
            node_id
            for node_id, node in remaining.items()
            if all(ref.node_id not in remaining for ref in node.inputs)
        ]
        if not ready:
            cycle = ", ".join(sorted(remaining))
            raise ValueError(f"Execution plan contains cyclic or unresolved inputs: {cycle}")
        for node_id in ready:
            ordered.append(remaining.pop(node_id))
    plan.nodes = ordered
    plan.node_index = {node.id: node for node in ordered}


def _inline_clone_order(
    plan: ExecutionPlan,
    *,
    branch: InlineVariationBranch,
) -> list[str]:
    clone_order: list[str] = []
    cloned: set[str] = set()
    stop_before = set(branch.stop_before)
    for node in plan.nodes:
        if node.id in stop_before:
            continue
        if node.id == branch.anchor_node_id:
            clone_order.append(node.id)
            cloned.add(node.id)
            continue
        if not cloned:
            continue
        if not _has_cloned_event_stream_input(node, cloned):
            continue
        if not _is_cloneable_event_stream_node(node, branch=branch):
            continue
        clone_order.append(node.id)
        cloned.add(node.id)
    return clone_order


def _has_cloned_event_stream_input(node: ExecutionNode, cloned: set[str]) -> bool:
    return any(
        ref.node_id in cloned
        and ref.output_name == "stream"
        and ref.input_name not in {"dependency"}
        for ref in node.inputs
    )


def _is_cloneable_event_stream_node(
    node: ExecutionNode,
    *,
    branch: InlineVariationBranch,
) -> bool:
    if node.role == "transform":
        return node.outputs.get("stream") == "event_stream"
    if node.role == "observer":
        return branch.clone_observers
    if node.role == "sink":
        return branch.clone_sinks
    return False


def _clone_node(
    node: ExecutionNode,
    *,
    clone_id: str,
    cloned_producers: dict[str, str],
    variation: dict[str, Any],
    parameter_patch: Mapping[str, Any],
) -> ExecutionNode:
    params = deepcopy(node.params)
    if parameter_patch:
        params = _merge_patch(params, parameter_patch)
    meta = deepcopy(node.meta)
    meta["variation"] = deepcopy(variation)
    meta["variation_of"] = node.id
    return ExecutionNode(
        id=clone_id,
        graph_node_id=clone_id,
        role=node.role,
        impl=node.impl,
        inputs=[
            PlanInputRef(
                node_id=cloned_producers.get(ref.node_id, ref.node_id),
                output_name=ref.output_name,
                input_name=ref.input_name,
            )
            for ref in node.inputs
        ],
        params=params,
        outputs=deepcopy(node.outputs),
        input_scope=node.input_scope,
        output_scope=node.output_scope,
        partitioning=deepcopy(node.partitioning),
        materialize=node.materialize,
        meta=meta,
    )


def _validate_variation_compatible(
    plan: ExecutionPlan,
    node: ExecutionNode,
    *,
    requested_variation: dict[str, Any],
    cloned_producers: dict[str, str],
) -> None:
    node_variation = _node_variation(node)
    if node_variation is not None and node_variation != requested_variation:
        raise ValueError(
            f"Node {node.id!r} is already in incompatible variation context "
            f"{_variation_name(node_variation)!r}"
        )
    for ref in node.inputs:
        if ref.node_id in cloned_producers:
            continue
        upstream = plan.node_index.get(ref.node_id)
        if upstream is None:
            continue
        upstream_variation = _node_variation(upstream)
        if upstream_variation is None or upstream_variation == requested_variation:
            continue
        raise ValueError(
            f"Cannot form inline variation {_variation_name(requested_variation)!r}: "
            f"node {node.id!r} consumes {ref.node_id!r} from incompatible "
            f"variation context {_variation_name(upstream_variation)!r}"
        )


def _node_variation(node: ExecutionNode) -> dict[str, Any] | None:
    variation = node.meta.get("variation")
    return dict(variation) if isinstance(variation, dict) else None


def _merge_patch(
    params: dict[str, Any],
    patch: Mapping[str, Any],
) -> dict[str, Any]:
    merged = deepcopy(params)
    for key, value in patch.items():
        if (
            isinstance(value, Mapping)
            and isinstance(merged.get(str(key)), dict)
        ):
            merged[str(key)] = _merge_patch(dict(merged[str(key)]), value)
            continue
        merged[str(key)] = deepcopy(value)
    return merged


def _variation_suffix(variation: dict[str, Any]) -> str:
    name = variation.get("name") or variation.get("id")
    if not isinstance(name, str) or not name.strip():
        raise ValueError("Inline variation metadata requires non-empty 'name' or 'id'")
    suffix = "".join(char if char.isalnum() or char in "._-" else "_" for char in name)
    return suffix.strip("._-") or "variation"


def _variation_name(variation: dict[str, Any]) -> str:
    return str(variation.get("name") or variation.get("id") or variation)
