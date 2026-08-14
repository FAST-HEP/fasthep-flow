from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import Any

import networkx as nx

from hepflow.model.applicability import applicability_is_empty, intersect_applicability
from hepflow.model.graph import (
    GraphNode,
    add_graph_edge,
    add_graph_node,
    get_graph_node,
)
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
    update_data_flow: bool = False,
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
        return InlineVariationResult(
            variation=requested_variation,
            cloned_nodes={},
        )

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

    del update_data_flow

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
        _insert_variation_collection_boundaries(plan, results)
    return results


def apply_inline_variation_branches_to_graph(
    graph: nx.DiGraph,
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
        result = add_inline_variation_branch_to_graph(
            graph,
            InlineVariationBranch(
                anchor_node_id=_stage_node_id(raw_variation.get("anchor")),
                variation=_inline_variation_metadata(raw_variation),
                parameter_patch=dict(raw_variation.get("patch") or {}),
                stop_before=frozenset(
                    _stage_node_id(item)
                    for item in list(raw_variation.get("stop_before") or [])
                ),
            ),
        )
        object.__setattr__(result, "export_fields", dict(raw_variation.get("export") or {}))
        results.append(result)

    if results:
        _insert_variation_collection_boundaries_in_graph(graph, results)
    return results


def add_inline_variation_branch_to_graph(
    graph: nx.DiGraph,
    branch: InlineVariationBranch,
) -> InlineVariationResult:
    if not isinstance(branch.variation, Mapping) or not branch.variation:
        raise ValueError("Inline variation requires non-empty variation metadata")

    anchor = get_graph_node(graph, branch.anchor_node_id)
    if not _is_cloneable_event_stream_graph_node(anchor, branch=branch):
        raise ValueError(
            f"Inline variation anchor {anchor.id!r} must be an event-stream transform"
        )
    requested_variation = dict(branch.variation)
    if _graph_node_variation(anchor) is not None:
        raise ValueError(
            f"Inline variation anchor {anchor.id!r} already has variation metadata"
        )

    clone_order = _inline_graph_clone_order(graph, branch=branch)
    if not clone_order:
        return InlineVariationResult(
            variation=requested_variation,
            cloned_nodes={},
        )

    suffix = branch.id_suffix or _variation_suffix(requested_variation)
    clone_ids = {node_id: f"{node_id}@{suffix}" for node_id in clone_order}
    collisions = sorted(clone_id for clone_id in clone_ids.values() if clone_id in graph)
    if collisions:
        raise ValueError(f"Inline variation clone id already exists: {collisions[0]!r}")

    cloned_nodes: dict[str, str] = {}
    for node_id in clone_order:
        original = get_graph_node(graph, node_id)
        _validate_graph_variation_compatible(
            graph,
            original,
            requested_variation=requested_variation,
            cloned_producers=cloned_nodes,
        )
        clone_id = clone_ids[node_id]
        cloned_nodes[node_id] = clone_id
        add_graph_node(
            graph,
            _clone_graph_node(
                original,
                clone_id=clone_id,
                variation=requested_variation,
                parameter_patch=branch.parameter_patch if node_id == anchor.id else {},
            ),
        )
        for upstream, _, edge_data in list(graph.in_edges(node_id, data=True)):
            add_graph_edge(
                graph,
                cloned_nodes.get(str(upstream), str(upstream)),
                clone_id,
                output=str(edge_data.get("output") or "stream"),
                input_name=str(edge_data.get("input_name") or "stream"),
            )

    return InlineVariationResult(
        variation=requested_variation,
        cloned_nodes=cloned_nodes,
    )


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

    cloned_originals = {
        original_id for result in exported for original_id in result.cloned_nodes
    }

    for boundary in list(plan.nodes):
        if boundary.role not in {"transform", "sink"}:
            continue
        if boundary.id in cloned_originals or _node_variation(boundary) is not None:
            continue
        target_ref = _target_stream_ref(boundary)
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
                        "include_existing": False,
                    },
                    outputs={"stream": "event_stream"},
                    input_scope=boundary.input_scope,
                    output_scope=boundary.input_scope,
                    partitioning=deepcopy(boundary.partitioning),
                    meta={
                        "inserted_by": "inline_variations",
                        "variation": deepcopy(result.variation),
                        "export_of": varied_node_id,
                        **(
                            {"applies_to": applicability}
                            if (
                                applicability := _clone_applicability(
                                    plan.get_node(varied_node_id).meta,
                                    result.variation,
                                )
                            )
                            is not None
                            else {}
                        ),
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

        collection_node_id = _unique_node_id(plan, f"collect.{boundary.id}")
        plan.add_node(
            ExecutionNode(
                id=collection_node_id,
                graph_node_id=collection_node_id,
                role="transform",
                impl="hep.merge_fields",
                inputs=collection_inputs,
                params={"on_conflict": "error"},
                outputs={"stream": "event_stream"},
                input_scope=boundary.input_scope,
                output_scope=boundary.input_scope,
                partitioning=deepcopy(boundary.partitioning),
                meta={
                    "inserted_by": "inline_variations",
                    "collection_for": boundary.id,
                },
            )
        )
        _replace_input_ref(
            boundary,
            old_ref=target_ref,
            new_ref=PlanInputRef(
                node_id=collection_node_id,
                output_name="stream",
                input_name=target_ref.input_name,
            ),
        )


def _target_stream_ref(node: ExecutionNode) -> PlanInputRef | None:
    for ref in node.inputs:
        if ref.output_name == "stream":
            return ref
    return None


def _insert_variation_collection_boundaries_in_graph(
    graph: nx.DiGraph,
    results: list[InlineVariationResult],
) -> None:
    exported = [result for result in results if result.export_fields]
    if not exported:
        return

    cloned_originals = {
        original_id for result in exported for original_id in result.cloned_nodes
    }

    for boundary_id in list(nx.topological_sort(graph)):
        boundary = get_graph_node(graph, str(boundary_id))
        if boundary.role not in {"transform", "sink"}:
            continue
        if boundary.id in cloned_originals or _graph_node_variation(boundary) is not None:
            continue
        target_edge = _target_stream_edge(graph, boundary.id)
        if target_edge is None:
            continue
        target_upstream, edge_data = target_edge

        collection_inputs: list[tuple[str, str]] = [(target_upstream, "nominal")]
        export_index = 0
        for result in exported:
            varied_node_id = result.cloned_nodes.get(target_upstream)
            if varied_node_id is None:
                continue
            export_node_id = _unique_graph_node_id(
                graph,
                f"export.{target_upstream}@{_variation_suffix(result.variation)}",
            )
            add_graph_node(
                graph,
                GraphNode(
                    id=export_node_id,
                    role="transform",
                    impl="hep.project_fields",
                    params={
                        "stream_id": str(result.variation.get("name") or "variation"),
                        "aliases": dict(result.export_fields),
                        "include_existing": False,
                    },
                    outputs={"stream": "event_stream"},
                    meta={
                        "inserted_by": "inline_variations",
                        "variation": deepcopy(result.variation),
                        "export_of": varied_node_id,
                        **(
                            {"applies_to": applicability}
                            if (
                                applicability := _clone_applicability(
                                    get_graph_node(graph, varied_node_id).meta,
                                    result.variation,
                                )
                            )
                            is not None
                            else {}
                        ),
                    },
                ),
            )
            add_graph_edge(graph, varied_node_id, export_node_id, input_name="stream")
            export_index += 1
            collection_inputs.append((export_node_id, f"variation_{export_index}"))

        if len(collection_inputs) == 1:
            continue

        collection_node_id = _unique_graph_node_id(graph, f"collect.{boundary.id}")
        add_graph_node(
            graph,
            GraphNode(
                id=collection_node_id,
                role="transform",
                impl="hep.merge_fields",
                params={"on_conflict": "error"},
                outputs={"stream": "event_stream"},
                meta={
                    "inserted_by": "inline_variations",
                    "collection_for": boundary.id,
                },
            ),
        )
        for upstream, input_name in collection_inputs:
            add_graph_edge(graph, upstream, collection_node_id, input_name=input_name)
        graph.remove_edge(target_upstream, boundary.id)
        add_graph_edge(
            graph,
            collection_node_id,
            boundary.id,
            output=str(edge_data.get("output") or "stream"),
            input_name=str(edge_data.get("input_name") or "stream"),
        )


def _target_stream_edge(
    graph: nx.DiGraph,
    node_id: str,
) -> tuple[str, Mapping[str, Any]] | None:
    for upstream, _, edge_data in graph.in_edges(node_id, data=True):
        if str(edge_data.get("output") or "stream") == "stream":
            return str(upstream), dict(edge_data)
    return None


def _unique_graph_node_id(graph: nx.DiGraph, base: str) -> str:
    if base not in graph:
        return base
    index = 1
    while f"{base}.{index}" in graph:
        index += 1
    return f"{base}.{index}"


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
    if _lineage_identities_match(
        lineage,
        nominal_node_id=nominal_node_id,
        varied_node_id=varied_node_id,
    ):
        return
    for dataset_lineage in dict(
        (plan.data_flow or {}).get("_stream_lineage_by_dataset") or {}
    ).values():
        if _lineage_identities_match(
            dict(dataset_lineage or {}),
            nominal_node_id=nominal_node_id,
            varied_node_id=varied_node_id,
        ):
            return
    raise ValueError(
        f"Inline variation {_variation_name(variation)!r} cannot export fields from "
        f"{varied_node_id!r} into {nominal_node_id!r}: incompatible stream lineage"
    )


def _lineage_identities_match(
    lineage: Mapping[str, Any],
    *,
    nominal_node_id: str,
    varied_node_id: str,
) -> bool:
    nominal = dict(lineage.get(f"{nominal_node_id}:stream") or {}).get("identity")
    varied = dict(lineage.get(f"{varied_node_id}:stream") or {}).get("identity")
    return nominal is not None and varied is not None and nominal == varied


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
            if _clone_applicability_is_empty(node.meta, branch.variation):
                break
            clone_order.append(node.id)
            cloned.add(node.id)
            continue
        if not cloned:
            continue
        if not _has_cloned_event_stream_input(node, cloned):
            continue
        if not _is_cloneable_event_stream_node(node, branch=branch):
            continue
        if _clone_applicability_is_empty(node.meta, branch.variation):
            continue
        clone_order.append(node.id)
        cloned.add(node.id)
    return clone_order


def _inline_graph_clone_order(
    graph: nx.DiGraph,
    *,
    branch: InlineVariationBranch,
) -> list[str]:
    clone_order: list[str] = []
    cloned: set[str] = set()
    stop_before = set(branch.stop_before)
    for raw_node_id in nx.topological_sort(graph):
        node_id = str(raw_node_id)
        if node_id in stop_before:
            continue
        if node_id == branch.anchor_node_id:
            if _clone_applicability_is_empty(
                get_graph_node(graph, node_id).meta,
                branch.variation,
            ):
                break
            clone_order.append(node_id)
            cloned.add(node_id)
            continue
        if not cloned:
            continue
        if not _has_cloned_event_stream_graph_input(graph, node_id, cloned):
            continue
        node = get_graph_node(graph, node_id)
        if not _is_cloneable_event_stream_graph_node(node, branch=branch):
            continue
        if _clone_applicability_is_empty(node.meta, branch.variation):
            continue
        clone_order.append(node_id)
        cloned.add(node_id)
    return clone_order


def _has_cloned_event_stream_graph_input(
    graph: nx.DiGraph,
    node_id: str,
    cloned: set[str],
) -> bool:
    return any(
        str(upstream) in cloned
        and str(edge_data.get("output") or "stream") == "stream"
        and str(edge_data.get("input_name") or "stream") != "dependency"
        for upstream, _, edge_data in graph.in_edges(node_id, data=True)
    )


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


def _is_cloneable_event_stream_graph_node(
    node: GraphNode,
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
    applicability = _clone_applicability(node.meta, variation)
    if applicability is not None:
        meta["applies_to"] = applicability
    else:
        meta.pop("applies_to", None)
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


def _clone_graph_node(
    node: GraphNode,
    *,
    clone_id: str,
    variation: dict[str, Any],
    parameter_patch: Mapping[str, Any],
) -> GraphNode:
    params = deepcopy(node.params)
    if parameter_patch:
        params = _merge_patch(params, parameter_patch)
    meta = deepcopy(node.meta)
    meta["variation"] = deepcopy(variation)
    meta["variation_of"] = node.id
    applicability = _clone_applicability(node.meta, variation)
    if applicability is not None:
        meta["applies_to"] = applicability
    else:
        meta.pop("applies_to", None)
    return GraphNode(
        id=clone_id,
        role=node.role,
        impl=node.impl,
        params=params,
        outputs=deepcopy(node.outputs),
        meta=meta,
    )


def _clone_applicability(
    original_meta: Mapping[str, Any],
    variation: Mapping[str, Any],
) -> dict[str, Any] | None:
    return intersect_applicability(
        original_meta.get("applies_to"),
        variation.get("applies_to"),
    )


def _clone_applicability_is_empty(
    original_meta: Mapping[str, Any],
    variation: Mapping[str, Any],
) -> bool:
    return applicability_is_empty(_clone_applicability(original_meta, variation))


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


def _validate_graph_variation_compatible(
    graph: nx.DiGraph,
    node: GraphNode,
    *,
    requested_variation: dict[str, Any],
    cloned_producers: dict[str, str],
) -> None:
    node_variation = _graph_node_variation(node)
    if node_variation is not None and node_variation != requested_variation:
        raise ValueError(
            f"Node {node.id!r} is already in incompatible variation context "
            f"{_variation_name(node_variation)!r}"
        )
    for upstream, _, _edge_data in graph.in_edges(node.id, data=True):
        upstream_id = str(upstream)
        if upstream_id in cloned_producers:
            continue
        upstream_node = get_graph_node(graph, upstream_id)
        upstream_variation = _graph_node_variation(upstream_node)
        if upstream_variation is None or upstream_variation == requested_variation:
            continue
        raise ValueError(
            f"Cannot form inline variation {_variation_name(requested_variation)!r}: "
            f"node {node.id!r} consumes {upstream_id!r} from incompatible "
            f"variation context {_variation_name(upstream_variation)!r}"
        )


def _node_variation(node: ExecutionNode) -> dict[str, Any] | None:
    variation = node.meta.get("variation")
    return dict(variation) if isinstance(variation, dict) else None


def _graph_node_variation(node: GraphNode) -> dict[str, Any] | None:
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
