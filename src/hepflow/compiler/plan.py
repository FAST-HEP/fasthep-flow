from __future__ import annotations

from typing import Any, cast

import networkx as nx

from hepflow.compiler.data_flow import (
    apply_data_flow_to_sources,
    infer_data_flow,
)
from hepflow.compiler.execution import (
    normalize_global_execution,
    validate_stage_execution_resource_references,
)
from hepflow.compiler.lower_graph import lower_author_to_graph
from hepflow.model.graph import get_graph_node
from hepflow.model.lifecycle import WHEN_ALIASES
from hepflow.model.plan import (
    ExecutionNode,
    ExecutionPartition,
    ExecutionPlan,
    MaterializeMode,
    PartitionSpec,
    PlanInputRef,
    Scope,
)


def build_execution_plan(
    graph: nx.DiGraph,
    *,
    chunk_size: int | None = None,
    registry: dict[str, Any] | None = None,
    provenance: dict[str, Any] | None = None,
    execution: dict[str, Any] | None = None,
    execution_hooks: list[dict[str, Any]] | None = None,
) -> ExecutionPlan:
    plan = ExecutionPlan()
    plan.registry = dict(registry or {})
    plan.provenance = dict(provenance or {})
    plan.execution = normalize_global_execution(execution)
    plan.execution_hooks = _normalize_execution_hooks(execution_hooks or [])
    context_datasets_by_name: dict[str, dict[str, Any]] = {}

    for node_id in nx.topological_sort(graph):
        graph_node = get_graph_node(graph, node_id)

        inputs: list[PlanInputRef] = []
        for upstream_node_id, _, edge_data in graph.in_edges(node_id, data=True):
            inputs.append(
                PlanInputRef(
                    node_id=upstream_node_id,
                    output_name=str(edge_data.get("output") or "stream"),
                    input_name=str(edge_data.get("input_name") or "stream"),
                )
            )

        input_scope, output_scope, partitioning, materialize = (
            _default_execution_policy(
                role=graph_node.role,
                impl=graph_node.impl,
                outputs=graph_node.outputs,
                chunk_size=chunk_size,
            )
        )

        params = dict(graph_node.params)
        if graph_node.role == "source":
            for dataset in list(graph_node.params.get("datasets") or []):
                dataset_name = str(dataset["name"])
                if dataset_name in context_datasets_by_name:
                    continue
                context_datasets_by_name[dataset_name] = {
                    "name": dataset_name,
                    "files": list(dataset.get("files") or []),
                    "nevents": dataset.get("nevents"),
                    "eventtype": dataset.get("eventtype"),
                    "group": dataset.get("group"),
                    "meta": dict(dataset.get("meta") or {}),
                }
        if graph_node.role == "source" and "datasets" in params:
            params.pop("datasets", None)
            params["datasets_ref"] = "context.datasets"

        plan.add_node(
            ExecutionNode(
                id=node_id,
                graph_node_id=node_id,
                role=graph_node.role,
                impl=graph_node.impl,
                inputs=inputs,
                params=params,
                outputs=dict(graph_node.outputs),
                input_scope=cast(Scope, input_scope),
                output_scope=cast(Scope, output_scope),
                partitioning=partitioning,
                materialize=cast(MaterializeMode, materialize),
                meta=dict(graph_node.meta),
            )
        )

    plan.context = build_plan_context(
        plan,
        datasets_by_name=context_datasets_by_name,
        globals_block=dict(graph.graph.get("analysis_globals") or {}),
    )
    plan.data_flow = infer_data_flow(plan, registry_cfg=plan.registry)
    apply_data_flow_to_sources(plan)
    plan.partitions = build_execution_partitions(plan, chunk_size=chunk_size)
    return plan


def build_plan_from_normalized(
    normalized: dict[str, Any],
    *,
    chunk_size: int | None = None,
) -> tuple[nx.DiGraph, ExecutionPlan]:
    execution = normalize_global_execution(normalized.get("execution"))
    validate_stage_execution_resource_references(
        list((normalized.get("analysis") or {}).get("stages") or []),
        execution["resources"],
        execution["pools"],
    )
    graph = lower_author_to_graph(normalized)
    plan = build_execution_plan(
        graph,
        chunk_size=chunk_size,
        registry=dict(normalized.get("registry") or {}),
        provenance=dict(normalized.get("provenance") or {}),
        execution=execution,
        execution_hooks=list(normalized.get("execution_hooks") or []),
    )
    variation = normalized.get("variation")
    if isinstance(variation, dict):
        plan.context["variation"] = dict(variation)
    return graph, plan


def _normalize_execution_hooks(
    execution_hooks: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for hook in list(execution_hooks or []):
        if not isinstance(hook, dict):
            normalized.append(hook)
            continue
        item = dict(hook)
        if "events" in item:
            item["events"] = [
                WHEN_ALIASES.get(str(event).strip(), str(event).strip())
                for event in list(item.get("events") or [])
            ]
        normalized.append(item)
    return normalized


def build_plan_context(
    plan: ExecutionPlan,
    *,
    datasets_by_name: dict[str, dict[str, Any]] | None = None,
    globals_block: dict[str, Any] | None = None,
) -> dict[str, Any]:
    del plan
    datasets_by_name = dict(datasets_by_name or {})
    return {
        "datasets": datasets_by_name,
        "dataset_names": list(datasets_by_name.keys()),
        "globals": dict(globals_block or {}),
    }


def build_execution_partitions(
    plan: ExecutionPlan,
    *,
    chunk_size: int | None = None,
) -> list[ExecutionPartition]:
    source_nodes = [node for node in plan.nodes if node.role == "source"]
    if not source_nodes:
        raise ValueError("Execution plan has no source nodes; cannot build partitions")

    partitions: list[ExecutionPartition] = []
    datasets_by_name = dict(plan.context.get("datasets") or {})

    for source_node in source_nodes:
        source_name = str(
            source_node.meta.get("source_name")
            or source_node.id.removeprefix("read.")
        )
        if not datasets_by_name:
            continue

        for dataset_name, dataset in datasets_by_name.items():
            files = list((dataset or {}).get("files") or [])
            if not files:
                raise ValueError(
                    f"Source {source_name!r} dataset {dataset_name!r} has no files"
                )

            nevents_raw = (dataset or {}).get("nevents")
            nevents = _coerce_nevents(nevents_raw)

            for file_index, file_path in enumerate(files):
                if chunk_size is not None and nevents is not None:
                    for chunk_index, start in enumerate(range(0, nevents, chunk_size)):
                        stop = min(start + chunk_size, nevents)
                        partitions.append(
                            ExecutionPartition(
                                id=(
                                    f"{source_name}__{dataset_name}__"
                                    f"{file_index}_{chunk_index}"
                                ),
                                dataset=dataset_name,
                                file=str(file_path),
                                source=source_name,
                                part=f"{file_index}_{chunk_index}",
                                start=start,
                                stop=stop,
                            )
                        )
                    continue

                partitions.append(
                    ExecutionPartition(
                        id=f"{source_name}__{dataset_name}__{file_index}",
                        dataset=dataset_name,
                        file=str(file_path),
                        source=source_name,
                        part=f"{file_index}_0",
                        start=0 if nevents is not None else None,
                        stop=nevents,
                    )
                )

    return partitions


def _coerce_nevents(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
    return int(value)


def _default_execution_policy(
    *,
    role: str,
    impl: str,
    outputs: dict[str, str],
    chunk_size: int | None,
) -> tuple[str, str, PartitionSpec, str]:
    """Very first-pass policy."""
    del impl
    if role == "source":
        if chunk_size is not None:
            return (
                "global",
                "partition",
                PartitionSpec(mode="dataset_chunks", chunk_size=chunk_size),
                "never",
            )
        return (
            "global",
            "dataset",
            PartitionSpec(mode="dataset"),
            "never",
        )

    if role == "transform":
        if outputs == {"hist": "histogram"}:
            return (
                "partition",
                "partition",
                PartitionSpec(mode="dataset_chunks", chunk_size=chunk_size)
                if chunk_size is not None
                else PartitionSpec(mode="dataset"),
                "never",
            )

        if outputs.get("cutflow") == "cutflow":
            return (
                "partition",
                "partition",
                PartitionSpec(mode="dataset_chunks", chunk_size=chunk_size)
                if chunk_size is not None
                else PartitionSpec(mode="dataset"),
                "never",
            )

        return (
            "partition",
            "partition",
            PartitionSpec(mode="dataset_chunks", chunk_size=chunk_size)
            if chunk_size is not None
            else PartitionSpec(mode="dataset"),
            "never",
        )

    if role == "observer":
        return ("global", "global", PartitionSpec(mode="none"), "always")

    if role == "sink":
        return ("global", "global", PartitionSpec(mode="none"), "always")

    raise ValueError(f"Unknown execution role: {role!r}")
