from __future__ import annotations

from typing import Any, cast

import networkx as nx

from hepflow.backends.loaders import (
    backend_build_directories,
    validate_backend_execution,
)
from hepflow.build_layout import validate_build_relative_path
from hepflow.compiler.compile_hooks import run_param_compile_hooks
from hepflow.compiler.component_defaults import apply_component_param_defaults
from hepflow.compiler.context import CompileContext
from hepflow.compiler.data_flow import (
    apply_data_flow_to_sources,
    infer_data_flow,
)
from hepflow.compiler.execution import (
    normalize_global_execution,
    validate_stage_execution_resource_references,
)
from hepflow.compiler.file_access import resolve_file_access
from hepflow.compiler.inline_variations import apply_inline_variation_branches_to_graph
from hepflow.compiler.lower_graph import lower_workflow_to_graph
from hepflow.compiler.output_claims import validate_output_claims
from hepflow.compiler.partitioning import (
    derive_dataset_partitions,
    resolve_chunk_size,
)
from hepflow.compiler.per_dataset import (
    per_dataset_enabled,
    stub_unselected_datasets,
    validate_dataset_selection,
    validate_per_dataset_plan,
    with_per_dataset,
)
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
from hepflow.model.plan_applicability import validate_plan_applicability


def build_execution_plan(
    graph: nx.DiGraph,
    *,
    chunk_size: int | None = None,
    dataset: str | None = None,
    registry: dict[str, Any] | None = None,
    provenance: dict[str, Any] | None = None,
    execution: dict[str, Any] | None = None,
    execution_hooks: list[dict[str, Any]] | None = None,
    compile_context: CompileContext | None = None,
) -> ExecutionPlan:
    plan = ExecutionPlan()
    plan.registry = dict(registry or {})
    plan.provenance = dict(provenance or {})
    plan.execution = normalize_global_execution(execution)
    plan.execution_hooks = _normalize_execution_hooks(execution_hooks or [])
    file_access = list(plan.execution.get("file_access") or [])
    context_datasets_by_name: dict[str, dict[str, Any]] = {
        str(dataset["name"]): _context_dataset(dataset, file_access)
        for dataset in list(graph.graph.get("datasets") or [])
        if isinstance(dataset, dict) and dataset.get("name") is not None
    }

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
            for record in list(graph_node.params.get("datasets") or []):
                dataset_name = str(record["name"])
                if dataset_name in context_datasets_by_name:
                    continue
                context_datasets_by_name[dataset_name] = _context_dataset(
                    record, file_access
                )
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

    _validate_connected_scopes(plan)
    plan.context = build_plan_context(
        plan,
        datasets_by_name=context_datasets_by_name,
        globals_block=dict(graph.graph.get("analysis_globals") or {}),
        compile_context=compile_context,
    )
    validate_plan_applicability(plan)
    plan.data_flow = infer_data_flow(plan, registry_cfg=plan.registry)
    run_param_compile_hooks(plan, registry_cfg=plan.registry, warn=True)
    plan.data_flow = infer_data_flow(plan, registry_cfg=plan.registry)
    _drop_compile_only_data_flow(plan.data_flow)
    apply_data_flow_to_sources(plan)
    validate_output_claims(plan)
    _validate_backend_policy(plan)
    if per_dataset_enabled(plan.execution):
        validate_per_dataset_plan(plan)
    plan.partitions = build_execution_partitions(
        plan, chunk_size=chunk_size, dataset=dataset
    )
    return plan


def _validate_backend_policy(
    plan: ExecutionPlan,
) -> None:
    validate_backend_execution(
        plan.registry,
        plan.execution,
        provenance=plan.provenance,
    )
    build_directories = backend_build_directories(
        plan.registry,
        plan.execution,
        provenance=plan.provenance,
    )
    for directory in build_directories:
        validate_build_relative_path(directory)
    plan.provenance["backend_build_directories"] = list(build_directories)


def build_plan_from_normalized(
    normalized: dict[str, Any],
    *,
    chunk_size: int | None = None,
    dataset: str | None = None,
    per_dataset: bool | None = None,
) -> tuple[nx.DiGraph, ExecutionPlan]:
    """Lower a normalized workflow to a graph and plan.

    ``per_dataset`` overrides ``execution.partitioning.per_dataset``. ``dataset`` selects one
    dataset (per-dataset mode only): the others keep metadata only and get no partitions.
    """
    normalized = apply_component_param_defaults(normalized)
    execution = normalize_global_execution(normalized.get("execution"))
    enabled = per_dataset_enabled(execution, per_dataset)
    execution = with_per_dataset(execution, enabled)
    validate_dataset_selection(
        dataset,
        enabled=enabled,
        names=[
            str(record.get("name"))
            for record in list((normalized.get("data") or {}).get("datasets") or [])
        ],
    )
    if dataset is not None:
        normalized = stub_unselected_datasets(normalized, dataset)
    validate_stage_execution_resource_references(
        list((normalized.get("analysis") or {}).get("stages") or []),
        execution["resources"],
        execution["pools"],
    )
    graph = lower_workflow_to_graph(normalized)
    apply_inline_variation_branches_to_graph(graph, normalized)
    plan = build_execution_plan(
        graph,
        chunk_size=chunk_size,
        dataset=dataset,
        registry=dict(normalized.get("registry") or {}),
        provenance=dict(normalized.get("provenance") or {}),
        execution=execution,
        execution_hooks=list(normalized.get("execution_hooks") or []),
        compile_context=CompileContext.from_normalized(normalized),
    )
    variation = normalized.get("variation")
    if isinstance(variation, dict):
        plan.context["variation"] = dict(variation)
    plan.reports = list(normalized.get("reports") or [])
    return graph, plan


def _drop_compile_only_data_flow(data_flow: dict[str, Any]) -> None:
    data_flow.pop("input_fields_by_node", None)
    data_flow.pop("input_fields_by_dataset", None)


def _validate_connected_scopes(plan: ExecutionPlan) -> None:
    for consumer in plan.nodes:
        for ref in consumer.inputs:
            if ref.input_name == "dependency":
                continue
            producer = plan.node_index.get(ref.node_id)
            if producer is None:
                raise ValueError(
                    f"Node {consumer.id!r} input references unknown producer "
                    f"{ref.node_id!r}"
                )
            if ref.output_name not in producer.outputs:
                raise ValueError(
                    f"Node {consumer.id!r} input references missing output "
                    f"{ref.node_id}.{ref.output_name}"
                )
            output_kind = producer.outputs[ref.output_name]
            if not _connected_scopes_compatible(
                producer,
                consumer,
                output_kind,
                input_name=ref.input_name,
            ):
                raise ValueError(
                    f"Node {consumer.id!r} with input scope {consumer.input_scope!r} "
                    f"cannot consume {ref.node_id}.{ref.output_name} from output "
                    f"scope {producer.output_scope!r}"
                )


def _connected_scopes_compatible(
    producer: ExecutionNode,
    consumer: ExecutionNode,
    output_kind: str,
    *,
    input_name: str,
) -> bool:
    if consumer.role != "transform":
        return True
    if consumer.input_scope != "partition":
        return True
    if producer.role == "source":
        return True
    if producer.output_scope != "global":
        return True
    return _is_reusable_global_side_product(output_kind, input_name=input_name)


def _is_reusable_global_side_product(kind: str, *, input_name: str) -> bool:
    return input_name != "stream" and kind != "event_stream"


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


def _context_dataset(
    dataset: dict[str, Any], file_access: list[dict[str, str]]
) -> dict[str, Any]:
    """The plan's record of a dataset; ``files`` are the URLs under ``file_access``."""
    declared = [str(path) for path in dataset.get("files") or []]
    files = [resolve_file_access(path, file_access) for path in declared]
    record: dict[str, Any] = {
        "name": str(dataset["name"]),
        "files": files,
        "nevents": dataset.get("nevents"),
        "file_nevents": dataset.get("file_nevents"),
        "eventtype": dataset.get("eventtype"),
        "group": dataset.get("group"),
        "meta": dict(dataset.get("meta") or {}),
    }
    if dataset.get("chunk_size") is not None:
        record["chunk_size"] = dataset["chunk_size"]
    if files != declared:
        record["logical_files"] = declared
    return record


def build_plan_context(
    plan: ExecutionPlan,
    *,
    datasets_by_name: dict[str, dict[str, Any]] | None = None,
    globals_block: dict[str, Any] | None = None,
    compile_context: CompileContext | None = None,
) -> dict[str, Any]:
    del plan
    datasets_by_name = dict(datasets_by_name or {})
    context = {
        "datasets": datasets_by_name,
        "dataset_names": list(datasets_by_name.keys()),
        "globals": dict(globals_block or {}),
    }
    if compile_context is not None:
        context.update(compile_context.to_normalized())
    return context


def build_execution_partitions(
    plan: ExecutionPlan,
    *,
    chunk_size: int | None = None,
    dataset: str | None = None,
) -> list[ExecutionPartition]:
    source_nodes = [node for node in plan.nodes if node.role == "source"]
    if not source_nodes:
        return []

    partitions: list[ExecutionPartition] = []
    datasets_by_name = dict(plan.context.get("datasets") or {})

    for source_node in source_nodes:
        source_name = str(
            source_node.meta.get("source_name") or source_node.id.removeprefix("read.")
        )
        if not datasets_by_name:
            continue

        local_numbers = per_dataset_enabled(plan.execution)
        for dataset_name, dataset_context in datasets_by_name.items():
            if dataset is not None and dataset_name != dataset:
                continue
            dataset_record = {"name": dataset_name, **dict(dataset_context or {})}
            effective_chunk_size = resolve_chunk_size(
                chunk_size, dataset_record, plan.execution
            )
            if local_numbers:
                plan.context.setdefault("chunk_sizes", {})[dataset_name] = (
                    effective_chunk_size
                )
            partitions.extend(
                derive_dataset_partitions(
                    dataset_record,
                    source_name=source_name,
                    chunk_size=effective_chunk_size,
                    local_numbers=local_numbers,
                )
            )

    return partitions


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
        if _produces_partition_products(outputs):
            return (
                "partition",
                "partition",
                PartitionSpec(mode="dataset_chunks", chunk_size=chunk_size)
                if chunk_size is not None
                else PartitionSpec(mode="dataset"),
                "never",
            )

        if "stream" not in outputs and "event_stream" not in set(outputs.values()):
            return (
                "global",
                "global",
                PartitionSpec(mode="none"),
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


def _produces_partition_products(outputs: dict[str, str]) -> bool:
    product_kinds = set(outputs.values()) - {"event_stream"}
    return bool(product_kinds & {"histogram", "cutflow"})
