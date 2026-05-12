from __future__ import annotations

import os
from typing import Any, Dict, List
import networkx as nx

from hepflow.compiler.exec_dag import ExecDag
from hepflow.compiler.exec_graph import fill_input_aliases
from hepflow.compiler.data_flow import (
    apply_data_flow_to_sources,
    infer_data_flow,
)
from hepflow.compiler.routing import rewrite_fieldmap_for_joins
from hepflow.model.deps import Deps
from hepflow.model.graph import get_graph_node
from hepflow.model.ir import InputRef
from hepflow.model.issues import FlowIssue, IssueLevel
from hepflow.model.plan import (
    ExecNode,
    ExecutionPartition,
    ExecutionNode,
    ExecutionPlan,
    NodeDeps,
    PartitionSpec,
    Plan,
    Paths,
    DatasetEntry,
    Partition,
    PlanDeps,
    PlanInputRef,
    ProductPlan,
    RenderPlan,
)
from hepflow.model.defaults import (
    DEFAULT_CHUNK_SIZE,
    DEFAULT_CONTEXT_SYMBOLS,
    DEFAULT_RESULTS_DIR,
    DEFAULT_WORK_DIR,
)
from hepflow.compiler import styles as sty_comp
from hepflow.model.render import RenderSpec
from hepflow.model.render_types import RenderCommonSpec
from hepflow.registry.loaders import (
    expr_registry_from_config,
    runtime_registry_from_config,
)
from hepflow.registry.defaults import (
    default_expr_registry_config,
    merge_registry_config,
)
from hepflow.model.lifecycle import WHEN_ALIASES, normalize_lifecycle_event


def _partition_file(
    *, dataset: str, file: str, nevents: int, chunk_size: int, file_index: int
) -> List[Partition]:
    parts: List[Partition] = []
    start = 0
    part_idx = 0
    while start < nevents:
        stop = min(start + chunk_size, nevents)
        parts.append(
            Partition(
                dataset=dataset,
                file=file,
                part=f"{file_index}_{part_idx}",
                start=start,
                stop=stop,
            )
        )
        start = stop
        part_idx += 1
    return parts


def _build_render_plan_from_block(
    *,
    node_id: str,
    render_id: str,
    render_block: dict[str, Any],
    style_defs: dict[str, Any],
    datasets: dict[str, Any],
    hist_product_ids: dict[str, str],
    effective_renderreg: dict[str, Any],
    default_product: str | None,
    explicit_inputs: dict[str, str] | None,
) -> tuple[RenderPlan | None, list[FlowIssue]]:
    issues: list[FlowIssue] = []

    when = normalize_lifecycle_event(render_block.get("when") or "final")

    style_ref = render_block.get("style")
    if not style_ref:
        issues.append(
            FlowIssue(
                level=IssueLevel.ERROR,
                code="RENDER_STAGE_MISSING_STYLE",
                message=f"Render '{render_id}' requires style",
                meta={"node_id": node_id, "render_id": render_id},
            )
        )
        return None, issues

    style_spec = sty_comp.resolve_style_ref(style_ref, style_defs)

    render_overrides = dict(render_block)
    render_overrides.pop("style", None)

    out_path = render_overrides.pop("out", None) or f"{render_id}.png"
    select = render_overrides.pop("select", None) or {}

    spec_dict = sty_comp.deep_merge(style_spec, render_overrides)
    spec_dict = sty_comp.resolve_style_tree(spec_dict, style_defs)
    spec_dict = sty_comp.expand_render_transforms(
        spec_dict,
        dataset_entries=datasets,
    )

    render_op = spec_dict.get("op")
    if not isinstance(render_op, str) or not render_op:
        issues.append(
            FlowIssue(
                level=IssueLevel.ERROR,
                code="RENDER_OP_MISSING",
                message=f"Render '{render_id}' requires explicit spec.op",
                meta={"node_id": node_id, "render_id": render_id},
            )
        )
        return None, issues

    entry = effective_renderreg.get(render_op)
    if entry is None:
        issues.append(
            FlowIssue(
                level=IssueLevel.ERROR,
                code="RENDER_OP_UNKNOWN",
                message=f"Unknown render op '{render_op}' for render '{render_id}'",
                meta={"node_id": node_id, "render_id": render_id, "op": render_op},
            )
        )
        return None, issues

    try:
        common = RenderCommonSpec.from_dict(spec_dict)
        render_params = entry.spec.parse_params(spec_dict)
    except Exception as e:
        issues.append(
            FlowIssue(
                level=IssueLevel.ERROR,
                code="RENDER_SPEC_INVALID",
                message=f"Invalid render spec for '{render_id}': {e}",
                meta={
                    "node_id": node_id,
                    "render_id": render_id,
                    "op": render_op,
                    "out": out_path,
                    "error": str(e),
                },
            )
        )
        return None, issues

    effective_categories = sty_comp.resolve_effective_dataset_categories_for_render(
        common,
        dataset_entries=datasets,
    )

    validation_ctx = {
        "available_datasets": sorted(effective_categories),
        "available_products": sorted(hist_product_ids),
        "default_product": default_product,
        "explicit_inputs": explicit_inputs or {},
        "node_id": node_id,
        "render_id": render_id,
    }

    issues.extend(entry.spec.validate(common, render_params, validation_ctx))

    try:
        render_input = entry.spec.resolve_input(common, render_params, validation_ctx)
    except Exception as e:
        issues.append(
            FlowIssue(
                level=IssueLevel.ERROR,
                code="RENDER_INPUT_INVALID",
                message=f"Could not resolve render inputs for '{render_id}': {e}",
                meta={
                    "node_id": node_id,
                    "render_id": render_id,
                    "op": render_op,
                    "error": str(e),
                },
            )
        )
        return None, issues

    plan = RenderPlan(
        id=render_id,
        op=render_op,
        input=render_input,
        when=str(when),
        output=out_path,
        params={
            "spec": spec_dict,
            "select": select,
        },
    )
    return plan, issues


def _explicit_render_inputs_from_in_refs(
    *,
    node_id: str,
    in_refs: list[dict[str, Any]],
) -> tuple[dict[str, str] | None, list[FlowIssue]]:
    issues: list[FlowIssue] = []
    product_map: dict[str, str] = {}

    for ref in in_refs:
        if not isinstance(ref, dict):
            issues.append(
                FlowIssue(
                    level=IssueLevel.ERROR,
                    code="RENDER_STAGE_INPUT_INVALID",
                    message=f"Explicit render stage '{node_id}' has non-dict input ref",
                    meta={"node_id": node_id, "input_ref": ref},
                )
            )
            return None, issues

        src_node = ref.get("node")
        port = ref.get("port")
        alias = ref.get("as")

        if not src_node or port != "hist" or not alias:
            issues.append(
                FlowIssue(
                    level=IssueLevel.ERROR,
                    code="RENDER_STAGE_INPUT_INVALID",
                    message=(
                        f"Explicit render stage '{node_id}' requires inputs of the form "
                        f"{{node: ..., port: 'hist', as: ...}}"
                    ),
                    meta={"node_id": node_id, "input_ref": ref},
                )
            )
            return None, issues

        product_map[str(alias)] = str(src_node)

    if not product_map:
        issues.append(
            FlowIssue(
                level=IssueLevel.ERROR,
                code="RENDER_STAGE_INPUT_MISSING",
                message=f"Explicit render stage '{node_id}' has no valid histogram inputs",
                meta={"node_id": node_id},
            )
        )
        return None, issues

    return product_map, issues


def make_plan(
    norm: Dict[str, Any],
    ir: Dict[str, Any],
    deps: Deps,
    *,
    work_dir: str = DEFAULT_WORK_DIR,
    results_dir: str = DEFAULT_RESULTS_DIR,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> tuple[dict[str, Any], dict[str, list[FlowIssue]]]:
    paths = Paths(work=work_dir, results=results_dir or DEFAULT_RESULTS_DIR)
    paths_resolved, paths_report = paths.resolve(base_dir=os.getcwd())
    results_dir = paths_resolved.results

    primary_stream = ir.get("primary_stream", "events")
    streams = ir.get("streams", {})

    analysis_block = norm.get("analysis") or {}
    globals_block = dict(analysis_block.get("globals") or {})

    # ---- datasets + partitions ----
    datasets: Dict[str, DatasetEntry] = {}
    partitions: List[Partition] = []

    for ds in norm["data"]["datasets"]:
        name = ds["name"]
        files = ds["files"]
        nevents_raw = ds.get("nevents")
        if nevents_raw is None:
            raise ValueError(
                f"Dataset '{name}' missing nevents after inspection-fill step"
            )
        nevents = int(nevents_raw)

        datasets[name] = DatasetEntry(
            files=files,
            nevents=nevents,
            eventtype=ds.get("eventtype", "mc"),
            group=ds.get("group"),
            meta=ds.get("meta", {}),
        )

        for file_index, fpath in enumerate(files):
            partitions.extend(
                _partition_file(
                    dataset=name,
                    file=fpath,
                    nevents=nevents,
                    chunk_size=chunk_size,
                    file_index=file_index,
                )
            )

    # ---- products from IR outputs ----
    products: List[ProductPlan] = []
    for out in ir.get("outputs", []):
        kind = out["kind"]
        node = out["from"]["node"]
        port = out["from"]["port"]

        if kind == "hist":
            products.append(
                ProductPlan(
                    id=node,
                    kind="hist",
                    ext="pkl",
                    ir_node=node,
                    ir_port=port,
                    map={"command": "fill-hist"},
                    reduce={"command": "merge-hists"},
                )
            )
        elif kind == "cutflow":
            prod_id = f"{node}_{port}"
            products.append(
                ProductPlan(
                    id=prod_id,
                    kind="cutflow",
                    ext="json",
                    ir_node=node,
                    ir_port=port,
                    map={"command": "fill-cutflow"},
                    reduce={"command": "merge-cutflows"},
                )
            )
        else:
            raise ValueError(f"Unsupported output kind in plan: {kind}")

    style_defs = sty_comp.collect_styles(norm) if isinstance(norm, dict) else {}

    # ---- render registry ----
    runtime_registry = runtime_registry_from_config(norm["registry"])
    effective_renderreg = runtime_registry.renderers

    # ---- renders from IR graph nodes ----
    renders: List[RenderPlan] = []
    render_issues: list[FlowIssue] = []

    hist_product_ids = {p.ir_node: p.id for p in products if p.kind == "hist"}
    graph_nodes = ir.get("graph", [])

    for node in graph_nodes:
        nid = node["id"]
        op = node.get("op")

        # ------------------------------------------------------------
        # Case A: inline render blocks attached to hist-producing nodes
        # ------------------------------------------------------------
        if nid in hist_product_ids and "render" in node:
            rblocks = node["render"]
            if isinstance(rblocks, dict):
                rblocks = [rblocks]
            if not isinstance(rblocks, list):
                raise ValueError(f"IR node '{nid}' render must be list/dict")

            for i, r in enumerate(rblocks):
                if not isinstance(r, dict):
                    raise ValueError(f"IR node '{nid}' render entry must be dict")

                plan_render, issues = _build_render_plan_from_block(
                    node_id=nid,
                    render_id=f"render_{nid}_{i}",
                    render_block=dict(r),
                    style_defs=style_defs,
                    datasets=datasets,
                    hist_product_ids=hist_product_ids,
                    effective_renderreg=effective_renderreg,
                    default_product=nid,
                    explicit_inputs=None,
                )
                render_issues.extend(issues)
                if plan_render is not None:
                    renders.append(plan_render)

        # ------------------------------------------------------------
        # Case B: explicit render-only stages in the IR graph
        # ------------------------------------------------------------
        elif op == "hep.render.plot":
            params = dict(node.get("params") or {})
            in_refs = node.get("in") or []

            product_map, issues = _explicit_render_inputs_from_in_refs(
                node_id=nid,
                in_refs=in_refs,
            )
            render_issues.extend(issues)
            if not product_map:
                continue

            plan_render, issues = _build_render_plan_from_block(
                node_id=nid,
                render_id=nid,
                render_block=params,
                style_defs=style_defs,
                datasets=datasets,
                hist_product_ids=hist_product_ids,
                effective_renderreg=effective_renderreg,
                default_product=None,
                explicit_inputs=product_map,
            )
            render_issues.extend(issues)
            if plan_render is not None:
                renders.append(plan_render)

    # ---- exec graph ----
    exec_nodes: list[ExecNode] = []
    for n in ir.get("graph", []):
        nid = str(n["id"])
        in_refs = tuple(InputRef.from_dict(ref) for ref in (n.get("in") or []))
        if nid not in deps.required_symbols_per_node:
            raise ValueError(
                f"Missing required_symbols_per_node entry for node '{nid}'"
            )
        if nid not in deps.provides_symbols_per_node:
            raise ValueError(
                f"Missing provides_symbols_per_node entry for node '{nid}'"
            )
        node_deps = NodeDeps(
            requires=deps.required_symbols_per_node.get(nid),
            provides=deps.provides_symbols_per_node.get(nid),
        )
        exec_nodes.append(
            ExecNode(
                id=nid,
                op=str(n["op"]),
                in_=in_refs,
                params=dict(n.get("params") or {}),
                out=dict(n.get("out") or {}),
                deps=node_deps,
            )
        )

    exec_graph: tuple[ExecNode, ...] = tuple(exec_nodes)
    exec_graph = fill_input_aliases(exec_graph)

    fieldmap_ir = ir.get("fieldmap") or {}
    fieldmap_plan = rewrite_fieldmap_for_joins(fieldmap=fieldmap_ir, streams=streams)

    plan_deps = PlanDeps(
        context_symbols=tuple(set(deps.context_symbols) | DEFAULT_CONTEXT_SYMBOLS),
        external_symbols=deps.external_symbols,
    )

    plan = Plan(
        version="2.1",
        paths=paths_resolved,
        datasets=datasets,
        partitions=partitions,
        primary_stream=primary_stream,
        streams=streams,
        required_inputs=deps.required_inputs,
        products=products,
        renders=renders,
        fieldmap=fieldmap_plan,
        reports={"paths": paths_report},
        exec_graph=exec_graph,
        deps=plan_deps,
        globals=globals_block,
        registry=norm["registry"],
    )

    plan_dict = plan.to_dict()

    exec_graph_issues = analyse_plan_exec_graph(plan_dict)
    dag = ExecDag.from_plan(plan_dict)
    mermaid = dag.to_mermaid()

    plan_dict.setdefault("reports", {}).setdefault("exec_graph", {})
    plan_dict["reports"]["exec_graph"]["mermaid"] = mermaid

    all_issues: dict[str, list[FlowIssue]] = {
        "exec_graph": exec_graph_issues,
        "render_spec": render_issues,
    }
    return plan_dict, all_issues


def analyse_plan_exec_graph(plan: dict[str, Any]) -> list[FlowIssue]:
    """
    Returns a list of report messages (json-friendly dicts).
    Raise on hard errors (cycle, unknown node refs).
    """
    msgs: list[FlowIssue] = []
    dag = ExecDag.from_plan(plan)

    if not dag.is_dag():
        try:
            cyc = nx.find_cycle(dag.g)
        except Exception:
            cyc = []
        raise ValueError(f"exec_graph contains a cycle: {cyc}")

    eg = plan.get("exec_graph") or []
    given_order = [str(n.get("id")) for n in eg]
    topo = dag.topo_order()
    if given_order != topo:
        msgs.append(
            FlowIssue(
                level=IssueLevel.INFO,
                code="EXEC_GRAPH_NOT_TOPO",
                message="exec_graph is not in topological order; runtime may fail if it assumes order.",
                meta={"given": given_order, "topo": topo},
            )
        )

    comps = list(nx.weakly_connected_components(dag.g))
    if len(comps) > 1:
        msgs.append(
            FlowIssue(
                level=IssueLevel.INFO,
                code="EXEC_GRAPH_MULTIPLE_COMPONENTS",
                message="exec_graph has multiple disconnected components (independent branches).",
                meta={"components": [sorted(list(c)) for c in comps]},
            )
        )

    analysis = dag.analyze_dataflow(plan)
    msgs.extend(analysis.issues)

    return msgs


def format_validation_messages(
    msgs: list[FlowIssue],
    min_level: IssueLevel = IssueLevel.INFO,
) -> str:
    """
    Human-readable report for console/exception messages.
    """
    selected = [i for i in msgs if i.level <= min_level]
    selected.sort(key=lambda i: (i.level, i.code))
    return "\n".join(i.format() for i in selected)


def raise_on_errors(msgs: list[FlowIssue], *, context: str = "plan validation") -> None:
    errors = [m for m in msgs if m.is_error()]
    if not errors:
        return
    report = format_validation_messages(errors, min_level=IssueLevel.ERROR)
    raise ValueError(f"{context} failed with {len(errors)} error(s):\n{report}")


def _render_input_from_ir_inputs(
    *,
    node_id: str,
    in_refs: list[dict[str, Any]],
) -> dict[str, Any]:
    """
    Convert IR node inputs for a render stage into plan render input.

    Rules for now:
    - explicit render stages must consume upstream node outputs
    - each input ref should have `node`, `port`, and `as`
    - single input with alias 'hist' could be represented as {'product': ...}
      but to keep runtime simple we normalize everything to:
        {'products': {alias: product_id}}
    """
    if not isinstance(in_refs, list) or not in_refs:
        raise ValueError(f"Render stage '{node_id}' requires non-empty 'in' list")

    products: dict[str, str] = {}

    for ref in in_refs:
        if not isinstance(ref, dict):
            raise ValueError(f"Render stage '{node_id}' input refs must be dicts")

        src_node = ref.get("node")
        port = ref.get("port")
        alias = ref.get("as")

        if not src_node or not port:
            raise ValueError(
                f"Render stage '{node_id}' input must reference upstream node+port, got {ref}"
            )

        if port != "hist":
            raise ValueError(
                f"Render stage '{node_id}' currently only supports upstream port='hist', got {port!r}"
            )

        if not alias:
            raise ValueError(
                f"Render stage '{node_id}' requires 'as' on each input ref for stable render input mapping"
            )

        products[str(alias)] = str(src_node)

    return {"products": products}


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
    plan.execution = {
        "backend": "local",
        "strategy": "default",
        "config": {},
        **dict(execution or {}),
    }
    plan.execution["config"] = dict(plan.execution.get("config") or {})
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
                input_scope=input_scope,
                output_scope=output_scope,
                partitioning=partitioning,
                materialize=materialize,
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
                                id=f"{source_name}__{dataset_name}__{file_index}_{chunk_index}",
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
    """
    Very first-pass policy.

    This should stay dumb at first; refine once the new path is exercised.
    """
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
        # Histogram-like transforms are special because they usually produce
        # partition-local reducible products.
        if outputs == {"hist": "histogram"}:
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
        return (
            "global",
            "global",
            PartitionSpec(mode="none"),
            "always",
        )

    if role == "sink":
        return (
            "global",
            "global",
            PartitionSpec(mode="none"),
            "always",
        )

    raise ValueError(f"Unknown execution role: {role!r}")
