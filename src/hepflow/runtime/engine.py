from __future__ import annotations

from typing import Any, Dict
import networkx as nx

from hepflow.model.defaults import DEFAULT_PRIMARY_STREAM_ID
from hepflow.model.graph import get_graph_node, upstream_binding
from hepflow.model.plan import (
    ExecutionNode,
    ExecutionPartition,
    ExecutionPlan,
    resolve_plan_ref,
)
from hepflow.registry.defaults import default_expr_registry
from hepflow.registry.loaders import (
    expr_registry_from_config,
    runtime_registry_from_config,
)
from hepflow.registry.runtime import RuntimeRegistry
from hepflow.runtime.handlers import run_observer, run_sink, run_source, run_transform
from hepflow.runtime.hooks.manager import HookDispatchError, HookManager
from hepflow.model.lifecycle import normalize_lifecycle_event
from hepflow.runtime.merge import merge_hists
from hepflow.runtime.stream_readers import read_stream


# TODO:
# Event-stream merging should become package-owned via merge strategies.
# fasthep-flow should not contain awkward-specific logic long term.
def _awkward():
    import awkward as ak  # noqa: PLC0415

    return ak


def merge_records(base: Any, extra: Any, *, prefer_extra: bool = True) -> Any:
    """
    Merge two awkward RecordArrays at the top level.
    - base: usually raw stream (tree/aliases)
    - extra: usually derived fields from upstream stage
    prefer_extra=True => if both define field 'x', use extra['x'].
    """
    if not (hasattr(base, "fields") and hasattr(extra, "fields")):
        # If either isn't record-like, just return extra if preferred, else base
        return extra if prefer_extra else base

    base_cols = {k: base[k] for k in base.fields}
    extra_cols = {k: extra[k] for k in extra.fields}

    if prefer_extra:
        merged = {**base_cols, **extra_cols}
    else:
        merged = {**extra_cols, **base_cols}

    return _awkward().zip(merged, depth_limit=1)


def resolve_runtime_op_handler(runtime_registry: RuntimeRegistry, op: str):
    entry = runtime_registry.ops.get(op)
    if entry is None:
        raise KeyError(f"No runtime registry entry for op '{op}'")
    handler = entry.handler
    if handler is None:
        raise KeyError(f"No handler registered for op '{op}'")
    return handler


def compute_product_partition(
    *,
    plan: Dict[str, Any],
    op_registry: Dict[str, Any],  # unused for now
    product_node_id: str,
    product_port: str,  # e.g. "hist" or "cutflow"
    partition: Dict[str, Any],  # {dataset,file,part,start,stop}
    expr_registry,
) -> Any:
    """
    Execute IR graph for a single partition and return the requested product.

    v4 semantics:
    - Node inputs are explicit: node["in"] is a list of InputRef dicts.
    - Each node handler is called as: handler(data: dict[str,Any], params: dict, ctx: dict) -> dict[str,Any]
    - Produced outputs are stored by (node_id, port_name).
    - Streams are loaded on-demand via read_stream(plan, stream_id, file, start, stop).
    """
    dataset = partition["dataset"]
    file_path = partition["file"]
    start, stop = partition["start"], partition["stop"]
    raw_primary = None
    dataset_names = list((plan.get("datasets") or {}).keys())

    primary_stream = plan.get("primary_stream", DEFAULT_PRIMARY_STREAM_ID)
    runtime_registry = runtime_registry_from_config(plan["registry"])

    ds_cfg = dict((plan.get("datasets") or {}).get(dataset) or {})
    ds_meta_extra = dict(ds_cfg.get("meta") or {})

    dataset_meta: Dict[str, Any] = {
        "name": dataset,
        "group": ds_cfg.get("group"),
        "nevents": ds_cfg.get("nevents"),
        "eventtype": ds_cfg.get("eventtype"),
        **ds_meta_extra,
    }

    globals_block = dict(plan.get("globals") or {})

    ctx: Dict[str, Any] = {
        "dataset_name": dataset,
        "dataset": dataset,
        "file": file_path,
        "entries": (start, stop),
        "primary_stream": primary_stream,
        "dataset_names": dataset_names,
        # structured access
        "globals": globals_block,
        "dataset_meta": dataset_meta,
        "expr_registry": expr_registry,
    }

    # flatten globals directly
    for k, v in globals_block.items():
        ctx[k] = v

    # flatten dataset metadata with prefix
    for k, v in dataset_meta.items():
        ctx[f"dataset_{k}"] = v

    # convenience flag
    ctx["dataset_is_data"] = str(dataset_meta.get("eventtype", "")).lower() == "data"

    # Cache loaded streams for this partition (avoid re-reading the same tree)
    stream_cache: Dict[str, Any] = {}

    # All produced node outputs for this partition: (node_id, port) -> value
    produced: Dict[tuple[str, str], Any] = {}

    found_product: Any = None

    graph = plan.get("exec_graph") or []
    if not isinstance(graph, list):
        raise TypeError("Plan 'exec_graph' must be a list")

    for node in graph:
        nid = node["id"]
        op = node["op"]
        params = node.get("params", {}) or {}
        data: Dict[str, Any] = {}  # Build runtime inputs dict for this node
        upstream_events = None

        is_render = str(op).startswith("hep.render.")
        if is_render:
            continue

        handler = resolve_runtime_op_handler(runtime_registry, op)

        in_refs = node.get("in") or []
        if not isinstance(in_refs, list):
            raise TypeError(f"IR node '{nid}' field 'in' must be a list of input refs")

        for ref in in_refs:
            if not isinstance(ref, dict):
                raise TypeError(
                    f"IR node '{nid}' input ref must be a dict, got {type(ref).__name__}"
                )

            alias = ref.get("as")  # optional alias in runtime dict

            if "stream" in ref and ref.get("stream"):
                sid = str(ref["stream"])
                if sid not in stream_cache:
                    stream_cache[sid] = read_stream(plan, sid, file_path, start, stop)
                val = stream_cache[sid]
                key = str(alias) if alias else sid
                data[key] = val
                continue

            nsrc = ref.get("node")
            port = ref.get("port")
            if nsrc and port:
                key2 = (str(nsrc), str(port))
                if key2 not in produced:
                    raise KeyError(
                        f"IR node '{nid}' requires '{nsrc}.{port}', but it is not available yet "
                        f"(missing upstream output or wrong node order)."
                    )
                val = produced[key2]
                key = str(alias) if alias else f"{nsrc}.{port}"
                data[key] = val
                # capture upstream event-stream if present
                if str(port) == "events" and upstream_events is None:
                    upstream_events = val
                continue

            raise ValueError(
                f"IR node '{nid}' has invalid input ref. Expected either "
                f"{{stream: ...}} or {{node: ..., port: ...}}. Got: {ref}"
            )

        if raw_primary is None:
            # cache raw primary once per partition
            if primary_stream not in stream_cache:
                stream_cache[primary_stream] = read_stream(
                    plan, primary_stream, file_path, start, stop
                )
            raw_primary = stream_cache[primary_stream]

        raw_for_node = data.get(primary_stream, raw_primary)
        # Merge: raw tree/aliases + derived fields from upstream stage
        if upstream_events is not None:
            virtual = merge_records(raw_for_node, upstream_events, prefer_extra=True)
        else:
            virtual = raw_for_node
        data[primary_stream] = virtual
        # also under default ID for convenience
        data[DEFAULT_PRIMARY_STREAM_ID] = virtual

        out = handler(data, params, ctx)
        # If op produced an event stream, ensure it keeps raw+derived fields
        if "events" in out and hasattr(out["events"], "fields"):
            out["events"] = merge_records(
                data[primary_stream],
                out["events"],
                prefer_extra=True,
            )

        if not isinstance(out, dict):
            raise TypeError(
                f"Handler for op '{op}' (node '{nid}') must return a dict, got {type(out).__name__}"
            )

        # Store all outputs as produced ports
        for port_name, value in out.items():
            produced[(nid, str(port_name))] = value

        # Capture requested product
        if nid == product_node_id:
            keyp = (nid, product_port)
            if keyp not in produced:
                raise KeyError(
                    f"Node '{nid}' did not produce requested port '{product_port}'. "
                    f"Produced ports: {sorted(k[1] for k in produced.keys() if k[0] == nid)}"
                )
            found_product = produced[keyp]

    if found_product is None:
        raise KeyError(
            f"Product node '{product_node_id}' not found in IR graph or did not produce '{product_port}'"
        )

    return found_product


def run_partition(plan: dict, partition: dict) -> dict:

    primary_stream = plan.get("primary_stream", "events")
    file_path = partition["file"]
    start, stop = partition["start"], partition["stop"]

    events = read_stream(plan, primary_stream, file_path, start, stop)
    ctx = {"dataset_name": partition["dataset"]}
    runtime_registry = runtime_registry_from_config(plan["registry"])

    products = {}

    for node in plan["graph"]:
        op = node["op"]
        params = node.get("params") or {}
        is_render = str(op).startswith("hep.render.")
        if is_render:
            continue
        handler = resolve_runtime_op_handler(runtime_registry, op)
        if handler is None:
            raise KeyError(f"No handler registered for op '{op}'")

        out = handler(events, params, ctx)

        # Convention: ops may return {"events": <ak.Array>, ...}
        # If present, this is the new stream for downstream ops.
        if isinstance(out, dict) and "events" in out:
            events = out["events"]

        # Any non-stream outputs are products/side outputs
        if isinstance(out, dict):
            for k, v in out.items():
                if k != "events":
                    products[k] = v

    return products


def build_expr_scope(
    data: Any,
    ctx: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if hasattr(data, "fields"):
        scope = {k: data[k] for k in data.fields}
    elif isinstance(data, dict):
        scope = dict(data)
    else:
        raise TypeError(
            f"Cannot build expression scope from {type(data).__name__}; "
            "expected object with .fields or mapping"
        )

    reg = (ctx or {}).get("expr_registry") or default_expr_registry()
    scope.update(reg.functions)
    scope.update(reg.constants)
    if ctx:
        scope.update(ctx)
    return scope


def eval_expr(
    events: Any,
    expr: str,
    ctx: dict[str, Any] | None = None,
) -> Any:
    """
    v1 expression evaluator:
    - intended for internal prototype use
    - uses eval with restricted builtins
    - variables resolve to event columns

    TODO: move expression evaluation into a package-owned expression helper
    layer once the extension boundary settles.
    """
    scope = build_expr_scope(events, ctx)
    expr = expr.replace("&&", " and ").replace("||", " or ").strip()
    try:
        return eval(expr, {"__builtins__": {}}, scope)
    except NameError as exc:
        symbols = sorted(str(k) for k in scope.keys())
        shown = symbols[:50]
        suffix = " ..." if len(symbols) > 50 else ""
        raise NameError(
            f"{exc}. While evaluating expression {expr!r}. "
            f"Available symbols include: {shown}{suffix}"
        ) from exc


def execute_graph_sink(
    *,
    graph,
    sink_node_id: str,
    value_store: dict[tuple[str, str], Any],
    ctx: dict[str, Any] | None = None,
    registry_cfg: dict[str, Any] | None = None,
) -> Any:
    node = get_graph_node(graph, sink_node_id)

    if node.role != "sink":
        raise ValueError(f"Node {sink_node_id!r} is not a sink")

    inputs = _collect_graph_inputs(graph, sink_node_id, value_store)
    if not inputs:
        raise ValueError(f"Sink node {sink_node_id!r} has no input bindings")
    target = _sink_target(inputs)

    result = run_sink(
        sink_name=node.impl,
        target=target,
        params=node.params,
        ctx=dict(ctx or {}),
        meta={**dict(node.meta or {}), "node_id": node.id},
        registry_cfg=registry_cfg,
    )

    value_store[(sink_node_id, "artifact")] = result
    return result


def execute_graph_observer(
    *,
    graph,
    observer_node_id: str,
    value_store: dict[tuple[str, str], Any],
    ctx: dict[str, Any] | None = None,
    registry_cfg: dict[str, Any] | None = None,
) -> Any:
    node = get_graph_node(graph, observer_node_id)

    if node.role != "observer":
        raise ValueError(f"Node {observer_node_id!r} is not an observer node")

    binding = upstream_binding(graph, observer_node_id, "target")
    if binding is None:
        raise ValueError(
            f"Observer node {observer_node_id!r} does not have a bound 'target' input"
        )

    upstream_node_id, output_name = binding

    try:
        target = value_store[(upstream_node_id, output_name)]
    except KeyError as exc:
        raise KeyError(
            "Missing upstream runtime value for observer target: "
            f"{upstream_node_id}.{output_name}"
        ) from exc

    result = run_observer(
        observer_name=node.impl,
        target=target,
        params=node.params,
        registry_cfg=registry_cfg,
        ctx=ctx,
    )

    output_port = next(iter(node.outputs.keys()), "report")
    value_store[(observer_node_id, output_port)] = result
    return result


def execute_graph_source(
    *,
    graph: nx.DiGraph,
    source_node_id: str,
    value_store: dict[tuple[str, str], Any],
    registry_cfg: dict[str, Any] | None = None,
    ctx: dict[str, Any] | None = None,
) -> Any:
    """
    Execute a single source node from the lowered graph.
    """
    node = get_graph_node(graph, source_node_id)

    if node.role != "source":
        raise ValueError(f"Node {source_node_id!r} is not a source node")

    params = _resolve_source_params(node.params, plan=None, plan_ctx=ctx)

    result = run_source(
        source_name=node.impl,
        params=params,
        registry_cfg=registry_cfg,
        ctx=ctx,
    )

    value_store[(source_node_id, "stream")] = result
    return result


def execute_graph_transform(
    *,
    graph: nx.DiGraph,
    transform_node_id: str,
    value_store: dict[tuple[str, str], Any],
    registry_cfg: dict[str, Any] | None = None,
) -> Any:
    """
    Execute a single transform node from the lowered graph.

    Inputs are collected from incoming graph edges using each edge's
    `input_name` and upstream `output` metadata.
    """
    node = get_graph_node(graph, transform_node_id)

    if node.role != "transform":
        raise ValueError(f"Node {transform_node_id!r} is not a transform node")

    inputs: dict[str, Any] = {}

    for upstream_node_id, _, edge_data in graph.in_edges(transform_node_id, data=True):
        input_name = str(edge_data.get("input_name") or "stream")
        output_name = str(edge_data.get("output") or "stream")

        try:
            value = value_store[(upstream_node_id, output_name)]
        except KeyError as exc:
            raise KeyError(
                "Missing upstream runtime value for transform input: "
                f"{upstream_node_id}.{output_name} -> {transform_node_id}.{input_name}"
            ) from exc

        if input_name in inputs:
            raise ValueError(
                f"Duplicate bound transform input {input_name!r} "
                f"for node {transform_node_id!r}"
            )

        inputs[input_name] = value

    result = run_transform(
        transform_name=node.impl,
        inputs=inputs,
        params=node.params,
        registry_cfg=registry_cfg,
    )
    _store_node_outputs(transform_node_id, node.outputs, result, value_store)
    output_names = list(node.outputs.keys())
    if len(output_names) == 1:
        return _single_output_return_value(result, output_names[0])
    if isinstance(result, dict) and set(result.keys()) == set(output_names):
        return {name: value_store[(transform_node_id, name)] for name in output_names}
    return result


def execute_plan_partition(
    plan: ExecutionPlan,
    *,
    ctx: dict[str, Any],
    registry_cfg: dict[str, Any] | None = None,
    initial_values: dict[tuple[str, str], Any] | None = None,
    skip_roles: set[str] | None = None,
    hook_manager: HookManager | None = None,
) -> dict[tuple[str, str], Any]:
    """
    Very small first executor for the new execution plan.

    No parallelism yet.
    No merge steps yet.
    Assumes graph/plan ordering is already topological.
    """
    ctx = dict(ctx or {})
    registry_cfg = registry_cfg or plan.registry
    _ensure_expr_registry(ctx, registry_cfg)
    value_store: dict[tuple[str, str], Any] = dict(initial_values or {})
    skip_roles = set(skip_roles or set())
    hook_manager = hook_manager or HookManager.from_plan(plan)
    partition = ctx.get("partition")
    hook_manager.partition_start(partition=partition, ctx=ctx)

    for node in plan.nodes:
        if node.role in skip_roles:
            continue
        inputs: dict[str, Any] = {}
        try:
            if node.role == "source":
                with hook_manager.around_node(node=node, inputs=inputs, ctx=ctx):
                    hook_manager.before_node(node=node, inputs=inputs, ctx=ctx)
                    params = _resolve_source_params(node.params, plan=plan, plan_ctx=ctx)
                    result = run_source(
                        source_name=node.impl,
                        params=params,
                        registry_cfg=registry_cfg,
                        ctx=ctx,
                    )
                    _store_node_outputs(node.id, node.outputs, result, value_store)
                    hook_manager.after_node(
                        node=node,
                        inputs=inputs,
                        outputs=result,
                        ctx=ctx,
                    )
                continue

            inputs = _collect_inputs(node.inputs, value_store)

            if node.role == "transform":
                with hook_manager.around_node(node=node, inputs=inputs, ctx=ctx):
                    hook_manager.before_node(node=node, inputs=inputs, ctx=ctx)
                    result = run_transform(
                        transform_name=node.impl,
                        inputs=inputs,
                        params=node.params,
                        registry_cfg=registry_cfg,
                        ctx=ctx,
                    )
                    _store_node_outputs(node.id, node.outputs, result, value_store)
                    hook_manager.after_node(
                        node=node,
                        inputs=inputs,
                        outputs=result,
                        ctx=ctx,
                    )
                continue

            if node.role == "observer":
                with hook_manager.around_node(node=node, inputs=inputs, ctx=ctx):
                    hook_manager.before_node(node=node, inputs=inputs, ctx=ctx)
                    target = _default_target(inputs)
                    result = run_observer(
                        observer_name=node.impl,
                        target=target,
                        params=node.params,
                        registry_cfg=registry_cfg,
                        ctx=ctx,
                    )
                    _store_node_outputs(node.id, node.outputs, result, value_store)
                    hook_manager.after_node(
                        node=node,
                        inputs=inputs,
                        outputs=result,
                        ctx=ctx,
                    )
                continue

            if node.role == "sink":
                when = _sink_when(node)
                if when in {"dataset_end", "run_end"}:
                    continue
                if when != "partition_end":
                    raise ValueError(
                        f"Unsupported sink execution timing for node {node.id!r}: {when!r}"
                    )
                with hook_manager.around_node(node=node, inputs=inputs, ctx=ctx):
                    hook_manager.before_node(node=node, inputs=inputs, ctx=ctx)
                    target = _sink_target(inputs)
                    result = run_sink(
                        sink_name=node.impl,
                        target=target,
                        params=node.params,
                        ctx=ctx,
                        meta=_node_meta(node),
                        registry_cfg=registry_cfg,
                    )
                    _store_node_outputs(node.id, node.outputs, result, value_store)
                    hook_manager.after_node(
                        node=node,
                        inputs=inputs,
                        outputs=result,
                        ctx=ctx,
                    )
                continue

            raise ValueError(f"Unknown plan node role: {node.role!r}")
        except Exception as exc:
            _dispatch_node_error(
                hook_manager=hook_manager,
                node=node,
                inputs=inputs,
                ctx=ctx,
                exc=exc,
            )
            raise

    hook_manager.partition_end(partition=partition, ctx=ctx, value_store=value_store)
    return value_store


def _dispatch_node_error(
    *,
    hook_manager: HookManager,
    node: ExecutionNode,
    inputs: dict[str, Any],
    ctx: dict[str, Any],
    exc: BaseException,
) -> None:
    if not hook_manager.has_event("on_node_error"):
        print(
            f"Runtime error in node {node.id}: "
            f"{type(exc).__name__}: {exc}"
        )
        return
    try:
        hook_manager.on_node_error(node=node, inputs=inputs, ctx=ctx, exc=exc)
    except HookDispatchError as hook_exc:
        print(f"Error hook {hook_exc.kind} failed: {hook_exc.cause}")


def execute_plan_locally(
    plan: ExecutionPlan,
    *,
    registry_cfg: dict[str, Any] | None = None,
    ctx: dict[str, Any] | None = None,
    initial_values: dict[tuple[str, str], Any] | None = None,
    skip_roles: set[str] | None = None,
    partitions: list[ExecutionPartition] | None = None,
) -> Any:
    """
    Execute an execution plan locally, optionally once per partition.
    """
    registry_cfg = registry_cfg or plan.registry
    base_ctx = dict(plan.context)
    base_ctx.update(dict(ctx or {}))
    base_ctx.update(dict(base_ctx.get("globals") or {}))
    _ensure_expr_registry(base_ctx, registry_cfg)
    if "dataset_names" not in base_ctx:
        base_ctx["dataset_names"] = list((base_ctx.get("datasets") or {}).keys())
    hook_manager = HookManager.from_plan(plan)

    if partitions is None:
        value_store = execute_plan_partition(
            plan,
            ctx=base_ctx,
            registry_cfg=registry_cfg,
            initial_values=initial_values,
            skip_roles=skip_roles,
            hook_manager=hook_manager,
        )
        execute_final_nodes(
            plan,
            value_store=value_store,
            ctx=base_ctx,
            registry_cfg=registry_cfg,
            skip_roles=skip_roles,
            hook_manager=hook_manager,
        )
        hook_manager.run_end(plan=plan, ctx=base_ctx, summary={})
        if isinstance(ctx, dict):
            ctx["_hook_summary"] = base_ctx.get("_hook_summary")
        return value_store

    results: list[dict[tuple[str, str], Any]] = []
    for partition in partitions:
        partition_ctx = build_partition_context(
            plan,
            base_ctx=base_ctx,
            partition=partition,
        )
        results.append(
            execute_plan_partition(
                plan,
                ctx=partition_ctx,
                registry_cfg=registry_cfg,
                initial_values=initial_values,
                skip_roles=skip_roles,
                hook_manager=hook_manager,
            )
        )

    dataset_stores: list[dict[tuple[str, str], Any]] = []
    grouped_results = group_partition_results_by_dataset(results, partitions)
    for dataset_name, stores in grouped_results.items():
        dataset_value_store = merge_partition_value_stores_for_dataset(plan, stores)
        dataset_ctx = build_dataset_context(
            plan,
            base_ctx=base_ctx,
            dataset_name=dataset_name,
        )
        execute_dataset_sinks(
            plan,
            dataset_name=dataset_name,
            dataset_value_store=dataset_value_store,
            ctx=dataset_ctx,
            registry_cfg=registry_cfg,
            skip_roles=skip_roles,
            hook_manager=hook_manager,
        )
        dataset_stores.append(dataset_value_store)

    merged_value_store = merge_partition_value_stores(plan, dataset_stores)
    execute_final_nodes(
        plan,
        value_store=merged_value_store,
        ctx=base_ctx,
        registry_cfg=registry_cfg,
        skip_roles=skip_roles,
        hook_manager=hook_manager,
    )
    hook_manager.run_end(plan=plan, ctx=base_ctx, summary={})
    if isinstance(ctx, dict):
        ctx["_hook_summary"] = base_ctx.get("_hook_summary")
    return results


def build_partition_context(
    plan: ExecutionPlan,
    *,
    base_ctx: dict[str, Any],
    partition: ExecutionPartition,
) -> dict[str, Any]:
    datasets = dict(base_ctx.get("datasets") or {})
    globals_block = dict(base_ctx.get("globals") or {})
    return {
        **base_ctx,
        **globals_block,
        "partition": partition.to_context(),
        "dataset_name": partition.dataset,
        "dataset": datasets.get(
            partition.dataset,
            {"name": partition.dataset},
        ),
    }


def build_dataset_context(
    plan: ExecutionPlan,
    *,
    base_ctx: dict[str, Any],
    dataset_name: str,
) -> dict[str, Any]:
    datasets = dict(base_ctx.get("datasets") or {})
    globals_block = dict(base_ctx.get("globals") or {})
    return {
        **base_ctx,
        **globals_block,
        "dataset_name": dataset_name,
        "dataset": datasets.get(dataset_name, {"name": dataset_name}),
        "scope": "dataset",
    }


def _ensure_expr_registry(
    ctx: dict[str, Any],
    registry_cfg: dict[str, Any] | None,
) -> None:
    if ctx.get("expr_registry") is not None:
        return
    registry_cfg = dict(registry_cfg or {})
    if registry_cfg.get("functions") or registry_cfg.get("constants"):
        ctx["expr_registry"] = expr_registry_from_config(registry_cfg)
        return
    ctx["expr_registry"] = default_expr_registry()


def merge_partition_value_stores(
    plan: ExecutionPlan,
    stores: list[dict[tuple[str, str], Any]],
) -> dict[tuple[str, str], Any]:
    merged: dict[tuple[str, str], Any] = {}
    grouped: dict[tuple[str, str], list[Any]] = {}

    for store in stores:
        for key, value in store.items():
            grouped.setdefault(key, []).append(value)

    for key, values in grouped.items():
        node_id, output_name = key
        try:
            node = plan.get_node(node_id)
            output_kind = node.outputs.get(output_name)
        except KeyError:
            output_kind = None

        if output_kind == "histogram":
            merged[key] = merge_hists(values)
            continue

        if output_kind == "report":
            merged[key] = list(values)
            continue

        if output_kind == "event_stream":
            merged[key] = values[0] if len(values) == 1 else list(values)
            continue

        merged[key] = values[0] if len(values) == 1 else list(values)

    return merged


def group_partition_results_by_dataset(
    partition_results: list[dict[tuple[str, str], Any]],
    partitions: list[ExecutionPartition],
) -> dict[str, list[dict[tuple[str, str], Any]]]:
    grouped: dict[str, list[dict[tuple[str, str], Any]]] = {}
    for result, partition in zip(partition_results, partitions):
        grouped.setdefault(partition.dataset, []).append(result)
    return grouped


def merge_partition_value_stores_for_dataset(
    plan: ExecutionPlan,
    stores: list[dict[tuple[str, str], Any]],
) -> dict[tuple[str, str], Any]:
    merged: dict[tuple[str, str], Any] = {}
    grouped: dict[tuple[str, str], list[Any]] = {}

    for store in stores:
        for key, value in store.items():
            grouped.setdefault(key, []).append(value)

    for key, values in grouped.items():
        node_id, output_name = key
        try:
            node = plan.get_node(node_id)
            output_kind = node.outputs.get(output_name)
        except KeyError:
            output_kind = None

        if output_kind == "histogram":
            merged[key] = merge_hists(values)
            continue

        if output_kind == "event_stream":
            merged[key] = _merge_event_stream_values(values)
            continue

        if output_kind == "report":
            merged[key] = list(values)
            continue

        merged[key] = values[0] if len(values) == 1 else list(values)

    return merged


def _merge_event_stream_values(values: list[Any]) -> Any:
    if len(values) == 1:
        return values[0]
    if all(_is_awkward_array_like(value) for value in values):
        ak = _awkward()
        return ak.concatenate(values)
    return list(values)


def _is_awkward_array_like(value: Any) -> bool:
    cls = type(value)
    return cls.__module__.startswith("awkward.") and cls.__name__ == "Array"


def execute_dataset_sinks(
    plan: ExecutionPlan,
    *,
    dataset_name: str,
    dataset_value_store: dict[tuple[str, str], Any],
    ctx: dict[str, Any],
    registry_cfg: dict[str, Any] | None = None,
    skip_roles: set[str] | None = None,
    hook_manager: HookManager | None = None,
) -> dict[tuple[str, str], Any]:
    skip_roles = set(skip_roles or set())
    hook_manager = hook_manager or HookManager.from_plan(plan)

    for node in plan.nodes:
        if node.role in skip_roles or node.role != "sink":
            continue

        when = _sink_when(node)
        if when in {"partition_end", "run_end"}:
            continue
        if when != "dataset_end":
            raise ValueError(
                f"Unsupported sink execution timing for node {node.id!r}: {when!r}"
            )

        inputs = _collect_inputs(node.inputs, dataset_value_store)
        target = _sink_target(inputs)
        try:
            with hook_manager.around_node(node=node, inputs=inputs, ctx=ctx):
                hook_manager.before_node(node=node, inputs=inputs, ctx=ctx)
                result = run_sink(
                    sink_name=node.impl,
                    target=target,
                    params=node.params,
                    ctx=ctx,
                    meta=_node_meta(node),
                    registry_cfg=registry_cfg,
                )
                _store_node_outputs(node.id, node.outputs, result, dataset_value_store)
                hook_manager.after_node(
                    node=node,
                    inputs=inputs,
                    outputs=result,
                    ctx=ctx,
                )
        except Exception as exc:
            _dispatch_node_error(
                hook_manager=hook_manager,
                node=node,
                inputs=inputs,
                ctx=ctx,
                exc=exc,
            )
            raise

    hook_manager.dataset_end(
        dataset_name=dataset_name,
        ctx=ctx,
        value_store=dataset_value_store,
    )
    return dataset_value_store


def execute_final_nodes(
    plan: ExecutionPlan,
    *,
    value_store: dict[tuple[str, str], Any],
    ctx: dict[str, Any],
    registry_cfg: dict[str, Any] | None = None,
    skip_roles: set[str] | None = None,
    hook_manager: HookManager | None = None,
) -> None:
    skip_roles = set(skip_roles or set())
    hook_manager = hook_manager or HookManager.from_plan(plan)

    for node in plan.nodes:
        if node.role in skip_roles or node.role != "sink":
            continue

        when = _sink_when(node)
        if when in {"partition_end", "dataset_end"}:
            continue
        if when != "run_end":
            raise ValueError(
                f"Unsupported sink execution timing for node {node.id!r}: {when!r}"
            )

        inputs = _collect_inputs(node.inputs, value_store)
        target = _sink_target(inputs)
        try:
            with hook_manager.around_node(node=node, inputs=inputs, ctx=ctx):
                hook_manager.before_node(node=node, inputs=inputs, ctx=ctx)
                result = run_sink(
                    sink_name=node.impl,
                    target=target,
                    params=node.params,
                    ctx=ctx,
                    meta=_node_meta(node),
                    registry_cfg=registry_cfg,
                )
                _store_node_outputs(node.id, node.outputs, result, value_store)
                hook_manager.after_node(
                    node=node,
                    inputs=inputs,
                    outputs=result,
                    ctx=ctx,
                )
        except Exception as exc:
            _dispatch_node_error(
                hook_manager=hook_manager,
                node=node,
                inputs=inputs,
                ctx=ctx,
                exc=exc,
            )
            raise


_merge_partition_value_stores = merge_partition_value_stores
_execute_final_sinks = execute_final_nodes


def _resolve_source_params(
    params: dict[str, Any],
    *,
    plan: ExecutionPlan | None,
    plan_ctx: dict[str, Any] | None,
) -> dict[str, Any]:
    resolved = dict(params)
    ref = resolved.pop("datasets_ref", None)
    if ref is None:
        return resolved

    if plan is not None:
        resolved["datasets"] = resolve_plan_ref(str(ref), plan)
        return resolved

    if isinstance(plan_ctx, dict) and str(ref) == "context.datasets":
        datasets = plan_ctx.get("datasets")
        if isinstance(datasets, dict):
            resolved["datasets"] = list(datasets.values())
            return resolved

    raise KeyError(f"Could not resolve source datasets_ref {ref!r}")


def _collect_inputs(
    input_refs,
    value_store: dict[tuple[str, str], Any],
) -> dict[str, Any]:
    inputs: dict[str, Any] = {}
    for ref in input_refs:
        key = (ref.node_id, ref.output_name)
        if key not in value_store:
            raise KeyError(
                f"Missing planned input value: {ref.node_id}.{ref.output_name}"
            )
        if ref.input_name in inputs:
            raise ValueError(f"Duplicate bound input name: {ref.input_name!r}")
        inputs[ref.input_name] = value_store[key]
    return inputs


def _sink_when(node: ExecutionNode) -> str:
    default = "run_end" if str(node.impl).startswith("hep.render.") else "partition_end"
    return normalize_lifecycle_event(dict(node.params or {}).get("when") or default)


def _node_meta(node: ExecutionNode) -> dict[str, Any]:
    return {**dict(node.meta or {}), "node_id": node.id}


def _collect_graph_inputs(
    graph: nx.DiGraph,
    node_id: str,
    value_store: dict[tuple[str, str], Any],
) -> dict[str, Any]:
    inputs: dict[str, Any] = {}
    for upstream_node_id, _, edge_data in graph.in_edges(node_id, data=True):
        input_name = str(edge_data.get("input_name") or "target")
        output_name = str(edge_data.get("output") or "stream")
        key = (upstream_node_id, output_name)
        if key not in value_store:
            raise KeyError(
                "Missing upstream runtime value for sink input: "
                f"{upstream_node_id}.{output_name} -> {node_id}.{input_name}"
            )
        if input_name in inputs:
            raise ValueError(f"Duplicate bound sink input name: {input_name!r}")
        inputs[input_name] = value_store[key]
    return inputs


def _sink_target(inputs: dict[str, Any]) -> Any:
    if len(inputs) > 1:
        return inputs
    return _default_target(inputs)


def _default_target(inputs: dict[str, Any]) -> Any:
    if "target" in inputs:
        return inputs["target"]
    if "stream" in inputs:
        return inputs["stream"]
    if len(inputs) == 1:
        return next(iter(inputs.values()))
    raise ValueError(
        f"Could not infer default target from inputs: {sorted(inputs.keys())}"
    )


def _store_node_outputs(
    node_id: str,
    outputs: dict[str, str],
    result: Any,
    value_store: dict[tuple[str, str], Any],
) -> None:
    output_names = list(outputs.keys())
    if isinstance(result, dict) and set(result.keys()) == set(output_names):
        for output_name in output_names:
            value_store[(node_id, output_name)] = result[output_name]
        return

    if len(output_names) == 1:
        value_store[(node_id, output_names[0])] = result
        return

    raise ValueError(
        f"Node {node_id!r} returned a single value for multiple outputs {output_names}; "
        "return a mapping keyed by output port name instead"
    )


def _single_output_return_value(result: Any, output_name: str) -> Any:
    if isinstance(result, dict):
        if output_name in result and len(result) == 1:
            return result[output_name]
        if len(result) == 1:
            return next(iter(result.values()))
    return result
