from __future__ import annotations

from typing import Any

from hepflow.model.lifecycle import normalize_lifecycle_event
from hepflow.model.plan import (
    ExecutionNode,
    ExecutionPartition,
    ExecutionPlan,
    PlanInputRef,
    resolve_plan_ref,
)
from hepflow.model.plan_applicability import (
    active_plan_nodes_for_context,
    resolve_active_input_ref,
)
from hepflow.model.products import OperationResult
from hepflow.registry.defaults import default_expr_registry
from hepflow.registry.loaders import (
    expr_registry_from_config,
    runtime_registry_from_config,
)
from hepflow.registry.runtime import RuntimeRegistry
from hepflow.runtime.handlers import run_observer, run_sink, run_source, run_transform
from hepflow.runtime.hooks.manager import HookDispatchError, HookManager
from hepflow.runtime.materialize import materialize_final_products
from hepflow.runtime.operation_provenance import (
    RuntimeProvenanceRecorder,
    ensure_runtime_provenance,
)
from hepflow.runtime.writer_manifests import write_writer_manifests


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
        symbols = sorted(str(k) for k in scope)
        shown = symbols[:50]
        suffix = " ..." if len(symbols) > 50 else ""
        raise NameError(
            f"{exc}. While evaluating expression {expr!r}. "
            f"Available symbols include: {shown}{suffix}"
        ) from exc


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
    recorder = ensure_runtime_provenance(ctx)
    registry_cfg = registry_cfg or plan.registry
    _ensure_expr_registry(ctx, registry_cfg)
    value_store: dict[tuple[str, str], Any] = dict(initial_values or {})
    skip_roles = set(skip_roles or set())
    hook_manager = hook_manager or HookManager.from_plan(plan)
    partition = ctx.get("partition")
    hook_manager.partition_start(partition=partition, ctx=ctx)

    for node in active_plan_nodes_for_context(plan, ctx=ctx):
        if node.role in skip_roles:
            continue
        inputs: dict[str, Any] = {}
        try:
            if node.role == "source":
                with hook_manager.around_node(node=node, inputs=inputs, ctx=ctx):
                    hook_manager.before_node(node=node, inputs=inputs, ctx=ctx)
                    params = _resolve_source_params(
                        node.params, plan=plan, plan_ctx=ctx
                    )
                    if _source_should_read_metadata_only(plan, node, ctx=ctx):
                        params["metadata_only"] = True
                    with _operation_context(recorder, node=node, ctx=ctx):
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

            if node.role == "sink":
                when = _sink_when(node)
                if when in {"dataset_end", "run_end"}:
                    continue
                if when != "partition_end":
                    raise ValueError(
                        f"Unsupported sink execution timing for node {node.id!r}: {when!r}"
                    )

            inputs = _collect_inputs(node.inputs, value_store, plan=plan, ctx=ctx)

            if node.role == "transform":
                with hook_manager.around_node(node=node, inputs=inputs, ctx=ctx):
                    hook_manager.before_node(node=node, inputs=inputs, ctx=ctx)
                    with _operation_context(recorder, node=node, ctx=ctx):
                        result = run_transform(
                            transform_name=node.impl,
                            inputs=inputs,
                            params=node.params,
                            registry_cfg=registry_cfg,
                            ctx=ctx,
                        )
                    hook_manager.after_node(
                        node=node,
                        inputs=inputs,
                        outputs=result,
                        ctx=ctx,
                    )
                    _store_node_outputs(node.id, node.outputs, result, value_store)
                continue

            if node.role == "observer":
                with hook_manager.around_node(node=node, inputs=inputs, ctx=ctx):
                    hook_manager.before_node(node=node, inputs=inputs, ctx=ctx)
                    target = _default_target(inputs)
                    with _operation_context(recorder, node=node, ctx=ctx):
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
                with hook_manager.around_node(node=node, inputs=inputs, ctx=ctx):
                    hook_manager.before_node(node=node, inputs=inputs, ctx=ctx)
                    target = _sink_target(inputs)
                    with _operation_context(recorder, node=node, ctx=ctx):
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
        print(f"Runtime error in node {node.id}: {type(exc).__name__}: {exc}")  # noqa: T201
        return
    try:
        hook_manager.on_node_error(node=node, inputs=inputs, ctx=ctx, exc=exc)
    except HookDispatchError as hook_exc:
        print(f"Error hook {hook_exc.kind} failed: {hook_exc.cause}")  # noqa: T201


def _operation_context(
    recorder: RuntimeProvenanceRecorder,
    *,
    node: ExecutionNode,
    ctx: dict[str, Any],
) -> Any:
    partition = ctx.get("partition")
    if not isinstance(partition, dict):
        partition = None
    return recorder.operation_context(
        node_id=node.id,
        impl=node.impl,
        role=node.role,
        dataset=str(ctx["dataset_name"]) if ctx.get("dataset_name") else None,
        partition=partition,
    )


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
    recorder = ensure_runtime_provenance(base_ctx)
    base_ctx.setdefault("runtime_resources", {})
    resolved_resources = base_ctx.setdefault("resolved_resources", {})
    base_ctx.setdefault("resources", resolved_resources)
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
        materialize_final_products(
            plan,
            value_store=value_store,
            outdir=str(base_ctx.get("outdir") or "."),
            registry_cfg=registry_cfg,
        )
        execute_final_nodes(
            plan,
            value_store=value_store,
            ctx=base_ctx,
            registry_cfg=registry_cfg,
            skip_roles=skip_roles,
            hook_manager=hook_manager,
        )
        write_writer_manifests(
            plan,
            stores=[value_store],
            outdir=str(base_ctx.get("outdir") or "."),
            runtime_provenance=recorder,
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
        dataset_value_store = merge_partition_value_stores_for_dataset(
            plan,
            stores,
            dataset_name=dataset_name,
            registry_cfg=registry_cfg,
        )
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

    merged_value_store = merge_partition_value_stores(
        plan,
        dataset_stores,
        registry_cfg=registry_cfg,
    )
    materialize_final_products(
        plan,
        value_store=merged_value_store,
        outdir=str(base_ctx.get("outdir") or "."),
        registry_cfg=registry_cfg,
    )
    execute_final_nodes(
        plan,
        value_store=merged_value_store,
        ctx=base_ctx,
        registry_cfg=registry_cfg,
        skip_roles=skip_roles,
        hook_manager=hook_manager,
    )
    write_writer_manifests(
        plan,
        stores=results,
        partitions=partitions,
        outdir=str(base_ctx.get("outdir") or "."),
        runtime_provenance=recorder,
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
    del plan
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
    del plan
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
    *,
    registry_cfg: dict[str, Any] | None = None,
    runtime_registry: RuntimeRegistry | None = None,
) -> dict[tuple[str, str], Any]:
    runtime_registry = runtime_registry or runtime_registry_from_config(
        registry_cfg or plan.registry
    )
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
            node = None
            output_kind = None

        if node is not None:
            handler = runtime_registry.product_handlers.get(str(output_kind))
            if handler is not None and handler.merge is not None:
                merged[key] = handler.merge(
                    values,
                    node=node,
                    output_name=output_name,
                    dataset_name=None,
                )
                continue

        if output_kind in runtime_registry.product_handlers:
            merged[key] = values[0] if len(values) == 1 else list(values)
            continue

        if output_kind == "report":
            merged[key] = list(values)
            continue

        merged[key] = values[0] if len(values) == 1 else list(values)

    return merged


def group_partition_results_by_dataset(
    partition_results: list[dict[tuple[str, str], Any]],
    partitions: list[ExecutionPartition],
) -> dict[str, list[dict[tuple[str, str], Any]]]:
    grouped: dict[str, list[dict[tuple[str, str], Any]]] = {}
    for result, partition in zip(partition_results, partitions, strict=False):
        grouped.setdefault(partition.dataset, []).append(result)
    return grouped


def merge_partition_value_stores_for_dataset(
    plan: ExecutionPlan,
    stores: list[dict[tuple[str, str], Any]],
    *,
    dataset_name: str,
    registry_cfg: dict[str, Any] | None = None,
    runtime_registry: RuntimeRegistry | None = None,
) -> dict[tuple[str, str], Any]:
    runtime_registry = runtime_registry or runtime_registry_from_config(
        registry_cfg or plan.registry
    )
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
            node = None
            output_kind = None

        if node is not None:
            handler = runtime_registry.product_handlers.get(str(output_kind))
            if handler is not None and handler.merge is not None:
                merged[key] = handler.merge(
                    values,
                    node=node,
                    output_name=output_name,
                    dataset_name=dataset_name,
                )
                continue

        if output_kind in runtime_registry.product_handlers:
            merged[key] = values[0] if len(values) == 1 else list(values)
            continue

        if output_kind == "report":
            merged[key] = list(values)
            continue

        merged[key] = values[0] if len(values) == 1 else list(values)

    return merged


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
    recorder = ensure_runtime_provenance(ctx)

    for node in active_plan_nodes_for_context(plan, ctx=ctx):
        if node.role in skip_roles or node.role != "sink":
            continue

        when = _sink_when(node)
        if when in {"partition_end", "run_end"}:
            continue
        if when != "dataset_end":
            raise ValueError(
                f"Unsupported sink execution timing for node {node.id!r}: {when!r}"
            )

        inputs = _collect_inputs(
            node.inputs,
            dataset_value_store,
            plan=plan,
            ctx=ctx,
        )
        target = _sink_target(inputs)
        try:
            with hook_manager.around_node(node=node, inputs=inputs, ctx=ctx):
                hook_manager.before_node(node=node, inputs=inputs, ctx=ctx)
                with _operation_context(recorder, node=node, ctx=ctx):
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
    recorder = ensure_runtime_provenance(ctx)

    for node in active_plan_nodes_for_context(plan, ctx=ctx):
        if node.role in skip_roles or node.role != "sink":
            continue

        when = _sink_when(node)
        if when in {"partition_end", "dataset_end"}:
            continue
        if when != "run_end":
            raise ValueError(
                f"Unsupported sink execution timing for node {node.id!r}: {when!r}"
            )

        inputs = _collect_inputs(node.inputs, value_store, plan=plan, ctx=ctx)
        target = _sink_target(inputs)
        try:
            with hook_manager.around_node(node=node, inputs=inputs, ctx=ctx):
                hook_manager.before_node(node=node, inputs=inputs, ctx=ctx)
                with _operation_context(recorder, node=node, ctx=ctx):
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
    branches_by_dataset = resolved.pop("branches_by_dataset", None)
    ref = resolved.pop("datasets_ref", None)
    if isinstance(branches_by_dataset, dict):
        dataset_name = None
        if isinstance(plan_ctx, dict):
            dataset_name = plan_ctx.get("dataset_name")
        if dataset_name is not None:
            dataset_branches = branches_by_dataset.get(str(dataset_name))
            if dataset_branches is not None:
                existing = {
                    str(branch) for branch in list(resolved.get("branches") or [])
                }
                resolved["branches"] = sorted(
                    existing | {str(branch) for branch in list(dataset_branches)}
                )
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


def _source_should_read_metadata_only(
    plan: ExecutionPlan,
    node: ExecutionNode,
    *,
    ctx: dict[str, Any] | None = None,
) -> bool:
    if node.role != "source" or node.impl != "root_tree":
        return False

    output_names = set(node.outputs)
    direct_consumers: list[ExecutionNode] = []
    candidates = (
        active_plan_nodes_for_context(plan, ctx=dict(ctx or {}))
        if ctx is not None
        else plan.nodes
    )
    dataset = _ctx_dataset(ctx)
    for candidate in candidates:
        for ref in candidate.inputs:
            try:
                active_ref = (
                    resolve_active_input_ref(plan, ref, dataset=dataset)
                    if ctx is not None
                    else ref
                )
            except ValueError:
                continue
            if active_ref.node_id == node.id and active_ref.output_name in output_names:
                direct_consumers.append(candidate)
                break

    if not direct_consumers:
        return False

    downstream = _downstream_consumers(plan, direct_consumers, ctx=ctx)
    return bool(downstream) and all(_is_schema_snapshot_observer(n) for n in downstream)


def _downstream_consumers(
    plan: ExecutionPlan,
    initial: list[ExecutionNode],
    *,
    ctx: dict[str, Any] | None = None,
) -> list[ExecutionNode]:
    by_id = {node.id: node for node in plan.nodes}
    candidates = (
        active_plan_nodes_for_context(plan, ctx=dict(ctx or {}))
        if ctx is not None
        else plan.nodes
    )
    dataset = _ctx_dataset(ctx)
    seen: set[str] = set()
    out: list[ExecutionNode] = []
    queue = list(initial)
    while queue:
        current = queue.pop(0)
        if current.id in seen:
            continue
        seen.add(current.id)
        out.append(current)

        current_outputs = set(current.outputs)
        for candidate in candidates:
            if candidate.id in seen:
                continue
            if _node_consumes_active_output(
                plan,
                candidate,
                current_node_id=current.id,
                current_outputs=current_outputs,
                dataset=dataset,
                has_context=ctx is not None,
            ):
                queue.append(by_id[candidate.id])
    return out


def _ctx_dataset(ctx: dict[str, Any] | None) -> dict[str, Any] | None:
    dataset = ctx.get("dataset") if isinstance(ctx, dict) else None
    return dataset if isinstance(dataset, dict) else None


def _active_ref_for_metadata_walk(
    plan: ExecutionPlan,
    ref: PlanInputRef,
    *,
    dataset: dict[str, Any] | None,
    has_context: bool,
) -> PlanInputRef:
    if not has_context:
        return ref
    return resolve_active_input_ref(plan, ref, dataset=dataset)


def _node_consumes_active_output(
    plan: ExecutionPlan,
    candidate: ExecutionNode,
    *,
    current_node_id: str,
    current_outputs: set[str],
    dataset: dict[str, Any] | None,
    has_context: bool,
) -> bool:
    for ref in candidate.inputs:
        try:
            active_ref = _active_ref_for_metadata_walk(
                plan,
                ref,
                dataset=dataset,
                has_context=has_context,
            )
        except ValueError:
            continue
        if (
            active_ref.node_id == current_node_id
            and active_ref.output_name in current_outputs
        ):
            return True
    return False


def _is_schema_snapshot_observer(node: ExecutionNode) -> bool:
    return node.role == "observer" and node.impl == "hep.schema_snapshot"


def _collect_inputs(
    input_refs,
    value_store: dict[tuple[str, str], Any],
    *,
    plan: ExecutionPlan | None = None,
    ctx: dict[str, Any] | None = None,
) -> dict[str, Any]:
    inputs: dict[str, Any] = {}
    for ref in input_refs:
        active_ref = ref
        if (
            (ref.node_id, ref.output_name) not in value_store
            and plan is not None
            and ctx is not None
        ):
            dataset = ctx.get("dataset")
            active_ref = resolve_active_input_ref(
                plan,
                ref,
                dataset=dataset if isinstance(dataset, dict) else None,
            )
        key = (active_ref.node_id, active_ref.output_name)
        if key not in value_store:
            raise KeyError(
                f"Missing planned input value: {active_ref.node_id}.{active_ref.output_name}"
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
    if isinstance(result, OperationResult):
        for output_name, product in result.products.items():
            if output_name in outputs:
                value_store[(node_id, output_name)] = product
        return

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
