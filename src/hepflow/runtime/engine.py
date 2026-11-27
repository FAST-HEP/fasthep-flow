from __future__ import annotations

import uuid
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from hepflow.build_layout import BuildPaths, artifact_root_from_context
from hepflow.model.io import ArtifactReference, OutputResult
from hepflow.model.lifecycle import normalize_lifecycle_event
from hepflow.model.persistence import PersistenceSettings
from hepflow.model.plan import (
    ExecutionNode,
    ExecutionPartition,
    ExecutionPlan,
    PlanInputRef,
    resolve_plan_ref,
)
from hepflow.model.plan_applicability import (
    active_plan_nodes_for_context,
    inactive_inputs_behavior_for_node,
    node_applies_to_plan_dataset,
    resolve_active_input_ref,
)
from hepflow.model.products import OperationResult
from hepflow.progress import ProgressReporter
from hepflow.registry.defaults import default_expr_registry
from hepflow.registry.loaders import (
    expr_registry_from_config,
    runtime_registry_from_config,
)
from hepflow.registry.runtime import RuntimeRegistry
from hepflow.runtime.artifacts import (
    deliver_artifacts,
    deliver_location,
    localise_artifacts,
    normalise_location,
    stat_artifacts,
    wrap_artifact_results,
)
from hepflow.runtime.boundary import (
    PartitionBoundaryResult,
    PartitionExecutionSummary,
    ProductAccumulator,
    extract_boundary_products,
    plan_partition_boundary,
    reduce_product_values,
)
from hepflow.runtime.handlers import (
    run_observer,
    run_sink_located,
    run_source,
    run_transform,
)
from hepflow.runtime.hooks.manager import HookDispatchError, HookManager
from hepflow.runtime.materialize import materialize_final_products
from hepflow.runtime.persistence.evidence import EvidenceLedger
from hepflow.runtime.persistence.reuse import reuse_partition_products
from hepflow.runtime.persistence.save import (
    persist_boundary_products,
    provisional_plan_hash,
)
from hepflow.runtime.persistence.select import (
    global_side_product_nodes,
    resolve_persistence,
)
from hepflow.runtime.products import product_bindings, register_product_bindings
from hepflow.runtime.provenance import (
    ProvenanceRecorder,
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
        if _node_outputs_already_available(node, value_store):
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
                    _store_node_outputs(
                        node.id,
                        node.outputs,
                        result,
                        value_store,
                        ctx=ctx,
                    )
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

            inputs = _collect_inputs(node, value_store, plan=plan, ctx=ctx)
            input_products = _collect_input_products(node, plan=plan, ctx=ctx)

            if node.role == "transform":
                with (
                    _input_products_context(ctx, input_products),
                    hook_manager.around_node(node=node, inputs=inputs, ctx=ctx),
                ):
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
                    _store_node_outputs(
                        node.id,
                        node.outputs,
                        result,
                        value_store,
                        ctx=ctx,
                    )
                continue

            if node.role == "observer":
                with (
                    _input_products_context(ctx, input_products),
                    hook_manager.around_node(node=node, inputs=inputs, ctx=ctx),
                ):
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
                    _store_node_outputs(
                        node.id,
                        node.outputs,
                        result,
                        value_store,
                        ctx=ctx,
                    )
                    hook_manager.after_node(
                        node=node,
                        inputs=inputs,
                        outputs=result,
                        ctx=ctx,
                    )
                continue

            if node.role == "sink":
                with (
                    _input_products_context(ctx, input_products),
                    hook_manager.around_node(node=node, inputs=inputs, ctx=ctx),
                ):
                    hook_manager.before_node(node=node, inputs=inputs, ctx=ctx)
                    target = _sink_target(inputs)
                    with _operation_context(recorder, node=node, ctx=ctx):
                        result, artifact_root = run_sink_located(
                            sink_name=node.impl,
                            target=target,
                            params=node.params,
                            ctx=ctx,
                            meta=_node_meta(node),
                            registry_cfg=registry_cfg,
                        )
                    _store_node_outputs(
                        node.id,
                        node.outputs,
                        result,
                        value_store,
                        ctx=ctx,
                        artifact_root=artifact_root,
                        sink=node.impl,
                    )
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


@contextmanager
def _operation_context(
    recorder: ProvenanceRecorder,
    *,
    node: ExecutionNode,
    ctx: dict[str, Any],
) -> Iterator[None]:
    partition = ctx.get("partition")
    if not isinstance(partition, dict):
        partition = None
    had_node_id = "node_id" in ctx
    previous_node_id = ctx.get("node_id")
    ctx["node_id"] = node.id
    try:
        with recorder.operation_context(
            node_id=node.id,
            impl=node.impl,
            role=node.role,
            dataset=str(ctx["dataset_name"]) if ctx.get("dataset_name") else None,
            partition=partition,
        ):
            yield
    finally:
        if had_node_id:
            ctx["node_id"] = previous_node_id
        else:
            ctx.pop("node_id", None)


def execute_plan_locally(
    plan: ExecutionPlan,
    *,
    registry_cfg: dict[str, Any] | None = None,
    ctx: dict[str, Any] | None = None,
    initial_values: dict[tuple[str, str], Any] | None = None,
    skip_roles: set[str] | None = None,
    partitions: list[ExecutionPartition] | None = None,
    progress: ProgressReporter | None = None,
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
    if not base_ctx.get("per_dataset"):
        # Per-partition jobs share one out-dir: none may delete the manifests of the others.
        _reset_final_product_manifests(str(base_ctx.get("outdir") or "."))
    if progress is not None:
        progress.run_started()
        progress.phase_started("executing")

    if partitions is None:
        try:
            value_store = execute_plan_partition(
                plan,
                ctx=base_ctx,
                registry_cfg=registry_cfg,
                initial_values=initial_values,
                skip_roles=skip_roles,
                hook_manager=hook_manager,
            )
            if progress is not None:
                progress.phase_completed("executing")
                progress.phase_started("finalizing")
            product_items = materialize_final_products(
                plan,
                value_store=value_store,
                outdir=str(base_ctx.get("outdir") or "."),
                registry_cfg=registry_cfg,
            )
            register_product_bindings(base_ctx, product_items, plan=plan)
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
                paths=_record_paths(base_ctx),
            )
            hook_manager.run_end(plan=plan, ctx=base_ctx, summary={})
            if progress is not None:
                progress.phase_completed("finalizing")
                progress.run_completed()
            if isinstance(ctx, dict):
                ctx["_hook_summary"] = base_ctx.get("_hook_summary")
            return value_store
        except Exception as exc:
            if progress is not None:
                progress.run_failed(exc)
            raise

    runtime_registry = runtime_registry_from_config(registry_cfg)
    boundary_plan = plan_partition_boundary(plan, runtime_registry=runtime_registry)
    persistence_settings = PersistenceSettings.from_execution(plan.execution)
    persistence_selection = resolve_persistence(
        boundary_plan,
        runtime_registry,
        persistence_settings,
        global_nodes=global_side_product_nodes(plan),
    )
    if persistence_settings.mode != "off":
        base_ctx["_persistence"] = persistence_selection.to_dict()
    attempt_id = str(base_ctx.setdefault("_attempt_id", uuid.uuid4().hex))
    # before any partition or global side product runs: an operation may change the plan
    plan_hash = provisional_plan_hash(plan)
    evidence_ledger = EvidenceLedger()
    global_side_values = execute_global_side_product_nodes(
        plan,
        ctx=base_ctx,
        registry_cfg=registry_cfg,
        initial_values=initial_values,
        hook_manager=hook_manager,
    )
    partition_initial_values = {
        **dict(initial_values or {}),
        **global_side_values,
    }
    dataset_accumulators: dict[str, ProductAccumulator] = {}
    dataset_order: list[str] = []
    partition_summaries: list[PartitionExecutionSummary] = []
    try:
        for partition in partitions:
            partition_ctx = build_partition_context(
                plan,
                base_ctx=base_ctx,
                partition=partition,
            )
            if progress is not None:
                progress.running(partition.id)
            try:
                boundary_products = reuse_partition_products(
                    plan,
                    partition,
                    boundary=boundary_plan,
                    selection=persistence_selection,
                    settings=persistence_settings,
                    registry=runtime_registry,
                    outdir=str(base_ctx.get("outdir") or "."),
                    ledger=evidence_ledger,
                    store=recorder.store,
                    plan_hash=plan_hash,
                )
                if boundary_products is None:
                    value_store = execute_plan_partition(
                        plan,
                        ctx=partition_ctx,
                        registry_cfg=registry_cfg,
                        initial_values=partition_initial_values,
                        skip_roles=skip_roles,
                        hook_manager=hook_manager,
                    )
                    boundary_products = extract_boundary_products(
                        plan,
                        value_store,
                        partition=partition,
                        boundary=boundary_plan,
                        runtime_registry=runtime_registry,
                        ctx=partition_ctx,
                    )
                    boundary_products = persist_boundary_products(
                        boundary_products,
                        persistence_selection,
                        plan=plan,
                        settings=persistence_settings,
                        outdir=str(base_ctx.get("outdir") or "."),
                        store=recorder.store,
                        attempt_id=attempt_id,
                        plan_hash=plan_hash,
                    )
                    del value_store
                boundary_result = PartitionBoundaryResult(
                    partition=partition,
                    products=boundary_products,
                )
                accumulator = dataset_accumulators.get(partition.dataset)
                if accumulator is None:
                    accumulator = ProductAccumulator(
                        plan,
                        runtime_registry=runtime_registry,
                        dataset_name=partition.dataset,
                    )
                    dataset_accumulators[partition.dataset] = accumulator
                    dataset_order.append(partition.dataset)
                accumulator.add_result(boundary_result)
                partition_summaries.append(
                    PartitionExecutionSummary.from_boundary_result(boundary_result)
                )
                del boundary_result
                del boundary_products
            except Exception:
                if progress is not None:
                    progress.failed(partition.id)
                raise
            if progress is not None:
                progress.completed(partition.id)
        if progress is not None:
            progress.phase_completed("executing")
            progress.phase_started("finalizing")

        dataset_stores: list[dict[tuple[str, str], Any]] = []
        for dataset_name in dataset_order:
            accumulator = dataset_accumulators[dataset_name]
            dataset_value_store = accumulator.finalize()
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

        global_accumulator = ProductAccumulator(
            plan,
            runtime_registry=runtime_registry,
            dataset_name=None,
        )
        for dataset_store in dataset_stores:
            global_accumulator.add_value_store(dataset_store)
        merged_value_store = global_accumulator.finalize()
        product_items = materialize_final_products(
            plan,
            value_store=merged_value_store,
            outdir=str(base_ctx.get("outdir") or "."),
            registry_cfg=registry_cfg,
        )
        register_product_bindings(base_ctx, product_items, plan=plan)
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
            stores=[merged_value_store],
            outdir=str(base_ctx.get("outdir") or "."),
            runtime_provenance=recorder,
            paths=_record_paths(base_ctx),
        )
        hook_manager.run_end(plan=plan, ctx=base_ctx, summary={})
        if progress is not None:
            progress.phase_completed("finalizing")
            progress.run_completed()
        if isinstance(ctx, dict):
            if evidence_ledger.consumptions:
                base_ctx["_persistence"]["consumed"] = list(
                    evidence_ledger.consumptions
                )
            ctx["_hook_summary"] = base_ctx.get("_hook_summary")
            ctx["_persistence"] = base_ctx.get("_persistence")
        return partition_summaries
    except Exception as exc:
        if progress is not None:
            progress.run_failed(exc)
        raise


def _record_paths(ctx: Mapping[str, Any]) -> BuildPaths | None:
    """The paths of a per-partition run (records below its run directory), else None."""
    paths = ctx.get("build_paths")
    return (
        paths if isinstance(paths, BuildPaths) and paths.run_dir is not None else None
    )


def _reset_final_product_manifests(outdir: str) -> None:
    """Remove append-style product manifests before publishing current run products."""
    artifacts_dir = Path(outdir) / "artifacts"
    for family in ("histograms", "cutflows"):
        manifest = artifacts_dir / family / "manifest.json"
        if manifest.is_file():
            manifest.unlink()


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
        merged[key] = reduce_product_values(
            plan,
            runtime_registry,
            key=key,
            values=values,
            dataset_name=None,
        )

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
        merged[key] = reduce_product_values(
            plan,
            runtime_registry,
            key=key,
            values=values,
            dataset_name=dataset_name,
        )

    return merged


def execute_global_side_product_nodes(
    plan: ExecutionPlan,
    *,
    ctx: dict[str, Any],
    registry_cfg: dict[str, Any] | None = None,
    initial_values: dict[tuple[str, str], Any] | None = None,
    hook_manager: HookManager | None = None,
) -> dict[tuple[str, str], Any]:
    registry_cfg = registry_cfg or plan.registry
    value_store: dict[tuple[str, str], Any] = dict(initial_values or {})
    hook_manager = hook_manager or HookManager.from_plan(plan)
    recorder = ensure_runtime_provenance(ctx)

    for node in active_plan_nodes_for_context(plan, ctx=ctx):
        if not _is_global_side_product_node(node):
            continue
        if _node_outputs_already_available(node, value_store):
            continue
        inputs = _collect_inputs(node, value_store, plan=plan, ctx=ctx)
        input_products = _collect_input_products(node, plan=plan, ctx=ctx)
        with (
            _input_products_context(ctx, input_products),
            hook_manager.around_node(node=node, inputs=inputs, ctx=ctx),
        ):
            hook_manager.before_node(node=node, inputs=inputs, ctx=ctx)
            with _operation_context(recorder, node=node, ctx=ctx):
                result = run_transform(
                    transform_name=node.impl,
                    inputs=inputs,
                    params=node.params,
                    registry_cfg=registry_cfg,
                    ctx=ctx,
                )
            _store_node_outputs(
                node.id,
                node.outputs,
                result,
                value_store,
                ctx=ctx,
            )
            hook_manager.after_node(
                node=node,
                inputs=inputs,
                outputs=result,
                ctx=ctx,
            )

    return {
        key: value
        for key, value in value_store.items()
        if key not in dict(initial_values or {})
    }


def _is_global_side_product_node(node: ExecutionNode) -> bool:
    return (
        node.role == "transform"
        and node.input_scope == "global"
        and node.output_scope == "global"
        and any(kind != "event_stream" for kind in node.outputs.values())
    )


def _node_outputs_already_available(
    node: ExecutionNode,
    value_store: dict[tuple[str, str], Any],
) -> bool:
    return node.output_scope == "global" and all(
        (node.id, output_name) in value_store for output_name in node.outputs
    )


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
    registry_cfg = registry_cfg or plan.registry
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
            node,
            dataset_value_store,
            plan=plan,
            ctx=ctx,
        )
        input_products = _collect_input_products(node, plan=plan, ctx=ctx)
        target = _sink_target(inputs)
        try:
            with (
                _input_products_context(ctx, input_products),
                hook_manager.around_node(node=node, inputs=inputs, ctx=ctx),
            ):
                hook_manager.before_node(node=node, inputs=inputs, ctx=ctx)
                with _operation_context(recorder, node=node, ctx=ctx):
                    result, artifact_root = run_sink_located(
                        sink_name=node.impl,
                        target=target,
                        params=node.params,
                        ctx={
                            **ctx,
                            "datasets": dict(plan.context.get("datasets") or {}),
                        },
                        meta=_node_meta(node),
                        registry_cfg=registry_cfg,
                    )
                _store_node_outputs(
                    node.id,
                    node.outputs,
                    result,
                    dataset_value_store,
                    ctx=ctx,
                    artifact_root=artifact_root,
                    sink=node.impl,
                )
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
    registry_cfg = registry_cfg or plan.registry
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

        inputs = _collect_inputs(node, value_store, plan=plan, ctx=ctx)
        input_products = _collect_input_products(node, plan=plan, ctx=ctx)
        target = _sink_target(inputs)
        try:
            with (
                _input_products_context(ctx, input_products),
                hook_manager.around_node(node=node, inputs=inputs, ctx=ctx),
            ):
                hook_manager.before_node(node=node, inputs=inputs, ctx=ctx)
                with _operation_context(recorder, node=node, ctx=ctx):
                    result, artifact_root = run_sink_located(
                        sink_name=node.impl,
                        target=target,
                        params=node.params,
                        ctx=ctx,
                        meta=_node_meta(node),
                        registry_cfg=registry_cfg,
                    )
                _store_node_outputs(
                    node.id,
                    node.outputs,
                    result,
                    value_store,
                    ctx=ctx,
                    artifact_root=artifact_root,
                    sink=node.impl,
                )
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
    node: ExecutionNode,
    value_store: dict[tuple[str, str], Any],
    *,
    plan: ExecutionPlan | None = None,
    ctx: dict[str, Any] | None = None,
) -> dict[str, Any]:
    inputs: dict[str, Any] = {}
    input_refs = node.inputs
    omit_inactive_inputs = (
        plan is not None and inactive_inputs_behavior_for_node(plan, node) == "omit"
    )
    event_stream_input_count = (
        _event_stream_input_count(plan, node) if plan is not None else 0
    )
    for ref in input_refs:
        if ref.input_name == "dependency":
            continue
        active_ref = ref
        if plan is not None and ctx is not None:
            dataset = ctx.get("dataset")
            upstream = plan.node_index.get(ref.node_id)
            if upstream is not None and not node_applies_to_plan_dataset(
                upstream,
                dataset=dataset if isinstance(dataset, dict) else None,
            ):
                if omit_inactive_inputs:
                    continue
                if event_stream_input_count > 1:
                    raise KeyError(
                        f"Node {node.id!r} has inactive required input "
                        f"{ref.node_id!r}; declare input.inactive_inputs: omit "
                        "to allow contextual omission"
                    )
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


def _collect_input_products(
    node: ExecutionNode,
    *,
    plan: ExecutionPlan | None,
    ctx: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    bindings = product_bindings(ctx)
    products: dict[str, dict[str, Any]] = {}
    input_refs = node.inputs
    omit_inactive_inputs = (
        plan is not None and inactive_inputs_behavior_for_node(plan, node) == "omit"
    )
    event_stream_input_count = (
        _event_stream_input_count(plan, node) if plan is not None else 0
    )
    for ref in input_refs:
        if ref.input_name == "dependency":
            continue
        active_ref = ref
        if plan is not None:
            dataset = ctx.get("dataset")
            dataset_dict = dataset if isinstance(dataset, dict) else None
            upstream = plan.node_index.get(ref.node_id)
            if upstream is not None and not node_applies_to_plan_dataset(
                upstream,
                dataset=dataset_dict,
            ):
                if omit_inactive_inputs:
                    continue
                if event_stream_input_count > 1:
                    raise KeyError(
                        f"Node {node.id!r} has inactive required input "
                        f"{ref.node_id!r}; declare input.inactive_inputs: omit "
                        "to allow contextual omission"
                    )
            try:
                active_ref = resolve_active_input_ref(
                    plan,
                    ref,
                    dataset=dataset_dict,
                )
            except (KeyError, ValueError):
                # The input may have been supplied directly through initial_values.
                # In that case, no producer/product metadata is available.
                continue
        binding = bindings.get((active_ref.node_id, active_ref.output_name))
        if binding is not None:
            products[ref.input_name] = binding.to_input_product()
    return products


def _event_stream_input_count(plan: ExecutionPlan, node: ExecutionNode) -> int:
    count = 0
    for ref in node.inputs:
        if ref.input_name == "dependency":
            continue
        upstream = plan.node_index.get(ref.node_id)
        if upstream is None:
            continue
        if upstream.outputs.get(ref.output_name) == "event_stream":
            count += 1
    return count


@contextmanager
def _input_products_context(
    ctx: dict[str, Any],
    input_products: dict[str, dict[str, Any]],
) -> Iterator[None]:
    sentinel = object()
    previous = ctx.get("input_products", sentinel)
    ctx["input_products"] = input_products
    try:
        yield
    finally:
        if previous is sentinel:
            ctx.pop("input_products", None)
        else:
            ctx["input_products"] = previous


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
    *,
    ctx: dict[str, Any] | None = None,
    artifact_root: str | Path | None = None,
    sink: str | None = None,
) -> None:
    if isinstance(result, OperationResult):
        for output_name, product in result.products.items():
            if output_name in outputs:
                _localise_artifact_product(
                    product, outputs[output_name], ctx, node_id=node_id, sink=sink
                )
                value_store[(node_id, output_name)] = product
        return

    output_names = list(outputs.keys())
    if isinstance(result, dict) and set(result.keys()) == set(output_names):
        for output_name in output_names:
            value_store[(node_id, output_name)] = _normalize_single_output(
                result[output_name],
                node_id=node_id,
                output_name=output_name,
                product_kind=outputs[output_name],
                ctx=ctx,
                artifact_root=artifact_root,
                sink=sink,
            )
        return

    if len(output_names) == 1:
        output_name = output_names[0]
        value_store[(node_id, output_name)] = _normalize_single_output(
            result,
            node_id=node_id,
            output_name=output_name,
            product_kind=outputs[output_name],
            ctx=ctx,
            artifact_root=artifact_root,
            sink=sink,
        )
        return

    raise ValueError(
        f"Node {node_id!r} returned a single value for multiple outputs {output_names}; "
        "return a mapping keyed by output port name instead"
    )


def _normalize_single_output(
    result: Any,
    *,
    node_id: str,
    output_name: str,
    product_kind: str,
    ctx: dict[str, Any] | None,
    artifact_root: str | Path | None = None,
    sink: str | None = None,
) -> Any:
    if product_kind == "artifact" and isinstance(result, list | tuple):
        return _normalize_artifact_list(
            list(result),
            node_id=node_id,
            output_name=output_name,
            ctx=ctx,
            artifact_root=artifact_root,
            sink=sink,
        )
    normalised = _normalize_artifact_dict(
        result,
        node_id=node_id,
        output_name=output_name,
        product_kind=product_kind,
        ctx=ctx,
    )
    _localise_artifact_product(
        normalised,
        product_kind,
        ctx,
        node_id=node_id,
        sink=sink,
        artifact_root=artifact_root,
    )
    return normalised


def _normalize_artifact_list(
    items: list[Any],
    *,
    node_id: str,
    output_name: str,
    ctx: dict[str, Any] | None,
    artifact_root: str | Path | None,
    sink: str | None,
) -> Any:
    """One sink output that reported several files: tag, localise, check, then wrap."""
    converted = [
        _normalize_artifact_dict(
            item,
            node_id=node_id,
            output_name=output_name,
            product_kind="artifact",
            ctx=ctx,
        )
        for item in items
    ]
    references = [item for item in converted if isinstance(item, ArtifactReference)]
    if len(references) != len(converted):
        # Not a list of files (for example manifests of several datasets): keep it.
        _localise_artifact_product(
            converted,
            "artifact",
            ctx,
            node_id=node_id,
            sink=sink,
            artifact_root=artifact_root,
        )
        return converted
    for reference in references:
        reference.producer_node = reference.producer_node or node_id
        reference.output_name = reference.output_name or output_name
        if reference.dataset_name is None:
            reference.dataset_name = _ctx_dataset_name(ctx)
        if reference.partition_id is None:
            reference.partition_id = _ctx_partition_id(ctx)
        if reference.partition_index is None:
            reference.partition_index = _ctx_partition_index(ctx)
    _localise_artifact_product(
        references,
        "artifact",
        ctx,
        node_id=node_id,
        sink=sink,
        artifact_root=artifact_root,
    )
    root = _location_of_root(artifact_root, ctx)
    return wrap_artifact_results(
        references,
        node_id=node_id,
        output_name=output_name,
        dataset_name=_ctx_dataset_name(ctx),
        root=root,
    )


def _location_of_root(
    artifact_root: str | Path | None, ctx: dict[str, Any] | None
) -> str | None:
    outdir = (ctx or {}).get("outdir")
    if artifact_root is None or not outdir:
        return None
    location = normalise_location(str(artifact_root), str(outdir))
    base, destination = _delivery(ctx)
    if base is not None and destination is not None:
        return deliver_location(location, base=base, destination=destination)
    return location


def _delivery(ctx: dict[str, Any] | None) -> tuple[Path | None, str | None]:
    """A backend's run-level artifact base and the destination it delivers to, if any."""
    base = artifact_root_from_context(ctx)
    destination = (ctx or {}).get("output_destination")
    return base, (str(destination) if destination else None)


def _localise_artifact_product(
    value: Any,
    product_kind: str,
    ctx: dict[str, Any] | None,
    *,
    node_id: str = "",
    sink: str | None = None,
    artifact_root: str | Path | None = None,
) -> None:
    """Store artifact locations as Flow records them, in the producing process."""
    outdir = (ctx or {}).get("outdir")
    if product_kind != "artifact" or not outdir:
        return
    base, destination = _delivery(ctx)
    localise_artifacts(value, str(outdir), root=artifact_root)
    stat_artifacts(
        value,
        outdir=str(outdir),
        node_id=node_id,
        sink=sink or node_id,
        with_adler32=destination is not None,
    )
    if base is not None and destination is not None:
        deliver_artifacts(value, base=base, destination=destination)


def _normalize_artifact_dict(
    result: Any,
    *,
    node_id: str,
    output_name: str,
    product_kind: str,
    ctx: dict[str, Any] | None,
) -> Any:
    if (
        product_kind == "artifact"
        and isinstance(result, dict)
        and isinstance(result.get("path"), str)
    ):
        metadata = {
            key: value for key, value in result.items() if key != "path"
        }
        return OutputResult(
            kind="artifact",
            path=str(result["path"]),
            metadata=metadata,
            producer_node=node_id,
            output_name=output_name,
            dataset_name=_ctx_dataset_name(ctx),
            partition_id=_ctx_partition_id(ctx),
            partition_index=_ctx_partition_index(ctx),
        )
    return result


def _ctx_dataset_name(ctx: dict[str, Any] | None) -> str | None:
    if ctx is None:
        return None
    partition = dict(ctx.get("partition") or {})
    dataset_name = ctx.get("dataset_name") or partition.get("dataset")
    return str(dataset_name) if dataset_name is not None else None


def _ctx_partition_id(ctx: dict[str, Any] | None) -> str | None:
    if ctx is None:
        return None
    partition = dict(ctx.get("partition") or {})
    partition_id = partition.get("id")
    return str(partition_id) if partition_id is not None else None


def _ctx_partition_index(ctx: dict[str, Any] | None) -> int | None:
    if ctx is None:
        return None
    partition = dict(ctx.get("partition") or {})
    part = str(partition.get("part") or "")
    if not part:
        return None
    try:
        return int(part.rsplit("_", 1)[-1])
    except ValueError:
        return None
