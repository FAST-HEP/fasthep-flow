from __future__ import annotations

from importlib import import_module
from typing import Any

from hepflow.backends._dask._pools import dask_resources_for_resource
from hepflow.backends.local import _store_outputs_summary
from hepflow.backends.model import BackendResult
from hepflow.build_layout import BuildPaths
from hepflow.model.plan import ExecutionNode, ExecutionPartition, ExecutionPlan
from hepflow.runtime.engine import (
    build_dataset_context,
    build_partition_context,
    execute_dataset_sinks,
    execute_final_nodes,
    execute_plan_partition,
    group_partition_results_by_dataset,
    merge_partition_value_stores,
    merge_partition_value_stores_for_dataset,
)
from hepflow.runtime.hooks.manager import HookManager
from hepflow.runtime.operation_provenance import ensure_runtime_provenance
from hepflow.runtime.writer_manifests import write_writer_manifests


class DaskBackend:
    """
    Partition-granular Dask backend with pluggable worker provisioning strategies.
    """

    name = "dask"

    def run(
        self,
        plan: ExecutionPlan,
        *,
        ctx: dict[str, Any] | None = None,
    ) -> BackendResult:
        strategy = normalise_dask_strategy(plan.execution)
        validate_supported_dask_pools(plan.execution, strategy=strategy)
        dask_config = normalise_dask_config(plan.execution)

        base_ctx = dict(plan.context)
        base_ctx.update(dict(ctx or {}))
        base_ctx.pop("provenance", None)
        base_ctx.setdefault("runtime_resources", {})
        resolved_resources = base_ctx.setdefault("resolved_resources", {})
        base_ctx.setdefault("resources", resolved_resources)
        tasks = build_dask_graph(plan, base_ctx=base_ctx)

        dashboard_link: str | None = None
        strategy_config: dict[str, Any] | None = None
        if strategy == "htcondor":
            compute_with_htcondor = import_module(
                "hepflow.backends._dask._htcondor"
            ).compute_with_htcondor
            task_results, dashboard_link, strategy_config = compute_with_htcondor(
                tasks,
                execution=plan.execution,
                build_paths=BuildPaths.from_ctx(base_ctx),
            )
        elif strategy == "slurm":
            compute_with_slurm = import_module(
                "hepflow.backends._dask._slurm"
            ).compute_with_slurm
            task_results, dashboard_link, strategy_config = compute_with_slurm(
                tasks,
                execution=plan.execution,
                build_paths=BuildPaths.from_ctx(base_ctx),
            )
        else:
            compute_with_local_strategy = import_module(
                "hepflow.backends._dask._local"
            ).compute_with_local_strategy
            task_results, dashboard_link = compute_with_local_strategy(
                tasks,
                dask_config=dask_config,
                build_paths=BuildPaths.from_ctx(base_ctx),
            )

        return build_dask_backend_result(
            plan,
            base_ctx=base_ctx,
            task_results=task_results,
            partition_hook_summaries=[
                item.get("hooks") or {"enabled": []}
                for item in task_results
            ],
            strategy=strategy,
            dask_config=dask_config,
            strategy_config=strategy_config,
            dashboard_link=dashboard_link,
        )


def build_dask_graph(
    plan: ExecutionPlan,
    *,
    base_ctx: dict[str, Any],
) -> list[Any]:
    from dask import delayed  # noqa: PLC0415

    tasks: list[Any] = []
    annotations = _dask_resource_annotations_for_plan(plan)
    for partition in plan.partitions:
        # The backend is currently partition-granular: one Dask task executes the
        # partition plan. These annotations route that task only when matching
        # Dask workers have been provisioned with the same resource names.
        if annotations:
            import dask  # noqa: PLC0415

            with dask.annotate(resources=annotations):
                task = delayed(_execute_partition_task)(
                    plan,
                    partition,
                    base_ctx=base_ctx,
                    registry_cfg=plan.registry,
                )
        else:
            task = delayed(_execute_partition_task)(
                plan,
                partition,
                base_ctx=base_ctx,
                registry_cfg=plan.registry,
            )
        tasks.append(task)
    return tasks


def dask_resource_annotations_for_node(
    plan: ExecutionPlan,
    node: ExecutionNode | str,
) -> dict[str, Any]:
    if isinstance(node, str):
        node = plan.get_node(node)

    node_execution = dict((node.meta or {}).get("execution") or {})
    required_resource_name = node_execution.get("require")
    if required_resource_name is None:
        return {}

    resource_name = str(required_resource_name)
    resources = dict((plan.execution or {}).get("resources") or {})
    resource = resources.get(resource_name)
    if resource is None:
        raise ValueError(
            f"Dask resource annotation for node {node.id!r} references unknown "
            f"resource class {resource_name!r}."
        )
    if not isinstance(resource, dict):
        return {}

    return dask_resources_for_resource(resource_name, resource)


def _dask_resource_annotations_for_plan(plan: ExecutionPlan) -> dict[str, Any]:
    annotations: dict[str, Any] = {}
    for node in plan.nodes:
        node_annotations = dask_resource_annotations_for_node(plan, node)
        for resource_name, quantity in node_annotations.items():
            annotations[resource_name] = _merge_dask_resource_quantity(
                annotations.get(resource_name),
                quantity,
            )
    return annotations


def _merge_dask_resource_quantity(current: Any, new: Any) -> Any:
    if current is None:
        return new
    if isinstance(current, int | float) and isinstance(new, int | float):
        return max(current, new)
    return new


def compute_with_client(client: Any, tasks: list[Any]) -> tuple[list[Any], str | None]:
    dashboard_link = getattr(client, "dashboard_link", None)
    if not tasks:
        return [], dashboard_link
    futures = client.compute(tasks)
    return list(client.gather(futures)), dashboard_link


def compute_with_scheduler(
    tasks: list[Any],
    *,
    scheduler: str,
    n_workers: int | None,
) -> list[Any]:
    from dask import compute  # noqa: PLC0415

    compute_kwargs: dict[str, Any] = {"scheduler": scheduler}
    if n_workers is not None:
        compute_kwargs["num_workers"] = n_workers
    return list(compute(*tasks, **compute_kwargs)) if tasks else []


def normalise_dask_strategy(execution: dict[str, Any]) -> str:
    strategy = str(execution.get("strategy") or "default")
    if strategy in {"default", "local"}:
        return "local"
    if strategy in {"htcondor", "slurm"}:
        return strategy
    raise ValueError(f"Dask strategy {strategy!r} is not implemented yet.")


def normalise_dask_config(execution: dict[str, Any]) -> dict[str, Any]:
    config = dict(execution.get("config") or {})
    scheduler = str(config.get("scheduler") or "threads")
    default_pool = _default_pool(execution)
    n_workers = config.get("n_workers", config.get("workers"))
    if n_workers is None and default_pool is not None:
        n_workers = default_pool.get("workers")
    if n_workers is not None:
        n_workers = int(n_workers)

    use_local_cluster = scheduler == "distributed" or any(
        key in config
        for key in (
            "workers",
            "threads_per_worker",
            "processes",
            "memory_limit",
            "dashboard_address",
            "local_directory",
        )
    )

    return {
        "scheduler": "distributed" if use_local_cluster else scheduler,
        "use_local_cluster": use_local_cluster,
        "n_workers": n_workers,
        "threads_per_worker": int(config.get("threads_per_worker") or 1),
        "processes": _as_bool(config.get("processes", scheduler == "processes")),
        "memory_limit": config.get("memory_limit"),
        "dashboard_address": config.get("dashboard_address"),
        "local_directory": config.get("local_directory"),
        "pools": dict(execution.get("pools") or {}),
    }


def validate_supported_dask_pools(
    execution: dict[str, Any],
    *,
    strategy: str,
) -> None:
    pools = dict(execution.get("pools") or {})
    if not pools:
        return
    if strategy == "local":
        if len(pools) > 1:
            raise NotImplementedError(
                "Dask local strategy does not yet support heterogeneous worker pools. "
                "Use htcondor/slurm or configure a single default pool."
            )
        pool_name, pool = next(iter(pools.items()))
        if pool_name != "default" or dict(pool).get("resources") != "default":
            raise NotImplementedError(
                "Dask local strategy currently supports only the default worker pool."
            )
        return


def build_dask_backend_result(
    plan: ExecutionPlan,
    *,
    base_ctx: dict[str, Any],
    task_results: list[dict[str, Any]],
    partition_hook_summaries: list[dict[str, Any]],
    strategy: str,
    dask_config: dict[str, Any],
    strategy_config: dict[str, Any] | None,
    dashboard_link: str | None,
) -> BackendResult:
    partition_stores = [item["value_store"] for item in task_results]
    warnings = [
        warning
        for item in task_results
        for warning in item.get("warnings", [])
    ]

    final_ctx = dict(base_ctx)
    final_warnings: list[dict[str, Any]] = []
    final_ctx["_warnings"] = final_warnings
    recorder = ensure_runtime_provenance(final_ctx)
    for item in task_results:
        for operation in list(item.get("provenance_operations") or []):
            if isinstance(operation, dict):
                recorder.record_operation_record(operation)
        for resource_id, resource in dict(item.get("provenance_resources") or {}).items():
            if isinstance(resource, dict):
                recorder.record_resource_record(str(resource_id), resource)
    final_hook_manager = HookManager.from_plan(plan)
    dataset_stores: list[dict[tuple[str, str], Any]] = []
    grouped_results = group_partition_results_by_dataset(
        partition_stores,
        plan.partitions,
    )
    for dataset_name, stores in grouped_results.items():
        dataset_value_store = merge_partition_value_stores_for_dataset(
            plan,
            stores,
            dataset_name=dataset_name,
            registry_cfg=plan.registry,
        )
        dataset_ctx = build_dataset_context(
            plan,
            base_ctx=final_ctx,
            dataset_name=dataset_name,
        )
        execute_dataset_sinks(
            plan,
            dataset_name=dataset_name,
            dataset_value_store=dataset_value_store,
            ctx=dataset_ctx,
            registry_cfg=plan.registry,
            hook_manager=final_hook_manager,
        )
        dataset_stores.append(dataset_value_store)

    merged_value_store = merge_partition_value_stores(
        plan,
        dataset_stores,
        registry_cfg=plan.registry,
    )

    execute_final_nodes(
        plan,
        value_store=merged_value_store,
        ctx=final_ctx,
        registry_cfg=plan.registry,
        hook_manager=final_hook_manager,
    )
    write_writer_manifests(
        plan,
        stores=partition_stores,
        partitions=plan.partitions,
        outdir=str(final_ctx.get("outdir") or "."),
        runtime_provenance=recorder,
    )
    warnings.extend(final_warnings)

    backend_summary: dict[str, Any] = {
        "name": DaskBackend.name,
        "strategy": strategy,
        "scheduler": dask_config["scheduler"],
        "n_workers": dask_config["n_workers"],
        "threads_per_worker": dask_config["threads_per_worker"],
        "processes": dask_config["processes"],
        "pools": dask_config["pools"],
    }
    if strategy_config is not None:
        backend_summary[strategy] = strategy_config
    if dashboard_link is not None:
        backend_summary["dashboard_link"] = dashboard_link

    summary: dict[str, Any] = {
        "backend": backend_summary,
        "strategy": strategy,
        "scheduler": dask_config["scheduler"],
        "workers": dask_config["n_workers"],
        "n_workers": dask_config["n_workers"],
        "threads_per_worker": dask_config["threads_per_worker"],
        "processes": dask_config["processes"],
        "pools": dask_config["pools"],
        "dashboard_link": dashboard_link,
        "npartitions": len(plan.partitions),
        "warnings": warnings,
        "outputs": _store_outputs_summary(merged_value_store),
    }
    final_hook_manager.run_end(plan=plan, ctx=base_ctx, summary=summary)
    summary["hooks"] = _merge_hook_summaries(
        [*partition_hook_summaries, summary.get("hooks") or {"enabled": []}]
    )

    return BackendResult(
        backend="dask",
        strategy=strategy,
        success=True,
        outputs={
            "value_store": merged_value_store,
            "partition_value_stores": partition_stores,
        },
        summary=summary,
    )


def _execute_partition_task(
    plan: ExecutionPlan,
    partition: ExecutionPartition,
    *,
    base_ctx: dict[str, Any],
    registry_cfg: dict[str, Any] | None,
) -> dict[str, Any]:
    partition_ctx = build_partition_context(
        plan,
        base_ctx={k: v for k, v in dict(base_ctx).items() if k != "_warnings"},
        partition=partition,
    )
    warnings: list[dict[str, Any]] = []
    partition_ctx["_warnings"] = warnings
    recorder = ensure_runtime_provenance(partition_ctx)
    hook_manager = HookManager.from_plan(plan)
    value_store = execute_plan_partition(
        plan,
        ctx=partition_ctx,
        registry_cfg=registry_cfg,
        hook_manager=hook_manager,
    )
    partition_ctx["_hook_summary"] = hook_manager.usage_summary()
    partition_meta = partition.to_context()
    for warning in warnings:
        warning.setdefault("partition", partition_meta)
    return {
        "value_store": value_store,
        "warnings": warnings,
        "hooks": partition_ctx.get("_hook_summary") or {"enabled": []},
        "provenance_operations": recorder.operation_records(),
        "provenance_resources": recorder.serialise_resources(),
    }


def _merge_hook_summaries(summaries: list[dict[str, Any]]) -> dict[str, Any]:
    merged: dict[tuple[Any, ...], dict[str, Any]] = {}
    order: list[tuple[Any, ...]] = []
    for summary in summaries:
        for item in list((summary or {}).get("enabled") or []):
            key = (
                item.get("kind"),
                tuple(item.get("events") or []),
                item.get("source"),
                repr(item.get("params")),
            )
            if key not in merged:
                order.append(key)
                merged[key] = dict(item)
                merged[key]["calls"] = 0
            merged[key]["calls"] += int(item.get("calls") or 0)
    return {"enabled": [merged[key] for key in order]}


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _default_pool(execution: dict[str, Any]) -> dict[str, Any] | None:
    pool = dict(execution.get("pools") or {}).get("default")
    if not isinstance(pool, dict):
        return None
    return pool
