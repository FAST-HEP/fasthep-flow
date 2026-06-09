from __future__ import annotations

from pathlib import Path
from typing import Any

from hepflow.backends.dask_htcondor import compute_with_htcondor
from hepflow.backends.local import _store_outputs_summary
from hepflow.backends.model import BackendResult
from hepflow.build_layout import BuildPaths
from hepflow.model.plan import ExecutionPartition, ExecutionPlan
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


class DaskLocalBackend:
    """
    Partition-granular Dask backend.

    The default thread scheduler avoids most pickling constraints. The process
    scheduler is exposed for early testing, but plans and runtime objects must
    be picklable for it to work.
    """

    name = "dask.local"

    def run(
        self,
        plan: ExecutionPlan,
        *,
        ctx: dict[str, Any] | None = None,
    ) -> BackendResult:
        from dask import compute, delayed  # noqa: PLC0415

        strategy = _normalize_dask_strategy(plan.execution)
        dask_config = _normalize_dask_local_config(plan.execution)

        base_ctx = dict(plan.context)
        base_ctx.update(dict(ctx or {}))

        tasks = [
            delayed(_execute_partition_task)(
                plan,
                partition,
                base_ctx=base_ctx,
                registry_cfg=plan.registry,
            )
            for partition in plan.partitions
        ]

        dashboard_link: str | None = None
        htcondor_config: dict[str, Any] | None = None
        if strategy == "htcondor":
            task_results, dashboard_link, htcondor_config = compute_with_htcondor(
                tasks,
                execution=plan.execution,
                build_paths=BuildPaths.from_ctx(base_ctx),
            )
        elif dask_config["use_local_cluster"]:
            task_results, dashboard_link = _compute_distributed(
                tasks,
                n_workers=dask_config["n_workers"],
                threads_per_worker=dask_config["threads_per_worker"],
                processes=dask_config["processes"],
                memory_limit=dask_config["memory_limit"],
                dashboard_address=dask_config["dashboard_address"],
                local_directory=dask_config["local_directory"],
                build_paths=BuildPaths.from_ctx(base_ctx),
            )
        elif dask_config["scheduler"] in {"threads", "processes", "synchronous"}:
            compute_kwargs: dict[str, Any] = {"scheduler": dask_config["scheduler"]}
            if dask_config["n_workers"] is not None:
                compute_kwargs["num_workers"] = dask_config["n_workers"]
            task_results = list(compute(*tasks, **compute_kwargs)) if tasks else []
        else:
            raise ValueError(
                "dask.local scheduler must be one of: "
                "threads, processes, synchronous, distributed"
            )

        partition_stores = [item["value_store"] for item in task_results]
        partition_hook_summaries = [
            item.get("hooks") or {"enabled": []}
            for item in task_results
        ]
        warnings = [
            warning
            for item in task_results
            for warning in item.get("warnings", [])
        ]

        final_ctx = dict(base_ctx)
        final_warnings: list[dict[str, Any]] = []
        final_ctx["_warnings"] = final_warnings
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
        warnings.extend(final_warnings)

        backend_summary: dict[str, Any] = {
            "name": self.name,
            "strategy": strategy,
            "scheduler": dask_config["scheduler"],
            "n_workers": dask_config["n_workers"],
            "threads_per_worker": dask_config["threads_per_worker"],
            "processes": dask_config["processes"],
        }
        if htcondor_config is not None:
            backend_summary["htcondor"] = htcondor_config
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


def _normalize_dask_strategy(execution: dict[str, Any]) -> str:
    strategy = str(execution.get("strategy") or "default")
    if strategy in {"default", "local"}:
        return "local"
    if strategy == "htcondor":
        return "htcondor"
    raise ValueError(f"Dask strategy {strategy!r} is not implemented yet.")


def _normalize_dask_local_config(execution: dict[str, Any]) -> dict[str, Any]:
    config = dict(execution.get("config") or {})
    scheduler = str(config.get("scheduler") or "threads")
    n_workers = config.get("n_workers", config.get("workers"))
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
    }


def _compute_distributed(
    tasks: list[Any],
    *,
    n_workers: int | None,
    threads_per_worker: int,
    processes: bool,
    memory_limit: Any,
    dashboard_address: Any,
    local_directory: Any,
    build_paths: BuildPaths,
) -> tuple[list[Any], str | None]:
    from distributed import Client, LocalCluster  # noqa: PLC0415

    if local_directory is None:
        local_directory = str(build_paths.debug_dir("dask"))
    Path(str(local_directory)).mkdir(parents=True, exist_ok=True)

    cluster = LocalCluster(
        n_workers=n_workers,
        threads_per_worker=threads_per_worker,
        processes=processes,
        memory_limit=memory_limit,
        dashboard_address=dashboard_address,
        local_directory=str(local_directory),
    )
    client = Client(cluster)
    try:
        dashboard_link = getattr(client, "dashboard_link", None)
        if not tasks:
            return [], dashboard_link
        futures = client.compute(tasks)
        return list(client.gather(futures)), dashboard_link
    finally:
        client.close()
        cluster.close()


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


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
