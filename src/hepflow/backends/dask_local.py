from __future__ import annotations

from pathlib import Path
from typing import Any

from hepflow.backends.local import _store_outputs_summary
from hepflow.backends.model import BackendResult
from hepflow.model.plan import ExecutionPartition, ExecutionPlan
from hepflow.runtime.engine import (
    build_dataset_context,
    build_partition_context,
    execute_dataset_sinks,
    execute_final_nodes,
    execute_plan_partition,
    group_partition_results_by_dataset,
    merge_partition_value_stores_for_dataset,
    merge_partition_value_stores,
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
        from dask import compute, delayed

        config = dict((plan.execution.get("config") or {}))
        scheduler = str(config.get("scheduler") or "threads")
        n_workers = config.get("n_workers", config.get("workers"))
        if n_workers is not None:
            n_workers = int(n_workers)
        threads_per_worker = int(config.get("threads_per_worker") or 1)
        processes = _as_bool(config.get("processes", scheduler == "processes"))
        memory_limit = config.get("memory_limit")
        dashboard_address = config.get("dashboard_address")

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
        if scheduler == "distributed":
            task_results, dashboard_link = _compute_distributed(
                tasks,
                n_workers=n_workers,
                threads_per_worker=threads_per_worker,
                processes=processes,
                memory_limit=memory_limit,
                dashboard_address=dashboard_address,
                local_directory=config.get("local_directory"),
                outdir=base_ctx.get("outdir"),
            )
        elif scheduler in {"threads", "processes", "synchronous"}:
            compute_kwargs: dict[str, Any] = {"scheduler": scheduler}
            if n_workers is not None:
                compute_kwargs["num_workers"] = n_workers
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
            dataset_value_store = merge_partition_value_stores_for_dataset(plan, stores)
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

        merged_value_store = merge_partition_value_stores(plan, dataset_stores)

        execute_final_nodes(
            plan,
            value_store=merged_value_store,
            ctx=final_ctx,
            registry_cfg=plan.registry,
            hook_manager=final_hook_manager,
        )
        warnings.extend(final_warnings)

        backend_summary = {
            "name": self.name,
            "scheduler": scheduler,
            "n_workers": n_workers,
            "threads_per_worker": threads_per_worker,
            "processes": processes,
        }
        if dashboard_link is not None:
            backend_summary["dashboard_link"] = dashboard_link

        summary = {
            "backend": backend_summary,
            "strategy": "local",
            "scheduler": scheduler,
            "workers": n_workers,
            "n_workers": n_workers,
            "threads_per_worker": threads_per_worker,
            "processes": processes,
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
            strategy="local",
            success=True,
            outputs={
                "value_store": merged_value_store,
                "partition_value_stores": partition_stores,
            },
            summary=summary,
        )


def _compute_distributed(
    tasks: list[Any],
    *,
    n_workers: int | None,
    threads_per_worker: int,
    processes: bool,
    memory_limit: Any,
    dashboard_address: Any,
    local_directory: Any,
    outdir: Any,
) -> tuple[list[Any], str | None]:
    from distributed import Client, LocalCluster

    if local_directory is None:
        if outdir:
            local_directory = str(Path(outdir) / "dask")
        else:
            local_directory = ".dask"
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
        if dashboard_link:
            print(f"Dask dashboard: {dashboard_link}")
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
