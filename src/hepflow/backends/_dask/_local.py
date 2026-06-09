from __future__ import annotations

from pathlib import Path
from typing import Any

from hepflow.backends._dask._common import compute_with_client, compute_with_scheduler
from hepflow.build_layout import BuildPaths


def compute_with_local_strategy(
    tasks: list[Any],
    *,
    dask_config: dict[str, Any],
    build_paths: BuildPaths,
) -> tuple[list[Any], str | None]:
    if dask_config["use_local_cluster"]:
        return compute_with_local_cluster(
            tasks,
            n_workers=dask_config["n_workers"],
            threads_per_worker=dask_config["threads_per_worker"],
            processes=dask_config["processes"],
            memory_limit=dask_config["memory_limit"],
            dashboard_address=dask_config["dashboard_address"],
            local_directory=dask_config["local_directory"],
            build_paths=build_paths,
        )
    if dask_config["scheduler"] in {"threads", "processes", "synchronous"}:
        return (
            compute_with_scheduler(
                tasks,
                scheduler=dask_config["scheduler"],
                n_workers=dask_config["n_workers"],
            ),
            None,
        )
    raise ValueError(
        "dask.local scheduler must be one of: "
        "threads, processes, synchronous, distributed"
    )


def compute_with_local_cluster(
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
        return compute_with_client(client, tasks)
    finally:
        client.close()
        cluster.close()
