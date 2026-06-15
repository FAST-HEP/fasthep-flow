from __future__ import annotations

from pathlib import Path
from typing import Any

from hepflow.backends._dask._common import compute_with_client
from hepflow.backends._dask._pooled import DaskPooledSlurmCluster
from hepflow.backends._dask._pools import (
    DaskWorkerPool,
    dask_resources_for_resource,
    dask_worker_resource_args,
    resolve_dask_worker_pools,
)
from hepflow.build_layout import BuildPaths

MISSING_DASK_JOBQUEUE_MESSAGE = (
    "Dask Slurm strategy requires dask-jobqueue. Install the dask Slurm "
    "extra or add dask-jobqueue to the environment."
)


def normalize_dask_slurm_config(execution: dict[str, Any]) -> dict[str, Any]:
    pools = _resolve_slurm_worker_pools(execution)
    pool_specs: dict[str, dict[str, Any]] = {
        pool.name: {
            "workers": pool.workers or 0,
            "job_kwargs": _slurm_cluster_options_for_pool(pool),
        }
        for pool in pools
    }
    scale = {pool.name: pool.workers or 0 for pool in pools}
    first_pool = pools[0]
    first_options = dict(pool_specs[first_pool.name]["job_kwargs"])

    return {
        "workers": first_pool.workers,
        "cluster_options": first_options,
        "pool_specs": pool_specs,
        "scale": scale,
        "pools": [_pool_summary(pool) for pool in pools],
    }


def _resolve_slurm_worker_pools(execution: dict[str, Any]) -> list[DaskWorkerPool]:
    pools = resolve_dask_worker_pools(execution)
    if pools:
        return pools

    config = dict(execution.get("config") or {})
    resources_by_name = dict(execution.get("resources") or {})
    default_resources = dict(resources_by_name.get("default") or {})
    workers = config.get("n_workers", config.get("workers"))
    if workers is not None:
        workers = int(workers)
    dask_resources = (
        dask_resources_for_resource("default", default_resources)
        if "default" in resources_by_name
        else {}
    )
    return [
        DaskWorkerPool(
            name="default",
            resource_name="default",
            workers=workers,
            resources=default_resources,
            dask_resources=dask_resources,
            config=config,
        )
    ]


def compute_with_slurm(
    tasks: list[Any],
    *,
    execution: dict[str, Any],
    build_paths: BuildPaths,
) -> tuple[list[Any], str | None, dict[str, Any]]:
    try:
        import dask_jobqueue  # noqa: F401, PLC0415
    except ModuleNotFoundError as exc:
        raise RuntimeError(MISSING_DASK_JOBQUEUE_MESSAGE) from exc

    from distributed import Client  # noqa: PLC0415

    slurm_config = normalize_dask_slurm_config(execution)
    pool_specs = _prepare_slurm_pool_specs(
        slurm_config["pool_specs"],
        build_paths=build_paths,
    )

    cluster = DaskPooledSlurmCluster(pools=pool_specs)
    client = Client(cluster)
    try:
        cluster.scale(slurm_config["scale"])
        results, dashboard_link = compute_with_client(client, tasks)
        return results, dashboard_link, slurm_config
    finally:
        client.close()
        cluster.close()


def _prepare_slurm_pool_specs(
    pool_specs: dict[str, dict[str, Any]],
    *,
    build_paths: BuildPaths,
) -> dict[str, dict[str, Any]]:
    prepared = {
        name: {"workers": spec["workers"], "job_kwargs": dict(spec["job_kwargs"])}
        for name, spec in pool_specs.items()
    }
    for spec in prepared.values():
        job_kwargs = spec["job_kwargs"]
        log_directory = job_kwargs.get("log_directory")
        if log_directory is not None:
            log_path = Path(str(log_directory))
            if not log_path.is_absolute():
                log_path = build_paths.root / log_path
                job_kwargs["log_directory"] = str(log_path)
            log_path.mkdir(parents=True, exist_ok=True)
    return prepared


def _normalize_job_extra_directives(raw: Any) -> list[str]:
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise ValueError("execution.config.job_extra_directives must be a list")
    directives: list[str] = []
    for idx, item in enumerate(raw):
        if not isinstance(item, str) or not item.strip():
            raise ValueError(
                f"execution.config.job_extra_directives[{idx}] must be a non-empty string"
            )
        directives.append(item.strip())
    return directives


def _slurm_cluster_options_for_pool(pool: DaskWorkerPool) -> dict[str, Any]:
    return _slurm_cluster_options(
        resources=pool.resources,
        config=pool.config,
        dask_resources=pool.dask_resources,
    )


def _slurm_cluster_options(
    *,
    resources: dict[str, Any],
    config: dict[str, Any],
    dask_resources: dict[str, Any],
) -> dict[str, Any]:
    cores = resources.get("cpus", config.get("cores"))
    if cores is not None:
        cores = int(cores)

    log_directory = config.get("log_directory")
    if log_directory is None:
        log_directory = "debug/dask/slurm"

    cluster_options: dict[str, Any] = {}
    if cores is not None:
        cluster_options["cores"] = cores
    if resources.get("memory") is not None:
        cluster_options["memory"] = resources["memory"]
    if config.get("walltime") is not None:
        cluster_options["walltime"] = config["walltime"]
    if config.get("queue") is not None:
        cluster_options["queue"] = config["queue"]
    if config.get("account") is not None:
        cluster_options["account"] = config["account"]
    if log_directory is not None:
        cluster_options["log_directory"] = log_directory

    job_extra_directives = _normalize_job_extra_directives(
        config.get("job_extra_directives")
    )
    if resources.get("gpus") is not None:
        job_extra_directives.append(f"--gres=gpu:{resources['gpus']}")
    if job_extra_directives:
        cluster_options["job_extra_directives"] = job_extra_directives

    worker_extra_args = _worker_extra_args(config.get("worker_extra_args"))
    worker_extra_args.extend(dask_worker_resource_args(dask_resources))
    if worker_extra_args:
        cluster_options["worker_extra_args"] = worker_extra_args

    return cluster_options


def _worker_extra_args(raw: Any) -> list[str]:
    if raw is None:
        return []
    if not isinstance(raw, list) or not all(isinstance(item, str) for item in raw):
        raise ValueError("execution.config.worker_extra_args must be a list of strings")
    return list(raw)


def _pool_summary(pool: DaskWorkerPool) -> dict[str, Any]:
    return {
        "name": pool.name,
        "resources": pool.resource_name,
        "workers": pool.workers,
        "dask_resources": pool.dask_resources,
        "cluster_options": _slurm_cluster_options_for_pool(pool),
    }
