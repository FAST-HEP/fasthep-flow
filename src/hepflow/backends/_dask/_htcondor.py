from __future__ import annotations

from pathlib import Path
from typing import Any

from hepflow.backends._dask._common import compute_with_client
from hepflow.build_layout import BuildPaths

MISSING_DASK_JOBQUEUE_MESSAGE = (
    "Dask HTCondor strategy requires dask-jobqueue. Install the dask HTCondor "
    "extra or add dask-jobqueue to the environment."
)


def normalize_dask_htcondor_config(execution: dict[str, Any]) -> dict[str, Any]:
    config = dict(execution.get("config") or {})
    default_resources = dict((execution.get("resources") or {}).get("default") or {})

    workers = config.get("n_workers", config.get("workers"))
    if workers is not None:
        workers = int(workers)

    cores = default_resources.get("cpus", config.get("cores"))
    if cores is not None:
        cores = int(cores)

    log_directory = config.get("log_directory")
    if log_directory is None:
        log_directory = "debug/dask/htcondor"

    cluster_options: dict[str, Any] = {}
    if cores is not None:
        cluster_options["cores"] = cores
    if default_resources.get("memory") is not None:
        cluster_options["memory"] = default_resources["memory"]
    if default_resources.get("disk") is not None:
        cluster_options["disk"] = default_resources["disk"]
    if config.get("walltime") is not None:
        cluster_options["walltime"] = config["walltime"]
    if log_directory is not None:
        cluster_options["log_directory"] = log_directory

    job_extra_directives: dict[str, Any] = {}
    if config.get("queue") is not None:
        job_extra_directives["+JobFlavour"] = f'"{config["queue"]}"'
    if config.get("job_extra_directives") is not None:
        raw_directives = config["job_extra_directives"]
        if not isinstance(raw_directives, dict):
            raise ValueError(
                "execution.config.job_extra_directives must be a mapping"
            )
        job_extra_directives.update(raw_directives)
    if job_extra_directives:
        cluster_options["job_extra_directives"] = job_extra_directives

    return {
        "workers": workers,
        "cluster_options": cluster_options,
    }


def compute_with_htcondor(
    tasks: list[Any],
    *,
    execution: dict[str, Any],
    build_paths: BuildPaths,
) -> tuple[list[Any], str | None, dict[str, Any]]:
    try:
        from dask_jobqueue import HTCondorCluster  # noqa: PLC0415
    except ModuleNotFoundError as exc:
        raise RuntimeError(MISSING_DASK_JOBQUEUE_MESSAGE) from exc

    from distributed import Client  # noqa: PLC0415

    htcondor_config = normalize_dask_htcondor_config(execution)
    cluster_options = dict(htcondor_config["cluster_options"])
    log_directory = cluster_options.get("log_directory")
    if log_directory is not None:
        log_path = Path(str(log_directory))
        if not log_path.is_absolute():
            log_path = build_paths.root / log_path
            cluster_options["log_directory"] = str(log_path)
        log_path.mkdir(parents=True, exist_ok=True)

    cluster = HTCondorCluster(**cluster_options)
    client = Client(cluster)
    try:
        workers = htcondor_config["workers"]
        if workers is not None:
            cluster.scale(workers)
        results, dashboard_link = compute_with_client(client, tasks)
        return results, dashboard_link, htcondor_config
    finally:
        client.close()
        cluster.close()
