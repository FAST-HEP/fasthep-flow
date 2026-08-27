from __future__ import annotations

from pathlib import Path
from typing import Any

from hepflow.backends._dask._common import compute_with_client
from hepflow.backends._dask._pooled import DaskPooledHTCondorCluster
from hepflow.backends._dask._pools import (
    DaskWorkerPool,
    dask_resources_for_resource,
    dask_worker_resource_args,
    resolve_dask_worker_pools,
)
from hepflow.backends._dask._worker_env import (
    StagedExecutionFiles,
    prepare_staged_execution_files,
    prepare_worker_environment,
    validate_transfer_file_basenames,
)
from hepflow.build_layout import BuildPaths

MISSING_DASK_JOBQUEUE_MESSAGE = (
    "Dask HTCondor strategy requires dask-jobqueue. Install the dask HTCondor "
    "extra or add dask-jobqueue to the environment."
)

_HTCONDOR_FIXED_DIRECTIVES = {
    "transfer_executable": "False",
    "transfer_output_files": '""',
    "Stream_Output": "True",
    "Stream_Error": "True",
}
_HTCONDOR_RUNTIME_DIRECTIVES = {"Output", "Error", "Log"}


def normalize_dask_htcondor_config(execution: dict[str, Any]) -> dict[str, Any]:
    pools = _resolve_htcondor_worker_pools(execution)
    pool_specs: dict[str, dict[str, Any]] = {
        pool.name: {
            "workers": pool.workers or 0,
            "job_kwargs": _htcondor_cluster_options_for_pool(pool),
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


def _resolve_htcondor_worker_pools(execution: dict[str, Any]) -> list[DaskWorkerPool]:
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


def compute_with_htcondor(
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

    htcondor_config = normalize_dask_htcondor_config(execution)
    pool_specs = _prepare_htcondor_pool_specs(
        htcondor_config["pool_specs"],
        execution=execution,
        build_paths=build_paths,
    )

    cluster = DaskPooledHTCondorCluster(pools=pool_specs)
    client = Client(cluster)
    try:
        cluster.scale(htcondor_config["scale"])
        results, dashboard_link = compute_with_client(client, tasks)
        return results, dashboard_link, htcondor_config
    finally:
        client.close()
        cluster.close()


def _prepare_htcondor_pool_specs(
    pool_specs: dict[str, dict[str, Any]],
    *,
    execution: dict[str, Any],
    build_paths: BuildPaths,
) -> dict[str, dict[str, Any]]:
    prepared = {
        name: {"workers": spec["workers"], "job_kwargs": dict(spec["job_kwargs"])}
        for name, spec in pool_specs.items()
    }
    worker_environment = prepare_worker_environment(
        execution,
        build_paths=build_paths,
    )
    staged_files = prepare_staged_execution_files(
        worker_environment,
        build_paths=build_paths,
        staging=dict(execution.get("staging") or {}),
    )
    paths = _htcondor_execution_paths(build_paths)
    for path in [build_paths.dask_htcondor_dir("submit"), *paths.values()]:
        path.mkdir(parents=True, exist_ok=True)

    for name, spec in prepared.items():
        job_kwargs = spec["job_kwargs"]
        log_directory = job_kwargs.get("log_directory")
        if log_directory is not None:
            log_path = Path(str(log_directory))
            if not log_path.is_absolute():
                log_path = build_paths.root / log_path
                job_kwargs["log_directory"] = str(log_path)
            log_path.mkdir(parents=True, exist_ok=True)
        if staged_files:
            job_kwargs = _merge_htcondor_job_kwargs(
                _htcondor_staged_execution_kwargs(staged_files),
                job_kwargs,
            )
            spec["job_kwargs"] = job_kwargs
        job_kwargs["submit_directory"] = str(build_paths.dask_htcondor_dir("submit"))
        job_kwargs["job_extra_directives"] = _merge_htcondor_directives(
            dict(job_kwargs.get("job_extra_directives") or {}),
            _htcondor_bootstrap_directives(paths),
            f"HTCondor worker pool {name!r}",
        )
    return prepared


def _htcondor_execution_paths(build_paths: BuildPaths) -> dict[str, Path]:
    return {
        "logs": build_paths.dask_htcondor_dir("logs"),
        "out": build_paths.dask_htcondor_dir("out"),
        "err": build_paths.dask_htcondor_dir("err"),
    }


def _htcondor_bootstrap_directives(paths: dict[str, Path]) -> dict[str, str]:
    return {
        **_HTCONDOR_FIXED_DIRECTIVES,
        "Output": str((paths["out"] / "worker-$(ClusterId).$(ProcId).out").resolve()),
        "Error": str((paths["err"] / "worker-$(ClusterId).$(ProcId).err").resolve()),
        "Log": str((paths["logs"] / "worker-$(ClusterId).log").resolve()),
    }


def _htcondor_staged_execution_kwargs(
    staging: StagedExecutionFiles,
) -> dict[str, object]:
    validate_transfer_file_basenames(staging.transfer_files)
    job_directives: dict[str, str] = {}
    if staging.transfer_files:
        transfer_files = ",".join(str(path.resolve()) for path in staging.transfer_files)
        job_directives.update(
            {
                "should_transfer_files": "YES",
                "when_to_transfer_output": "ON_EXIT",
                "transfer_input_files": transfer_files,
            }
        )
    return {
        "python": staging.python,
        "job_extra_directives": job_directives,
        "job_script_prologue": list(staging.bootstrap_commands),
    }


def _merge_htcondor_job_kwargs(
    common: dict[str, object],
    pool: dict[str, Any],
) -> dict[str, Any]:
    merged = dict(common)
    for key, value in pool.items():
        if key == "job_extra_directives":
            current = merged.get(key)
            current_directives = current if isinstance(current, dict) else {}
            pool_directives = value if isinstance(value, dict) else {}
            merged[key] = _merge_htcondor_directives(
                current_directives,
                pool_directives,
                "HTCondor worker environment",
            )
        elif key in {"job_script_prologue", "worker_extra_args"}:
            current = merged.get(key)
            current_items = current if isinstance(current, list) else []
            pool_items = value if isinstance(value, list) else []
            merged[key] = [*current_items, *pool_items]
        else:
            merged[key] = value
    return merged


def _htcondor_cluster_options_for_pool(pool: DaskWorkerPool) -> dict[str, Any]:
    return _htcondor_cluster_options(
        resources=pool.resources,
        config=pool.config,
        dask_resources=pool.dask_resources,
    )


def _htcondor_cluster_options(
    *,
    resources: dict[str, Any],
    config: dict[str, Any],
    dask_resources: dict[str, Any],
) -> dict[str, Any]:
    _validate_htcondor_config_options(config)

    cores = resources.get("cpus", config.get("cores"))
    if cores is not None:
        cores = int(cores)

    log_directory = config.get("log_directory")

    cluster_options: dict[str, Any] = {}
    if cores is not None:
        cluster_options["cores"] = cores
    if resources.get("memory") is not None:
        cluster_options["memory"] = resources["memory"]
    if resources.get("disk") is not None:
        cluster_options["disk"] = resources["disk"]
    if log_directory is not None:
        cluster_options["log_directory"] = log_directory

    job_extra_directives: dict[str, Any] = dict(_HTCONDOR_FIXED_DIRECTIVES)
    if config.get("queue") is not None:
        job_extra_directives["+JobFlavour"] = f'"{config["queue"]}"'
    if config.get("job_extra_directives") is not None:
        raw_directives = config["job_extra_directives"]
        if not isinstance(raw_directives, dict):
            raise ValueError("execution.config.job_extra_directives must be a mapping")
        job_extra_directives = _merge_htcondor_directives(
            job_extra_directives,
            raw_directives,
            "execution.config.job_extra_directives",
        )
    if resources.get("gpus") is not None:
        job_extra_directives.setdefault("request_gpus", resources["gpus"])
    if job_extra_directives:
        cluster_options["job_extra_directives"] = job_extra_directives

    worker_extra_args = _worker_extra_args(config.get("worker_extra_args"))
    worker_extra_args.extend(dask_worker_resource_args(dask_resources))
    if worker_extra_args:
        cluster_options["worker_extra_args"] = worker_extra_args

    return cluster_options


def _validate_htcondor_config_options(config: dict[str, Any]) -> None:
    if config.get("walltime") is not None:
        raise ValueError(
            "execution.config.walltime is not supported by the Dask HTCondor "
            "backend. Use execution.config.queue for site job flavours or "
            "execution.config.job_extra_directives for explicit HTCondor ClassAds."
        )


def _merge_htcondor_directives(
    base: dict[str, Any],
    overlay: dict[str, Any],
    source: str,
) -> dict[str, Any]:
    merged = dict(base)
    fixed_by_lower = {key.lower(): key for key in _HTCONDOR_FIXED_DIRECTIVES}
    runtime_by_lower = {key.lower(): key for key in _HTCONDOR_RUNTIME_DIRECTIVES}
    for key, value in overlay.items():
        key_lower = str(key).lower()
        fixed_key = fixed_by_lower.get(key_lower)
        if fixed_key is not None:
            expected = _HTCONDOR_FIXED_DIRECTIVES[fixed_key]
            if str(value) != expected:
                raise ValueError(
                    f"{source} cannot override HTCondor invariant {fixed_key}={expected}"
                )
        runtime_key = runtime_by_lower.get(key_lower)
        if runtime_key is not None and key in base and str(value) != str(base[key]):
            raise ValueError(
                f"{source} cannot override HTCondor invariant {runtime_key}"
            )
        merged[key] = value
    return merged


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
        "cluster_options": _htcondor_cluster_options_for_pool(pool),
    }
