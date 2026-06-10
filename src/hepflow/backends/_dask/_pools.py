from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True, frozen=True)
class DaskWorkerPool:
    name: str
    resource_name: str
    workers: int | None
    resources: dict[str, Any]
    dask_resources: dict[str, Any]
    config: dict[str, Any] = field(default_factory=dict)


def resolve_dask_worker_pools(execution: dict[str, Any]) -> list[DaskWorkerPool]:
    pools_raw = dict(execution.get("pools") or {})
    if not pools_raw:
        return []

    resources_by_name = dict(execution.get("resources") or {})
    global_config = dict(execution.get("config") or {})
    cli_workers = global_config.get("n_workers")

    pools: list[DaskWorkerPool] = []
    for pool_name, pool_raw in pools_raw.items():
        if not isinstance(pool_raw, dict):
            raise ValueError(f"execution.pools[{pool_name!r}] must be a mapping")
        resource_name = str(pool_raw.get("resources") or "")
        if not resource_name:
            raise ValueError(
                f"execution.pools[{pool_name!r}].resources must be a string"
            )
        resource = resources_by_name.get(resource_name)
        if resource is None:
            raise ValueError(
                f"execution.pools[{pool_name!r}] references missing resource class "
                f"{resource_name!r}"
            )
        if not isinstance(resource, dict):
            raise ValueError(
                f"execution.resources[{resource_name!r}] must be a mapping"
            )
        pool_config = pool_raw.get("config") or {}
        if not isinstance(pool_config, dict):
            raise ValueError(f"execution.pools[{pool_name!r}].config must be a mapping")

        workers = pool_raw.get("workers")
        if str(pool_name) == "default" and cli_workers is not None:
            workers = cli_workers
        if workers is not None:
            workers = int(workers)

        config = {**global_config, **dict(pool_config)}
        pools.append(
            DaskWorkerPool(
                name=str(pool_name),
                resource_name=resource_name,
                workers=workers,
                resources=dict(resource),
                dask_resources=dask_resources_for_resource(resource),
                config=config,
            )
        )
    return pools


def dask_resources_for_resource(resource: dict[str, Any]) -> dict[str, Any]:
    gpus = resource.get("gpus")
    if gpus is None:
        return {}
    return {"GPU": _resource_quantity(gpus)}


def dask_worker_resource_args(dask_resources: dict[str, Any]) -> list[str]:
    if not dask_resources:
        return []
    resources_arg = ",".join(
        f"{name}={quantity}" for name, quantity in sorted(dask_resources.items())
    )
    return ["--resources", resources_arg]


def _resource_quantity(value: Any) -> Any:
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.isdigit():
            return int(stripped)
    return value
