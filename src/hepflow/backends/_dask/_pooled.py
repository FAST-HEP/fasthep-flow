from __future__ import annotations

from dataclasses import dataclass
from typing import Any, ClassVar

from distributed import Scheduler
from distributed.core import Status as DistributedStatus
from distributed.deploy.spec import SpecCluster
from distributed.utils import NoOpAwaitable

from hepflow.backends._dask._pools import dask_worker_resource_args


@dataclass(slots=True, frozen=True)
class DaskPooledWorkerPool:
    name: str
    workers: int
    job_kwargs: dict[str, Any]


class DaskPooledCluster(SpecCluster):
    """
    Experimental internal prototype for one scheduler with multiple job templates.

    It reuses dask-jobqueue Job classes for worker submission while letting each
    pool carry independent job options and Dask worker resource labels.
    """

    job_cls: ClassVar[type[Any] | None] = None

    def __init__(
        self,
        *,
        pools: dict[str, dict[str, Any]],
        schedd_class: type[Any] = Scheduler,
        scheduler_options: dict[str, Any] | None = None,
        asynchronous: bool = False,
        start: bool = True,
        **kwargs: Any,
    ) -> None:
        self.pools = normalize_pooled_worker_pools(pools)
        self.scheduler_options = dict(scheduler_options or {})
        self._pooled_started = start
        self._worker_pool: dict[str, str] = {}
        job_cls = self.resolve_job_cls()

        scheduler = self.build_scheduler_spec(schedd_class, self.scheduler_options)
        workers = self.build_worker_specs(
            self.pools,
            job_cls,
            scale={name: pool.workers for name, pool in self.pools.items()},
        )
        worker_template = {"cls": job_cls, "options": {}}

        if not start:
            self.scheduler_spec = scheduler
            self.worker_spec = workers
            self.new_spec = worker_template
            return

        if Scheduler is None:
            raise RuntimeError(
                "Dask pooled cluster requires distributed. Install the Dask extra."
            )

        super().__init__(
            workers=workers,
            scheduler=scheduler,
            worker=worker_template,
            asynchronous=asynchronous,
            **kwargs,
        )

    @classmethod
    def resolve_job_cls(cls) -> type[Any]:
        if cls.job_cls is None:
            raise RuntimeError("Dask pooled cluster requires a job_cls")
        return cls.job_cls

    @staticmethod
    def build_scheduler_spec(
        schedd_class: type[Any],
        scheduler_options: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return {"cls": schedd_class, "options": dict(scheduler_options or {})}

    def build_worker_specs(
        self,
        pools: dict[str, DaskPooledWorkerPool],
        job_cls: type[Any],
        *,
        scale: dict[str, int],
    ) -> dict[str, dict[str, Any]]:
        specs: dict[str, dict[str, Any]] = {}
        for pool_name, count in scale.items():
            pool = pools.get(pool_name)
            if pool is None:
                raise ValueError(f"Unknown Dask worker pool {pool_name!r}")
            worker_count = _normalize_worker_count(
                count,
                f"scale[{pool_name!r}]",
                allow_zero=True,
            )
            for idx in range(worker_count):
                worker_name = f"{pool.name}-{idx}"
                specs[worker_name] = {
                    "cls": job_cls,
                    "options": dict(pool.job_kwargs),
                }
                self._worker_pool[worker_name] = pool.name
        return specs

    def scale(  # type: ignore[override]
        self,
        n: int | dict[str, int] = 0,
        memory: Any = None,
        cores: Any = None,
    ) -> Any:
        if memory is not None or cores is not None:
            raise ValueError("DaskPooledCluster.scale does not support memory/cores")
        scale = self._normalize_scale(n)
        self._worker_pool = {}
        self.worker_spec = self.build_worker_specs(
            self.pools,
            self.resolve_job_cls(),
            scale=scale,
        )

        if not self._pooled_started:
            return None

        if (
            self.status not in (DistributedStatus.closing, DistributedStatus.closed)
            and self.loop is not None
        ):
            self.loop.add_callback(self._correct_state)
        if self.asynchronous:
            return NoOpAwaitable()
        return None

    def _normalize_scale(self, n: int | dict[str, int]) -> dict[str, int]:
        if isinstance(n, int):
            if n == 0:
                return dict.fromkeys(self.pools, 0)
            if set(self.pools) != {"default"}:
                raise ValueError(
                    "Integer scale is only supported for a single default pool"
                )
            return {"default": _normalize_worker_count(n, "scale", allow_zero=True)}
        if not isinstance(n, dict):
            raise ValueError("DaskPooledCluster.scale expects an int or mapping")

        unknown = sorted(set(n) - set(self.pools))
        if unknown:
            raise ValueError(f"Unknown Dask worker pool {unknown[0]!r}")
        scale = dict.fromkeys(self.pools, 0)
        for pool_name, count in n.items():
            scale[pool_name] = _normalize_worker_count(
                count,
                f"scale[{pool_name!r}]",
                allow_zero=True,
            )
        return scale


class DaskPooledHTCondorCluster(DaskPooledCluster):
    @classmethod
    def resolve_job_cls(cls) -> type[Any]:
        try:
            from dask_jobqueue.htcondor import HTCondorJob  # noqa: PLC0415
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "Dask pooled HTCondor cluster requires dask-jobqueue."
            ) from exc
        return HTCondorJob


class DaskPooledSlurmCluster(DaskPooledCluster):
    @classmethod
    def resolve_job_cls(cls) -> type[Any]:
        try:
            from dask_jobqueue.slurm import SLURMJob  # noqa: PLC0415
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "Dask pooled Slurm cluster requires dask-jobqueue."
            ) from exc
        return SLURMJob


def normalize_pooled_worker_pools(
    pools: dict[str, dict[str, Any]],
) -> dict[str, DaskPooledWorkerPool]:
    if not isinstance(pools, dict) or not pools:
        raise ValueError("Dask pooled cluster pools must be a non-empty mapping")

    normalized: dict[str, DaskPooledWorkerPool] = {}
    for pool_name, pool_raw in pools.items():
        if not isinstance(pool_name, str) or not pool_name.strip():
            raise ValueError("Dask pooled cluster pool names must be non-empty strings")
        if not isinstance(pool_raw, dict):
            raise ValueError(f"Dask worker pool {pool_name!r} must be a mapping")

        workers = _normalize_worker_count(
            pool_raw.get("workers", 0),
            f"pools[{pool_name!r}].workers",
            allow_zero=True,
        )
        job_kwargs_raw = pool_raw.get("job_kwargs") or {}
        if not isinstance(job_kwargs_raw, dict):
            raise ValueError(
                f"Dask worker pool {pool_name!r}.job_kwargs must be a mapping"
            )
        normalized[pool_name] = DaskPooledWorkerPool(
            name=pool_name,
            workers=workers,
            job_kwargs=_normalize_job_kwargs(job_kwargs_raw),
        )
    return normalized


def _normalize_job_kwargs(job_kwargs: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(job_kwargs)
    resources = normalized.pop("resources", None)
    if resources is None:
        return normalized
    if not isinstance(resources, dict):
        raise ValueError("Dask worker pool job_kwargs.resources must be a mapping")

    worker_extra_args = list(normalized.get("worker_extra_args") or [])
    if not all(isinstance(item, str) for item in worker_extra_args):
        raise ValueError("Dask worker pool worker_extra_args must be a list of strings")
    worker_extra_args.extend(dask_worker_resource_args(resources))
    normalized["worker_extra_args"] = worker_extra_args
    return normalized


def _normalize_worker_count(value: Any, field_name: str, *, allow_zero: bool) -> int:
    if not isinstance(value, int):
        raise ValueError(f"{field_name} must be an integer")
    minimum = 0 if allow_zero else 1
    if value < minimum:
        comparator = "non-negative" if allow_zero else "positive"
        raise ValueError(f"{field_name} must be a {comparator} integer")
    return value
