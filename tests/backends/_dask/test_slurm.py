from __future__ import annotations

import sys
import types
from pathlib import Path
from re import escape
from typing import Any, cast

import pytest

from hepflow.backends._dask._common import DaskBackend
from hepflow.backends._dask._slurm import (
    MISSING_DASK_JOBQUEUE_MESSAGE,
    compute_with_slurm,
    normalize_dask_slurm_config,
)
from hepflow.build_layout import BuildPaths
from hepflow.model.plan import ExecutionPlan
from hepflow.runtime.config import _runtime_execution_with_overrides


def test_slurm_resources_map_to_cluster_options() -> None:
    config = normalize_dask_slurm_config(
        {
            "backend": "dask",
            "strategy": "slurm",
            "resources": {
                "default": {
                    "cpus": 1,
                    "memory": "4GB",
                    "disk": "10GB",
                }
            },
            "config": {
                "workers": 20,
                "queue": "compute",
                "account": "my-account",
                "walltime": "02:00:00",
                "job_extra_directives": ["--exclusive"],
            },
        }
    )

    assert config["workers"] == 20
    assert config["cluster_options"] == {
        "cores": 1,
        "memory": "4GB",
        "walltime": "02:00:00",
        "queue": "compute",
        "account": "my-account",
        "log_directory": "debug/dask/slurm",
        "job_extra_directives": ["--exclusive"],
    }


def test_slurm_default_pool_does_not_request_gpus() -> None:
    config = normalize_dask_slurm_config(
        {
            "backend": "dask",
            "strategy": "slurm",
            "resources": {"default": {"cpus": 1, "memory": "4GB"}},
            "pools": {
                "default": {"resources": "default", "workers": 100, "config": {}}
            },
            "config": {"queue": "compute"},
        }
    )

    assert config["workers"] == 100
    assert config["cluster_options"]["queue"] == "compute"
    assert "job_extra_directives" not in config["cluster_options"]
    assert "worker_extra_args" not in config["cluster_options"]


def test_slurm_gpu_pool_requests_gpus_and_advertises_dask_resource() -> None:
    config = normalize_dask_slurm_config(
        {
            "backend": "dask",
            "strategy": "slurm",
            "resources": {
                "gpu": {
                    "cpus": 4,
                    "memory": "16GB",
                    "disk": "20GB",
                    "gpus": 1,
                }
            },
            "pools": {
                "gpu": {
                    "resources": "gpu",
                    "workers": 2,
                    "config": {
                        "queue": "gpu",
                        "walltime": "01:00:00",
                        "job_extra_directives": ["--exclusive"],
                    },
                }
            },
            "config": {"queue": "compute", "walltime": "02:00:00"},
        }
    )

    assert config["workers"] == 2
    assert config["cluster_options"]["cores"] == 4
    assert config["cluster_options"]["memory"] == "16GB"
    assert config["cluster_options"]["queue"] == "gpu"
    assert config["cluster_options"]["walltime"] == "01:00:00"
    assert config["cluster_options"]["job_extra_directives"] == [
        "--exclusive",
        "--gres=gpu:1",
    ]
    assert config["cluster_options"]["worker_extra_args"] == [
        "--resources",
        "GPU=1",
    ]
    assert config["pools"][0]["dask_resources"] == {"GPU": 1}


def test_slurm_multiple_pools_fail_clearly() -> None:
    with pytest.raises(
        NotImplementedError,
        match="Dask Slurm strategy does not yet support heterogeneous worker pools",
    ):
        normalize_dask_slurm_config(
            {
                "backend": "dask",
                "strategy": "slurm",
                "resources": {"default": {}, "gpu": {"gpus": 1}},
                "pools": {
                    "default": {"resources": "default", "workers": 100},
                    "gpu": {"resources": "gpu", "workers": 2},
                },
                "config": {},
            }
        )


def test_slurm_missing_dask_jobqueue_errors_clearly(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setitem(sys.modules, "dask_jobqueue", None)

    with pytest.raises(RuntimeError, match=escape(MISSING_DASK_JOBQUEUE_MESSAGE)):
        compute_with_slurm(
            [],
            execution={"backend": "dask", "strategy": "slurm", "config": {}},
            build_paths=BuildPaths(root=tmp_path),
        )


def test_slurm_cluster_scales_workers_and_computes_tasks(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    calls: dict[str, Any] = {}

    class FakeSLURMCluster:
        def __init__(self, **kwargs: Any) -> None:
            calls["cluster_options"] = kwargs
            calls["cluster"] = self
            self.scaled_to: int | None = None
            self.closed = False

        def scale(self, workers: int) -> None:
            self.scaled_to = workers

        def close(self) -> None:
            self.closed = True

    class FakeClient:
        dashboard_link = "http://scheduler.example/status"

        def __init__(self, cluster: FakeSLURMCluster) -> None:
            calls["client_cluster"] = cluster
            self.closed = False

        def compute(self, tasks: list[Any]) -> list[Any]:
            calls["computed_tasks"] = tasks
            return tasks

        def gather(self, futures: list[Any]) -> list[Any]:
            calls["gathered_futures"] = futures
            return [{"value_store": {}, "warnings": [], "hooks": {"enabled": []}}]

        def close(self) -> None:
            self.closed = True
            calls["client_closed"] = True

    dask_jobqueue = types.ModuleType("dask_jobqueue")
    cast(Any, dask_jobqueue).SLURMCluster = FakeSLURMCluster
    distributed = types.ModuleType("distributed")
    cast(Any, distributed).Client = FakeClient
    monkeypatch.setitem(sys.modules, "dask_jobqueue", dask_jobqueue)
    monkeypatch.setitem(sys.modules, "distributed", distributed)

    results, dashboard_link, config = compute_with_slurm(
        ["task"],
        execution={
            "backend": "dask",
            "strategy": "slurm",
            "resources": {"default": {"cpus": 2, "memory": "8GB", "disk": "12GB"}},
            "config": {
                "workers": 3,
                "queue": "compute",
                "account": "my-account",
                "job_extra_directives": ["--exclusive"],
            },
        },
        build_paths=BuildPaths(root=tmp_path),
    )

    assert results == [{"value_store": {}, "warnings": [], "hooks": {"enabled": []}}]
    assert dashboard_link == "http://scheduler.example/status"
    assert config["workers"] == 3
    assert calls["cluster"].scaled_to == 3
    assert calls["cluster"].closed is True
    assert calls["client_closed"] is True
    assert calls["cluster_options"]["cores"] == 2
    assert calls["cluster_options"]["memory"] == "8GB"
    assert calls["cluster_options"]["queue"] == "compute"
    assert calls["cluster_options"]["account"] == "my-account"
    assert calls["cluster_options"]["job_extra_directives"] == ["--exclusive"]


def test_cli_workers_override_slurm_config_workers() -> None:
    execution = _runtime_execution_with_overrides(
        {
            "backend": "dask",
            "strategy": "slurm",
            "resources": {"default": {"cpus": 1}},
            "config": {"workers": 20},
        },
        backend=None,
        strategy=None,
        scheduler=None,
        workers=5,
    )

    config = normalize_dask_slurm_config(execution)

    assert execution["config"]["workers"] == 20
    assert execution["config"]["n_workers"] == 5
    assert config["workers"] == 5


def test_dask_backend_dispatches_to_slurm(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: dict[str, Any] = {}

    def fake_compute_with_slurm(
        tasks: list[Any],
        *,
        execution: dict[str, Any],
        build_paths: BuildPaths,
    ) -> tuple[list[Any], str | None, dict[str, Any]]:
        calls["tasks"] = tasks
        calls["execution"] = execution
        calls["build_paths"] = build_paths
        return [], None, {"workers": 2, "cluster_options": {}}

    monkeypatch.setattr(
        "hepflow.backends._dask._slurm.compute_with_slurm",
        fake_compute_with_slurm,
    )
    dask = types.ModuleType("dask")
    cast(Any, dask).compute = lambda *tasks, **_kwargs: tasks
    cast(Any, dask).delayed = lambda func: func
    monkeypatch.setitem(sys.modules, "dask", dask)

    plan = ExecutionPlan(
        execution={
            "backend": "dask",
            "strategy": "slurm",
            "profiles": [],
            "resources": {},
            "config": {"workers": 2},
        },
        context={"outdir": "."},
    )

    result = DaskBackend().run(plan)

    assert calls["execution"] == plan.execution
    assert result.strategy == "slurm"
    assert result.summary["strategy"] == "slurm"
    assert result.summary["backend"]["slurm"] == {
        "workers": 2,
        "cluster_options": {},
    }
