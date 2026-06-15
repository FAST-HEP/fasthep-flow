from __future__ import annotations

import sys
import types
from pathlib import Path
from re import escape
from typing import Any, cast

import pytest

from hepflow.backends._dask._common import DaskBackend
from hepflow.backends._dask._htcondor import (
    MISSING_DASK_JOBQUEUE_MESSAGE,
    _prepare_htcondor_pool_specs,
    compute_with_htcondor,
    normalize_dask_htcondor_config,
)
from hepflow.build_layout import BuildPaths
from hepflow.model.plan import ExecutionPlan
from hepflow.runtime.config import _runtime_execution_with_overrides


def test_htcondor_resources_map_to_cluster_options() -> None:
    config = normalize_dask_htcondor_config(
        {
            "backend": "dask",
            "strategy": "htcondor",
            "resources": {
                "default": {
                    "cpus": 1,
                    "memory": "4GB",
                    "disk": "10GB",
                }
            },
            "config": {
                "workers": 20,
                "queue": "workday",
                "walltime": "02:00:00",
                "log_directory": "debug/dask/htcondor",
            },
        }
    )

    assert config["workers"] == 20
    assert config["cluster_options"] == {
        "cores": 1,
        "memory": "4GB",
        "disk": "10GB",
        "walltime": "02:00:00",
        "log_directory": "debug/dask/htcondor",
        "job_extra_directives": {"+JobFlavour": '"workday"'},
        "worker_extra_args": ["--resources", "resource.default=1"],
    }


def test_htcondor_default_pool_does_not_request_gpus() -> None:
    config = normalize_dask_htcondor_config(
        {
            "backend": "dask",
            "strategy": "htcondor",
            "resources": {"default": {"cpus": 1, "memory": "4GB"}},
            "pools": {
                "default": {"resources": "default", "workers": 100, "config": {}}
            },
            "config": {"queue": "workday"},
        }
    )

    assert config["workers"] == 100
    assert config["cluster_options"]["job_extra_directives"] == {
        "+JobFlavour": '"workday"'
    }
    assert config["cluster_options"]["worker_extra_args"] == [
        "--resources",
        "resource.default=1",
    ]


def test_htcondor_high_memory_pool_requests_high_memory() -> None:
    config = normalize_dask_htcondor_config(
        {
            "backend": "dask",
            "strategy": "htcondor",
            "resources": {
                "high_memory": {
                    "cpus": 8,
                    "memory": "128GB",
                    "disk": "100GB",
                }
            },
            "pools": {
                "preprocess": {
                    "resources": "high_memory",
                    "workers": 2,
                    "config": {"queue": "long", "walltime": "04:00:00"},
                }
            },
            "config": {"queue": "workday", "walltime": "02:00:00"},
        }
    )

    assert config["workers"] == 2
    assert config["cluster_options"]["cores"] == 8
    assert config["cluster_options"]["memory"] == "128GB"
    assert config["cluster_options"]["disk"] == "100GB"
    assert config["cluster_options"]["walltime"] == "04:00:00"
    assert config["cluster_options"]["job_extra_directives"] == {
        "+JobFlavour": '"long"'
    }
    assert config["cluster_options"]["worker_extra_args"] == [
        "--resources",
        "resource.high_memory=1",
    ]
    assert config["pools"][0]["dask_resources"] == {"resource.high_memory": 1}


def test_htcondor_gpu_pool_requests_gpus_and_advertises_dask_resource() -> None:
    config = normalize_dask_htcondor_config(
        {
            "backend": "dask",
            "strategy": "htcondor",
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
                    "config": {"queue": "gpu", "walltime": "01:00:00"},
                }
            },
            "config": {"queue": "workday", "walltime": "02:00:00"},
        }
    )

    assert config["workers"] == 2
    assert config["cluster_options"]["cores"] == 4
    assert config["cluster_options"]["memory"] == "16GB"
    assert config["cluster_options"]["disk"] == "20GB"
    assert config["cluster_options"]["walltime"] == "01:00:00"
    assert config["cluster_options"]["job_extra_directives"] == {
        "+JobFlavour": '"gpu"',
        "request_gpus": 1,
    }
    assert config["cluster_options"]["worker_extra_args"] == [
        "--resources",
        "GPU=1,resource.gpu=1",
    ]
    assert config["pools"][0]["dask_resources"] == {"resource.gpu": 1, "GPU": 1}


def test_htcondor_multiple_pools_create_pooled_specs() -> None:
    config = normalize_dask_htcondor_config(
        {
            "backend": "dask",
            "strategy": "htcondor",
            "resources": {
                "default": {"cpus": 1, "memory": "4GB"},
                "gpu": {"cpus": 4, "memory": "16GB", "gpus": 1},
            },
            "pools": {
                "default": {"resources": "default", "workers": 100},
                "gpu": {"resources": "gpu", "workers": 2},
            },
            "config": {"queue": "workday"},
        }
    )

    assert config["scale"] == {"default": 100, "gpu": 2}
    assert sorted(config["pool_specs"]) == ["default", "gpu"]
    assert config["pool_specs"]["default"]["job_kwargs"]["memory"] == "4GB"
    assert config["pool_specs"]["gpu"]["job_kwargs"]["memory"] == "16GB"
    assert config["pool_specs"]["gpu"]["job_kwargs"]["job_extra_directives"] == {
        "+JobFlavour": '"workday"',
        "request_gpus": 1,
    }
    assert config["pool_specs"]["gpu"]["job_kwargs"]["worker_extra_args"] == [
        "--resources",
        "GPU=1,resource.gpu=1",
    ]


def test_htcondor_missing_dask_jobqueue_errors_clearly(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setitem(sys.modules, "dask_jobqueue", None)

    with pytest.raises(RuntimeError, match=escape(MISSING_DASK_JOBQUEUE_MESSAGE)):
        compute_with_htcondor(
            [],
            execution={"backend": "dask", "strategy": "htcondor", "config": {}},
            build_paths=BuildPaths(root=tmp_path),
        )


def test_htcondor_cluster_scales_workers_and_computes_tasks(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    calls: dict[str, Any] = {}

    class FakePooledHTCondorCluster:
        def __init__(self, *, pools: dict[str, Any]) -> None:
            calls["pools"] = pools
            calls["cluster"] = self
            self.scaled_to: dict[str, int] | None = None
            self.closed = False

        def scale(self, workers: dict[str, int]) -> None:
            self.scaled_to = workers

        def close(self) -> None:
            self.closed = True

    class FakeClient:
        dashboard_link = "http://scheduler.example/status"

        def __init__(self, cluster: FakePooledHTCondorCluster) -> None:
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
    distributed = types.ModuleType("distributed")
    cast(Any, distributed).Client = FakeClient
    monkeypatch.setitem(sys.modules, "dask_jobqueue", dask_jobqueue)
    monkeypatch.setitem(sys.modules, "distributed", distributed)
    monkeypatch.setattr(
        "hepflow.backends._dask._htcondor.DaskPooledHTCondorCluster",
        FakePooledHTCondorCluster,
    )

    results, dashboard_link, config = compute_with_htcondor(
        ["task"],
        execution={
            "backend": "dask",
            "strategy": "htcondor",
            "resources": {"default": {"cpus": 2, "memory": "8GB", "disk": "12GB"}},
            "config": {"workers": 3, "queue": "workday"},
        },
        build_paths=BuildPaths(root=tmp_path),
    )

    assert results == [{"value_store": {}, "warnings": [], "hooks": {"enabled": []}}]
    assert dashboard_link == "http://scheduler.example/status"
    assert config["workers"] == 3
    assert calls["cluster"].scaled_to == {"default": 3}
    assert calls["cluster"].closed is True
    assert calls["client_closed"] is True
    job_kwargs = calls["pools"]["default"]["job_kwargs"]
    assert job_kwargs["cores"] == 2
    assert job_kwargs["memory"] == "8GB"
    assert job_kwargs["disk"] == "12GB"
    assert job_kwargs["job_extra_directives"] == {
        "+JobFlavour": '"workday"'
    }
    assert "python" not in job_kwargs


def test_htcondor_packed_pixi_environment_kwargs_added_when_requested(
    tmp_path: Path,
) -> None:
    execution = {
        "backend": "dask",
        "strategy": "htcondor",
        "resources": {"default": {"cpus": 1, "memory": "4GB"}},
        "config": {"workers": 1},
        "environment": {
            "type": "packed-pixi",
            "environment": "default",
            "archive_path": "debug/distributed/htcondor/env.sh",
            "worker_env_dir": "worker-env",
        },
    }
    config = normalize_dask_htcondor_config(execution)

    prepared = _prepare_htcondor_pool_specs(
        config["pool_specs"],
        execution=execution,
        build_paths=BuildPaths(root=tmp_path),
    )

    job_kwargs = prepared["default"]["job_kwargs"]
    assert job_kwargs["python"] == "./worker-env/bin/python"
    assert "job_script_prologue" in job_kwargs
    directives = job_kwargs["job_extra_directives"]
    assert directives["transfer_input_files"].endswith(
        "/debug/distributed/htcondor/env.sh"
    )
    assert directives["transfer_executable"] == "False"


def test_cli_workers_override_htcondor_config_workers() -> None:
    execution = _runtime_execution_with_overrides(
        {
            "backend": "dask",
            "strategy": "htcondor",
            "resources": {"default": {"cpus": 1}},
            "config": {"workers": 20},
        },
        backend=None,
        strategy=None,
        scheduler=None,
        workers=5,
    )

    config = normalize_dask_htcondor_config(execution)

    assert execution["config"]["workers"] == 20
    assert execution["config"]["n_workers"] == 5
    assert config["workers"] == 5


def test_dask_backend_dispatches_to_htcondor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: dict[str, Any] = {}

    def fake_compute_with_htcondor(
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
        "hepflow.backends._dask._htcondor.compute_with_htcondor",
        fake_compute_with_htcondor,
    )
    dask = types.ModuleType("dask")
    cast(Any, dask).compute = lambda *tasks, **_kwargs: tasks
    cast(Any, dask).delayed = lambda func: func
    monkeypatch.setitem(sys.modules, "dask", dask)

    plan = ExecutionPlan(
        execution={
            "backend": "dask",
            "strategy": "htcondor",
            "profiles": [],
            "resources": {},
            "config": {"workers": 2},
        },
        context={"outdir": "."},
    )

    result = DaskBackend().run(plan)

    assert calls["execution"] == plan.execution
    assert result.strategy == "htcondor"
    assert result.summary["strategy"] == "htcondor"
    assert result.summary["backend"]["htcondor"] == {
        "workers": 2,
        "cluster_options": {},
    }
