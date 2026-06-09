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
    }


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

    class FakeHTCondorCluster:
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

        def __init__(self, cluster: FakeHTCondorCluster) -> None:
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
    cast(Any, dask_jobqueue).HTCondorCluster = FakeHTCondorCluster
    distributed = types.ModuleType("distributed")
    cast(Any, distributed).Client = FakeClient
    monkeypatch.setitem(sys.modules, "dask_jobqueue", dask_jobqueue)
    monkeypatch.setitem(sys.modules, "distributed", distributed)

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
    assert calls["cluster"].scaled_to == 3
    assert calls["cluster"].closed is True
    assert calls["client_closed"] is True
    assert calls["cluster_options"]["cores"] == 2
    assert calls["cluster_options"]["memory"] == "8GB"
    assert calls["cluster_options"]["disk"] == "12GB"
    assert calls["cluster_options"]["job_extra_directives"] == {
        "+JobFlavour": '"workday"'
    }


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
