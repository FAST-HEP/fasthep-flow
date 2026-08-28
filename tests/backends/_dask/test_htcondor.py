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
    _wait_for_htcondor_workers,
    compute_with_htcondor,
    normalize_dask_htcondor_config,
)
from hepflow.backends._dask._worker_env import PreparedWorkerEnvironment
from hepflow.build_layout import BuildPaths
from hepflow.model.plan import ExecutionPlan
from hepflow.progress import ProgressReporter, ProgressUpdate
from hepflow.runtime.config import _runtime_execution_with_overrides


def _bootstrap_directives(**extra: Any) -> dict[str, Any]:
    return {
        "transfer_executable": "False",
        "transfer_output_files": '""',
        "Stream_Output": "True",
        "Stream_Error": "True",
        **extra,
    }


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
                "log_directory": "debug/dask/htcondor",
            },
        }
    )

    assert config["workers"] == 20
    assert config["cluster_options"] == {
        "cores": 1,
        "memory": "4GB",
        "disk": "10GB",
        "log_directory": "debug/dask/htcondor",
        "job_extra_directives": _bootstrap_directives(
            **{"+JobFlavour": '"workday"'},
        ),
        "worker_extra_args": ["--resources", "resource.default=1"],
    }
    assert "walltime" not in config["cluster_options"]


def test_htcondor_unsupported_walltime_fails_clearly() -> None:
    with pytest.raises(
        ValueError,
        match=r"execution\.config\.walltime is not supported by the Dask HTCondor backend",
    ):
        normalize_dask_htcondor_config(
            {
                "backend": "dask",
                "strategy": "htcondor",
                "resources": {"default": {"cpus": 1, "memory": "4GB", "disk": "10GB"}},
                "config": {"workers": 20, "walltime": "02:00:00"},
            }
        )


def test_htcondor_conflicting_transfer_executable_fails_clearly() -> None:
    with pytest.raises(
        ValueError,
        match="cannot override HTCondor invariant transfer_executable=False",
    ):
        normalize_dask_htcondor_config(
            {
                "backend": "dask",
                "strategy": "htcondor",
                "resources": {"default": {"cpus": 1, "memory": "4GB", "disk": "10GB"}},
                "config": {
                    "workers": 20,
                    "job_extra_directives": {"transfer_executable": "True"},
                },
            }
        )


def test_htcondor_conflicting_output_log_override_fails_clearly(
    tmp_path: Path,
) -> None:
    execution = {
        "backend": "dask",
        "strategy": "htcondor",
        "resources": {"default": {"cpus": 1, "memory": "4GB", "disk": "10GB"}},
        "config": {"workers": 1, "job_extra_directives": {"Output": "worker.out"}},
    }
    config = normalize_dask_htcondor_config(execution)

    with pytest.raises(ValueError, match="cannot override HTCondor invariant Output"):
        _prepare_htcondor_pool_specs(
            config["pool_specs"],
            execution=execution,
            build_paths=BuildPaths(root=tmp_path),
        )


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
    assert config["cluster_options"]["job_extra_directives"] == _bootstrap_directives(
        **{"+JobFlavour": '"workday"'},
    )
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
                    "config": {"queue": "long"},
                }
            },
            "config": {"queue": "workday"},
        }
    )

    assert config["workers"] == 2
    assert config["cluster_options"]["cores"] == 8
    assert config["cluster_options"]["memory"] == "128GB"
    assert config["cluster_options"]["disk"] == "100GB"
    assert config["cluster_options"]["job_extra_directives"] == _bootstrap_directives(
        **{"+JobFlavour": '"long"'},
    )
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
                    "config": {"queue": "gpu"},
                }
            },
            "config": {"queue": "workday"},
        }
    )

    assert config["workers"] == 2
    assert config["cluster_options"]["cores"] == 4
    assert config["cluster_options"]["memory"] == "16GB"
    assert config["cluster_options"]["disk"] == "20GB"
    assert config["cluster_options"]["job_extra_directives"] == _bootstrap_directives(
        **{"+JobFlavour": '"gpu"'},
        request_gpus=1,
    )
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
    assert config["pool_specs"]["gpu"]["job_kwargs"]["job_extra_directives"] == (
        _bootstrap_directives(**{"+JobFlavour": '"workday"'}, request_gpus=1)
    )
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

        def wait_for_workers(self, n_workers: int, *, timeout: float) -> None:
            calls["wait_for_workers"] = (n_workers, timeout)

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
    assert config["worker_start_timeout"] == 120
    assert calls["wait_for_workers"] == (3, 120)
    assert calls["cluster"].scaled_to == {"default": 3}
    assert calls["cluster"].closed is True
    assert calls["client_closed"] is True
    job_kwargs = calls["pools"]["default"]["job_kwargs"]
    assert job_kwargs["cores"] == 2
    assert job_kwargs["memory"] == "8GB"
    assert job_kwargs["disk"] == "12GB"
    assert job_kwargs["job_extra_directives"] == _bootstrap_directives(
        **{"+JobFlavour": '"workday"'},
        Output=str(
            tmp_path
            / "execution"
            / "dask"
            / "htcondor"
            / "out"
            / "worker-$(ClusterId).$(ProcId).out"
        ),
        Error=str(
            tmp_path
            / "execution"
            / "dask"
            / "htcondor"
            / "err"
            / "worker-$(ClusterId).$(ProcId).err"
        ),
        Log=str(
            tmp_path
            / "execution"
            / "dask"
            / "htcondor"
            / "logs"
            / "worker-$(ClusterId).log"
        ),
    )
    assert job_kwargs["submit_directory"] == str(
        tmp_path / "execution" / "dask" / "htcondor" / "submit"
    )
    assert "python" not in job_kwargs


def test_htcondor_worker_start_timeout_is_configurable() -> None:
    config = normalize_dask_htcondor_config(
        {
            "backend": "dask",
            "strategy": "htcondor",
            "resources": {"default": {"cpus": 1}},
            "config": {"workers": 1, "worker_start_timeout": "30"},
        }
    )

    assert config["worker_start_timeout"] == 30


def test_wait_for_htcondor_workers_reports_timeout_details(tmp_path: Path) -> None:
    class FakeClient:
        def wait_for_workers(self, n_workers: int, *, timeout: float) -> None:
            raise TimeoutError("no workers")

        def scheduler_info(self) -> dict[str, Any]:
            return {"workers": {"worker-a": {}, "worker-b": {}}}

    with pytest.raises(
        TimeoutError,
        match=(
            r"requested=3, connected=2, pools=default, high_memory, "
            r"timeout=5s, logs="
        ),
    ):
        _wait_for_htcondor_workers(
            FakeClient(),
            requested=3,
            pool_names=["default", "high_memory"],
            timeout=5,
            build_paths=BuildPaths(root=tmp_path),
        )


def test_htcondor_timeout_closes_client_and_cluster(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    calls: dict[str, Any] = {}

    class FakePooledHTCondorCluster:
        def __init__(self, *, pools: dict[str, Any]) -> None:
            calls["cluster"] = self
            self.closed = False

        def scale(self, workers: dict[str, int]) -> None:
            calls["scaled"] = workers

        def close(self) -> None:
            self.closed = True
            calls["cluster_closed"] = True

    class FakeClient:
        dashboard_link = None

        def __init__(self, cluster: FakePooledHTCondorCluster) -> None:
            calls["client"] = self

        def wait_for_workers(self, n_workers: int, *, timeout: float) -> None:
            raise TimeoutError("workers did not start")

        def scheduler_info(self) -> dict[str, Any]:
            return {"workers": {}}

        def close(self) -> None:
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

    with pytest.raises(TimeoutError, match="requested=1, connected=0"):
        compute_with_htcondor(
            ["task"],
            execution={
                "backend": "dask",
                "strategy": "htcondor",
                "resources": {"default": {"cpus": 1}},
                "config": {"workers": 1, "worker_start_timeout": 1},
            },
            build_paths=BuildPaths(root=tmp_path),
        )

    assert calls["client_closed"] is True
    assert calls["cluster_closed"] is True


def test_htcondor_shared_environment_adds_no_transfer_inputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    archive = tmp_path / "env.tar.gz"
    bootstrap = tmp_path / "bootstrap.sh"
    manifest = tmp_path / "manifest.json"
    for path in [archive, bootstrap, manifest]:
        path.write_text("x", encoding="utf-8")

    def fake_prepare_worker_environment(
        execution: dict[str, Any],
        *,
        build_paths: BuildPaths,
        progress: Any | None = None,
    ) -> PreparedWorkerEnvironment:
        del progress
        return PreparedWorkerEnvironment(
            python="./worker-env/bin/python",
            bootstrap_commands=["set -e", f". {bootstrap}"],
            transfer_files=[],
            env={},
            environment_manifest=manifest,
            environment_archive=archive,
            bootstrap_script=bootstrap,
        )

    monkeypatch.setattr(
        "hepflow.backends._dask._htcondor.prepare_worker_environment",
        fake_prepare_worker_environment,
    )
    execution = {
        "backend": "dask",
        "strategy": "htcondor",
        "resources": {"default": {"cpus": 1, "memory": "4GB"}},
        "config": {"workers": 1},
        "environment": {
            "type": "packed-pixi",
            "mode": "prefix",
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
    assert "transfer_input_files" not in directives
    assert directives["transfer_executable"] == "False"
    assert directives["transfer_output_files"] == '""'


def test_htcondor_prefix_environment_translates_prepared_environment(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    env_archive = tmp_path / "execution" / "worker-environments" / "env" / "prefix.tar.gz"
    snapshot = (
        tmp_path
        / "execution"
        / "worker-environments"
        / "env"
        / "editable-snapshot.tar.gz"
    )
    bootstrap = tmp_path / "execution" / "worker-environments" / "env" / "bootstrap.sh"
    manifest = tmp_path / "execution" / "worker-environments" / "env" / "manifest.json"
    compile_dir = tmp_path / "compile"
    for path in [env_archive, snapshot, bootstrap, manifest]:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("x", encoding="utf-8")
    compile_dir.mkdir()
    for name in ["plan.yaml", "normalized.yaml", "deps.yaml"]:
        (compile_dir / name).write_text(name, encoding="utf-8")

    calls = 0

    def fake_prepare_worker_environment(
        execution: dict[str, Any],
        *,
        build_paths: BuildPaths,
        progress: Any | None = None,
    ) -> PreparedWorkerEnvironment:
        del progress
        nonlocal calls
        calls += 1
        return PreparedWorkerEnvironment(
            python="./worker-env/bin/python",
            bootstrap_commands=["set -e", ". ./bootstrap.sh"],
            transfer_files=[env_archive, bootstrap, snapshot],
            env={},
            environment_manifest=manifest,
            environment_archive=env_archive,
            bootstrap_script=bootstrap,
            editable_snapshot_archive=snapshot,
        )

    monkeypatch.setattr(
        "hepflow.backends._dask._htcondor.prepare_worker_environment",
        fake_prepare_worker_environment,
    )
    execution = {
        "backend": "dask",
        "strategy": "htcondor",
        "resources": {"default": {"cpus": 1, "memory": "4GB"}},
        "pools": {
            "default": {"resources": "default", "workers": 1},
            "second": {"resources": "default", "workers": 1},
        },
        "config": {},
        "staging": {"mode": "transfer"},
        "environment": {
            "type": "packed-pixi",
            "mode": "prefix",
        },
    }
    config = normalize_dask_htcondor_config(execution)

    prepared = _prepare_htcondor_pool_specs(
        config["pool_specs"],
        execution=execution,
        build_paths=BuildPaths(root=tmp_path),
    )

    assert calls == 1
    for spec in prepared.values():
        job_kwargs = spec["job_kwargs"]
        assert job_kwargs["python"] == "./worker-env/bin/python"
        assert job_kwargs["job_script_prologue"][:3] == [
            "set -e",
            "mkdir -p compile",
            "tar -xzf compile.tar.gz -C compile",
        ]
        directives = job_kwargs["job_extra_directives"]
        assert "execution/staging/compile.tar.gz" in directives["transfer_input_files"]
        assert "execution/staging/prefix.tar.gz" in directives["transfer_input_files"]
        assert (
            "execution/staging/editable-snapshot.tar.gz"
            in directives["transfer_input_files"]
        )
        assert directives["transfer_executable"] == "False"
        assert directives["transfer_output_files"] == '""'
        for item in directives["transfer_input_files"].split(","):
            assert Path(item).exists()


def test_htcondor_preparation_progress_reports_order_and_sizes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    prepared_env = _prepared_worker_environment(tmp_path, include_snapshot=True)
    build_paths = BuildPaths(root=tmp_path)
    build_paths.compile_dir().mkdir(parents=True)
    for name in ["plan.yaml", "normalized.yaml", "deps.yaml"]:
        build_paths.compile_file(name).write_text(name, encoding="utf-8")

    def fake_prepare_worker_environment(
        execution: dict[str, Any],
        *,
        build_paths: BuildPaths,
        progress: Any | None = None,
    ) -> PreparedWorkerEnvironment:
        del execution, build_paths
        assert progress is not None
        progress.step("packing_worker_environment", staging_mode="transfer")
        return prepared_env

    monkeypatch.setattr(
        "hepflow.backends._dask._htcondor.prepare_worker_environment",
        fake_prepare_worker_environment,
    )
    sink = _CollectSink()
    reporter = ProgressReporter([], sinks=[sink])
    config = normalize_dask_htcondor_config(_htcondor_execution(staging="transfer"))

    _prepare_htcondor_pool_specs(
        config["pool_specs"],
        execution=_htcondor_execution(staging="transfer"),
        build_paths=build_paths,
        progress=reporter,
    )
    reporter.close()

    events = [
        update.event for update in sink.updates
        if update.event.phase == "Preparing distributed execution"
    ]
    assert [event.kind for event in events] == [
        "phase_started",
        "phase_started",
        "phase_started",
        "phase_started",
        "phase_completed",
    ]
    assert [event.detail.get("step") for event in events[:-1]] == [
        "resolving_editable_snapshots",
        "packing_worker_environment",
        "preparing_compilation_and_staging_files",
        "ready_to_submit_workers",
    ]
    completed = events[-1].detail
    assert completed["status"] == "completed"
    assert completed["staging_mode"] == "transfer"
    assert completed["elapsed_seconds"] >= 0
    assert completed["worker_environment"]["prefix_archive_bytes"] == 7
    assert completed["worker_environment"]["editable_snapshot_bytes"] == 8
    assert completed["staging"]["transfer_file_count"] == 4
    assert completed["staging"]["transfer_bytes"] > 0


def test_htcondor_preparation_progress_shared_mode_omits_snapshot_and_transfer(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    prepared_env = _prepared_worker_environment(tmp_path, include_snapshot=False)

    def fake_prepare_worker_environment(
        execution: dict[str, Any],
        *,
        build_paths: BuildPaths,
        progress: Any | None = None,
    ) -> PreparedWorkerEnvironment:
        del execution, build_paths, progress
        return prepared_env

    monkeypatch.setattr(
        "hepflow.backends._dask._htcondor.prepare_worker_environment",
        fake_prepare_worker_environment,
    )
    sink = _CollectSink()
    reporter = ProgressReporter([], sinks=[sink])
    config = normalize_dask_htcondor_config(_htcondor_execution())

    _prepare_htcondor_pool_specs(
        config["pool_specs"],
        execution=_htcondor_execution(),
        build_paths=BuildPaths(root=tmp_path),
        progress=reporter,
    )
    reporter.close()

    completed = next(
        update.event.detail for update in sink.updates
        if update.event.kind == "phase_completed"
        and update.event.phase == "Preparing distributed execution"
    )
    assert completed["staging_mode"] == "shared"
    assert "editable_snapshot_bytes" not in completed["worker_environment"]
    assert completed["staging"] == {"transfer_file_count": 0, "transfer_bytes": 0}


def test_htcondor_preparation_progress_reports_failure_and_reraises(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    class SnapshotFailure(RuntimeError):
        pass

    def fake_prepare_worker_environment(
        execution: dict[str, Any],
        *,
        build_paths: BuildPaths,
        progress: Any | None = None,
    ) -> PreparedWorkerEnvironment:
        del execution, build_paths
        assert progress is not None
        progress.step("packing_worker_environment")
        raise SnapshotFailure("snapshot failed")

    monkeypatch.setattr(
        "hepflow.backends._dask._htcondor.prepare_worker_environment",
        fake_prepare_worker_environment,
    )
    sink = _CollectSink()
    reporter = ProgressReporter([], sinks=[sink])
    config = normalize_dask_htcondor_config(_htcondor_execution(staging="transfer"))

    with pytest.raises(SnapshotFailure, match="snapshot failed"):
        _prepare_htcondor_pool_specs(
            config["pool_specs"],
            execution=_htcondor_execution(staging="transfer"),
            build_paths=BuildPaths(root=tmp_path),
            progress=reporter,
        )
    reporter.close()

    failed = next(
        update.event.detail for update in sink.updates
        if update.event.kind == "phase_completed"
        and update.event.phase == "Preparing distributed execution"
    )
    assert failed["status"] == "failed"
    assert failed["active_step"] == "packing_worker_environment"
    assert failed["exception_type"] == "SnapshotFailure"


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
        progress: Any | None = None,
    ) -> tuple[list[Any], str | None, dict[str, Any]]:
        calls["tasks"] = tasks
        calls["execution"] = execution
        calls["build_paths"] = build_paths
        calls["progress"] = progress
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
    assert calls["progress"] is None
    assert result.strategy == "htcondor"
    assert result.summary["strategy"] == "htcondor"
    assert result.summary["backend"]["htcondor"] == {
        "workers": 2,
        "cluster_options": {},
    }


class _CollectSink:
    def __init__(self) -> None:
        self.updates: list[ProgressUpdate] = []

    def handle(self, update: ProgressUpdate) -> None:
        self.updates.append(update)


def _prepared_worker_environment(
    tmp_path: Path,
    *,
    include_snapshot: bool,
) -> PreparedWorkerEnvironment:
    env_dir = tmp_path / "execution" / "worker-environments" / "env"
    env_dir.mkdir(parents=True, exist_ok=True)
    prefix = env_dir / "prefix.tar.gz"
    bootstrap = env_dir / "bootstrap.sh"
    manifest = env_dir / "manifest.json"
    prefix.write_text("prefix\n", encoding="utf-8")
    bootstrap.write_text("bootstrap\n", encoding="utf-8")
    manifest.write_text("{}", encoding="utf-8")
    snapshot = None
    if include_snapshot:
        snapshot = env_dir / "editable-snapshot.tar.gz"
        snapshot.write_text("snapshot", encoding="utf-8")
    return PreparedWorkerEnvironment(
        python="./worker-env/bin/python",
        bootstrap_commands=["set -e", ". ./bootstrap.sh"],
        transfer_files=[],
        env={},
        environment_manifest=manifest,
        environment_archive=prefix,
        bootstrap_script=bootstrap,
        editable_snapshot_archive=snapshot,
    )


def _htcondor_execution(*, staging: str = "shared") -> dict[str, Any]:
    execution = {
        "backend": "dask",
        "strategy": "htcondor",
        "resources": {"default": {"cpus": 1, "memory": "4GB"}},
        "config": {"workers": 1},
        "environment": {"type": "packed-pixi", "mode": "prefix"},
    }
    if staging != "shared":
        execution["staging"] = {"mode": staging}
    return execution
