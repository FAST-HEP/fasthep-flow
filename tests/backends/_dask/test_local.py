from __future__ import annotations

from hepflow.backends._dask._common import normalise_dask_config
from hepflow.runtime.config import _runtime_execution_with_overrides


def test_dask_local_config_reads_plan_execution_config() -> None:
    config = normalise_dask_config(
        {
            "backend": "dask",
            "strategy": "local",
            "config": {
                "workers": 4,
                "threads_per_worker": 1,
                "processes": True,
            },
        }
    )

    assert config["use_local_cluster"] is True
    assert config["scheduler"] == "distributed"
    assert config["n_workers"] == 4
    assert config["threads_per_worker"] == 1
    assert config["processes"] is True


def test_dask_local_workers_config_is_applied() -> None:
    config = normalise_dask_config(
        {
            "backend": "dask",
            "strategy": "local",
            "config": {"workers": "2"},
        }
    )

    assert config["use_local_cluster"] is True
    assert config["n_workers"] == 2


def test_cli_workers_override_dask_config_workers() -> None:
    execution = _runtime_execution_with_overrides(
        {
            "backend": "dask",
            "strategy": "local",
            "config": {"workers": 4, "threads_per_worker": 1},
        },
        backend=None,
        strategy=None,
        scheduler=None,
        workers=8,
    )

    config = normalise_dask_config(execution)

    assert execution["config"]["workers"] == 4
    assert execution["config"]["n_workers"] == 8
    assert config["n_workers"] == 8
