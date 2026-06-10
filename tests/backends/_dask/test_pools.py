from __future__ import annotations

from hepflow.backends._dask._pools import (
    dask_resources_for_resource,
    dask_worker_resource_args,
    resolve_dask_worker_pools,
)


def test_default_pool_descriptor_creation() -> None:
    pools = resolve_dask_worker_pools(
        {
            "resources": {"default": {"cpus": 1, "memory": "4GB"}},
            "pools": {
                "default": {"resources": "default", "workers": 4, "config": {}}
            },
            "config": {"walltime": "02:00:00"},
        }
    )

    assert len(pools) == 1
    assert pools[0].name == "default"
    assert pools[0].resource_name == "default"
    assert pools[0].workers == 4
    assert pools[0].resources == {"cpus": 1, "memory": "4GB"}
    assert pools[0].dask_resources == {}
    assert pools[0].config["walltime"] == "02:00:00"


def test_gpu_pool_descriptor_creation() -> None:
    pools = resolve_dask_worker_pools(
        {
            "resources": {"gpu": {"cpus": 4, "memory": "16GB", "gpus": 1}},
            "pools": {"gpu": {"resources": "gpu", "workers": 2, "config": {}}},
            "config": {},
        }
    )

    assert len(pools) == 1
    assert pools[0].name == "gpu"
    assert pools[0].resource_name == "gpu"
    assert pools[0].workers == 2
    assert pools[0].dask_resources == {"GPU": 1}


def test_gpu_resource_maps_to_dask_worker_resource() -> None:
    assert dask_resources_for_resource({"gpus": "2"}) == {"GPU": 2}
    assert dask_worker_resource_args({"GPU": 2}) == ["--resources", "GPU=2"]


def test_pool_specific_config_overrides_global_config() -> None:
    pools = resolve_dask_worker_pools(
        {
            "resources": {"gpu": {"gpus": 1}},
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

    assert pools[0].config["queue"] == "gpu"
    assert pools[0].config["walltime"] == "01:00:00"


def test_cli_workers_override_only_default_pool_workers() -> None:
    pools = resolve_dask_worker_pools(
        {
            "resources": {"default": {}, "gpu": {"gpus": 1}},
            "pools": {
                "default": {"resources": "default", "workers": 4},
                "gpu": {"resources": "gpu", "workers": 2},
            },
            "config": {"n_workers": 8},
        }
    )

    workers_by_pool = {pool.name: pool.workers for pool in pools}
    assert workers_by_pool == {"default": 8, "gpu": 2}
