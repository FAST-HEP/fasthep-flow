from __future__ import annotations

from typing import Any

import pytest

from hepflow.backends._dask._pooled import (
    DaskPooledCluster,
    normalize_pooled_worker_pools,
)


class FakeJob:
    pass


class FakePooledCluster(DaskPooledCluster):
    job_cls = FakeJob


def test_pooled_cluster_creates_scheduler_spec() -> None:
    cluster = FakePooledCluster(
        pools=_pool_config(),
        scheduler_options={"dashboard_address": ":0"},
        start=False,
    )

    assert cluster.scheduler_spec["options"] == {"dashboard_address": ":0"}


def test_pooled_cluster_creates_initial_worker_specs() -> None:
    cluster = FakePooledCluster(pools=_pool_config(), start=False)

    assert sorted(cluster.worker_spec) == [
        "default-0",
        "default-1",
        "high_memory-0",
    ]
    assert cluster.worker_spec["default-0"]["cls"] is FakeJob
    assert "pool" not in cluster.worker_spec["default-0"]["options"]
    assert "pool" not in cluster.worker_spec["high_memory-0"]["options"]
    assert cluster._worker_pool == {
        "default-0": "default",
        "default-1": "default",
        "high_memory-0": "high_memory",
    }


def test_pooled_cluster_worker_specs_keep_distinct_job_kwargs() -> None:
    cluster = FakePooledCluster(pools=_pool_config(), start=False)

    assert cluster.worker_spec["default-0"]["options"]["memory"] == "4GB"
    assert cluster.worker_spec["high_memory-0"]["options"]["memory"] == "32GB"
    assert cluster.worker_spec["default-0"]["options"]["worker_extra_args"] == [
        "--resources",
        "resource.default=1",
    ]
    assert cluster.worker_spec["high_memory-0"]["options"]["worker_extra_args"] == [
        "--resources",
        "resource.high_memory=1",
    ]


def test_pooled_cluster_scale_mapping_replaces_worker_targets() -> None:
    cluster = FakePooledCluster(pools=_pool_config(), start=False)

    cluster.scale({"default": 1, "high_memory": 2})

    assert sorted(cluster.worker_spec) == [
        "default-0",
        "high_memory-0",
        "high_memory-1",
    ]
    assert cluster._worker_pool == {
        "default-0": "default",
        "high_memory-0": "high_memory",
        "high_memory-1": "high_memory",
    }


def test_pooled_cluster_integer_scale_zero_clears_multiple_pools() -> None:
    cluster = FakePooledCluster(pools=_pool_config(), start=False)

    cluster.scale(0)

    assert cluster.worker_spec == {}
    assert cluster._worker_pool == {}


def test_pooled_cluster_normalize_scale_zero_targets_all_pools() -> None:
    cluster = FakePooledCluster(pools=_pool_config(), start=False)

    assert cluster._normalize_scale(0) == {"default": 0, "high_memory": 0}


def test_pooled_cluster_integer_scale_supports_single_default_pool() -> None:
    cluster = FakePooledCluster(
        pools={
            "default": {
                "workers": 1,
                "job_kwargs": {
                    "cores": 1,
                    "memory": "4GB",
                    "resources": {"resource.default": 1},
                },
            }
        },
        start=False,
    )

    cluster.scale(3)

    assert sorted(cluster.worker_spec) == ["default-0", "default-1", "default-2"]


def test_pooled_cluster_integer_scale_errors_for_multiple_pools() -> None:
    cluster = FakePooledCluster(pools=_pool_config(), start=False)

    with pytest.raises(
        ValueError,
        match="Integer scale is only supported for a single default pool",
    ):
        cluster.scale(3)


def test_pooled_cluster_unknown_pool_errors_clearly() -> None:
    cluster = FakePooledCluster(pools=_pool_config(), start=False)

    with pytest.raises(ValueError, match="Unknown Dask worker pool 'missing'"):
        cluster.scale({"missing": 1})


@pytest.mark.parametrize("workers", [-1, "two"])
def test_pooled_cluster_invalid_worker_counts_error(workers: Any) -> None:
    with pytest.raises(ValueError, match=r"pools\['default'\]\.workers must be"):
        normalize_pooled_worker_pools(
            {"default": {"workers": workers, "job_kwargs": {}}}
        )


def test_pooled_cluster_invalid_scale_count_errors() -> None:
    cluster = FakePooledCluster(pools=_pool_config(), start=False)

    with pytest.raises(ValueError, match=r"scale\['default'\] must be"):
        cluster.scale({"default": -1})


def _pool_config() -> dict[str, dict[str, Any]]:
    return {
        "default": {
            "workers": 2,
            "job_kwargs": {
                "cores": 1,
                "memory": "4GB",
                "disk": "10GB",
                "resources": {"resource.default": 1},
            },
        },
        "high_memory": {
            "workers": 1,
            "job_kwargs": {
                "cores": 2,
                "memory": "32GB",
                "disk": "20GB",
                "resources": {"resource.high_memory": 1},
            },
        },
    }
