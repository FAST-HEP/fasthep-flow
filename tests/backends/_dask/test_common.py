from __future__ import annotations

from typing import Any

import pytest

from hepflow.backends._dask._common import (
    compute_with_client,
    normalise_dask_config,
    normalise_dask_strategy,
    validate_supported_dask_pools,
)


def test_compute_with_client_uses_client_compute_and_gather() -> None:
    calls: dict[str, Any] = {}

    class FakeClient:
        dashboard_link = "http://scheduler.example/status"

        def compute(self, tasks: list[Any]) -> list[Any]:
            calls["tasks"] = tasks
            return ["future"]

        def gather(self, futures: list[Any]) -> list[Any]:
            calls["futures"] = futures
            return [{"ok": True}]

    results, dashboard_link = compute_with_client(FakeClient(), ["task"])

    assert calls == {"tasks": ["task"], "futures": ["future"]}
    assert results == [{"ok": True}]
    assert dashboard_link == "http://scheduler.example/status"


def test_dask_default_strategy_normalises_to_local() -> None:
    assert normalise_dask_strategy({"backend": "dask", "strategy": "default"}) == "local"


def test_dask_slurm_strategy_is_supported() -> None:
    assert normalise_dask_strategy({"backend": "dask", "strategy": "slurm"}) == "slurm"


def test_dask_unsupported_strategy_errors_clearly() -> None:
    with pytest.raises(
        ValueError,
        match=r"Dask strategy 'pbs' is not implemented yet\.",
    ):
        normalise_dask_strategy({"backend": "dask", "strategy": "pbs"})


def test_normalise_dask_config_preserves_empty_config_defaults() -> None:
    config = normalise_dask_config(
        {
            "backend": "dask",
            "strategy": "local",
            "config": {},
        }
    )

    assert config["use_local_cluster"] is False
    assert config["scheduler"] == "threads"
    assert config["n_workers"] is None


def test_dask_default_pool_is_supported() -> None:
    validate_supported_dask_pools(
        {
            "pools": {
                "default": {"resources": "default", "workers": 4, "config": {}}
            }
        },
        strategy="local",
    )


def test_dask_heterogeneous_pools_fail_clearly() -> None:
    with pytest.raises(
        NotImplementedError,
        match="does not support heterogeneous worker pools yet",
    ):
        validate_supported_dask_pools(
            {
                "pools": {
                    "default": {"resources": "default", "workers": 100},
                    "gpu": {"resources": "gpu", "workers": 2},
                }
            },
            strategy="htcondor",
        )
