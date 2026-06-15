from __future__ import annotations

import sys
import types
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, cast

import pytest

from hepflow.backends._dask._common import (
    build_dask_graph,
    compute_with_client,
    dask_resource_annotations_for_node,
    normalise_dask_config,
    normalise_dask_strategy,
    validate_supported_dask_pools,
)
from hepflow.model.plan import ExecutionNode, ExecutionPartition, ExecutionPlan


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


@pytest.mark.parametrize("strategy", ["htcondor", "slurm"])
def test_dask_jobqueue_heterogeneous_pools_are_supported(strategy: str) -> None:
    validate_supported_dask_pools(
        {
            "pools": {
                "default": {"resources": "default", "workers": 100},
                "gpu": {"resources": "gpu", "workers": 2},
            }
        },
        strategy=strategy,
    )


def test_dask_local_heterogeneous_pools_fail_clearly() -> None:
    with pytest.raises(
        NotImplementedError,
        match="Dask local strategy does not yet support heterogeneous worker pools",
    ):
        validate_supported_dask_pools(
            {
                "pools": {
                    "default": {"resources": "default", "workers": 4},
                    "gpu": {"resources": "gpu", "workers": 1},
                }
            },
            strategy="local",
        )


def test_dask_resource_annotations_for_required_gpu_node() -> None:
    plan = _plan_with_node_execution(
        {"require": "gpu"},
        resources={"gpu": {"gpus": 1}},
    )

    assert dask_resource_annotations_for_node(plan, "stage.HeavyInference") == {
        "resource.gpu": 1,
        "GPU": 1,
    }


def test_dask_resource_annotations_for_required_high_memory_node() -> None:
    plan = _plan_with_node_execution(
        {"require": "high_memory"},
        resources={"high_memory": {"cpus": 8, "memory": "128GB"}},
    )

    assert dask_resource_annotations_for_node(plan, "stage.HeavyInference") == {
        "resource.high_memory": 1
    }


def test_dask_resource_annotations_coerce_string_gpu_count() -> None:
    plan = _plan_with_node_execution(
        {"require": "gpu"},
        resources={"gpu": {"gpus": "2"}},
    )

    assert dask_resource_annotations_for_node(plan, "stage.HeavyInference") == {
        "resource.gpu": 1,
        "GPU": 2,
    }


def test_dask_resource_annotations_without_require_are_empty() -> None:
    plan = _plan_with_node_execution(
        {"prefer": "gpu", "fallback": "default"},
        resources={"gpu": {"gpus": 1}},
    )

    assert dask_resource_annotations_for_node(plan, "stage.HeavyInference") == {}


def test_dask_resource_annotations_missing_resource_errors() -> None:
    plan = _plan_with_node_execution({"require": "missing"}, resources={})

    with pytest.raises(
        ValueError,
        match=r"node 'stage\.HeavyInference' references unknown resource class 'missing'",
    ):
        dask_resource_annotations_for_node(plan, "stage.HeavyInference")


def test_dask_resource_annotations_without_gpus_use_resource_label() -> None:
    plan = _plan_with_node_execution(
        {"require": "large"},
        resources={"large": {"cpus": 8, "memory": "32GB"}},
    )

    assert dask_resource_annotations_for_node(plan, "stage.HeavyInference") == {
        "resource.large": 1
    }


def test_build_dask_graph_applies_annotations_to_required_tasks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    records: list[dict[str, Any]] = []
    active_annotations: list[dict[str, Any]] = []

    @contextmanager
    def fake_annotate(**kwargs: Any) -> Iterator[None]:
        active_annotations.append(dict(kwargs))
        try:
            yield
        finally:
            active_annotations.pop()

    def fake_delayed(func: Any) -> Any:
        def wrapper(*args: Any, **kwargs: Any) -> dict[str, Any]:
            annotations = active_annotations[-1] if active_annotations else {}
            records.append(dict(annotations))
            return {"func": func, "annotations": dict(annotations)}

        return wrapper

    dask = types.ModuleType("dask")
    cast(Any, dask).annotate = fake_annotate
    cast(Any, dask).delayed = fake_delayed
    monkeypatch.setitem(sys.modules, "dask", dask)

    plan = _plan_with_node_execution(
        {"require": "gpu"},
        resources={"gpu": {"gpus": 1}},
    )
    plan.partitions = [_partition()]

    tasks = build_dask_graph(plan, base_ctx={})

    assert tasks[0]["annotations"] == {"resources": {"resource.gpu": 1, "GPU": 1}}
    assert records == [{"resources": {"resource.gpu": 1, "GPU": 1}}]


def test_build_dask_graph_skips_annotations_without_required_resources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    records: list[dict[str, Any]] = []

    def fake_delayed(func: Any) -> Any:
        def wrapper(*args: Any, **kwargs: Any) -> dict[str, Any]:
            records.append({})
            return {"func": func, "annotations": {}}

        return wrapper

    dask = types.ModuleType("dask")
    cast(Any, dask).delayed = fake_delayed
    monkeypatch.setitem(sys.modules, "dask", dask)

    plan = _plan_with_node_execution(
        {"prefer": "gpu"},
        resources={"gpu": {"gpus": 1}},
    )
    plan.partitions = [_partition()]

    tasks = build_dask_graph(plan, base_ctx={})

    assert tasks[0]["annotations"] == {}
    assert records == [{}]


def test_build_dask_graph_ignores_modifier_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    records: list[dict[str, Any]] = []

    def fake_delayed(func: Any) -> Any:
        def wrapper(*args: Any, **kwargs: Any) -> dict[str, Any]:
            records.append({})
            return {"func": func, "annotations": {}}

        return wrapper

    dask = types.ModuleType("dask")
    cast(Any, dask).delayed = fake_delayed
    monkeypatch.setitem(sys.modules, "dask", dask)

    plan = _plan_with_node_execution(
        {"modifiers": [{"name": "gpu.preload", "params": {}}]},
        resources={},
    )
    plan.partitions = [_partition()]

    tasks = build_dask_graph(plan, base_ctx={})

    assert tasks[0]["annotations"] == {}
    assert records == [{}]


def _plan_with_node_execution(
    node_execution: dict[str, Any],
    *,
    resources: dict[str, dict[str, Any]],
) -> ExecutionPlan:
    plan = ExecutionPlan(
        execution={
            "backend": "dask",
            "strategy": "local",
            "profiles": [],
            "resources": resources,
            "pools": {},
            "config": {},
        }
    )
    plan.add_node(
        ExecutionNode(
            id="stage.HeavyInference",
            graph_node_id="stage.HeavyInference",
            role="transform",
            impl="hep.inference",
            meta={"execution": dict(node_execution)},
        )
    )
    return plan


def _partition() -> ExecutionPartition:
    return ExecutionPartition(
        id="dataset:file:0",
        dataset="dataset",
        file="file.root",
        source="source.Events",
        part="0",
    )
