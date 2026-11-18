from __future__ import annotations

from typing import Any

import pytest

from hepflow.compiler.normalize import normalize_workflow
from hepflow.compiler.plan import build_plan_from_normalized


def _workflow(
    registry: dict[str, Any],
    *,
    per_dataset: bool | None = True,
    stages: list[dict[str, Any]] | None = None,
    observers: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    workflow: dict[str, Any] = {
        "version": "1.0",
        "registry": registry,
        "data": {
            "datasets": [
                {
                    "name": "alpha",
                    "group": "mc",
                    "files": ["a0.root", "a1.root"],
                    "file_nevents": [10, 7],
                },
                {
                    "name": "beta",
                    "group": "data",
                    "files": ["b0.root"],
                    "file_nevents": [6],
                    "chunk_size": 3,
                },
            ]
        },
        "sources": {
            "events": {
                "kind": "toy.source",
                "stream_type": "event_stream",
                "branches": ["pt"],
            }
        },
    }
    if per_dataset is not None:
        workflow["execution"] = {"partitioning": {"per_dataset": per_dataset}}
    if stages is not None:
        workflow["analysis"] = {"stages": stages}
    if observers is not None:
        workflow["observers"] = observers
    return workflow


def _plan(workflow: dict[str, Any], **kwargs: Any) -> Any:
    return build_plan_from_normalized(normalize_workflow(workflow), **kwargs)[1]


def test_per_dataset_flag_is_kept_in_the_normalized_execution(
    toy_registry: dict[str, Any],
) -> None:
    normalized = normalize_workflow(_workflow(toy_registry))
    plan = build_plan_from_normalized(normalized)[1]

    assert plan.execution["partitioning"]["per_dataset"] is True


def test_per_dataset_flag_must_be_boolean(toy_registry: dict[str, Any]) -> None:
    workflow = _workflow(toy_registry)
    workflow["execution"]["partitioning"]["per_dataset"] = "yes"

    with pytest.raises(ValueError, match="per_dataset"):
        _plan(workflow)


def test_selected_dataset_gets_local_one_based_partition_numbers(
    toy_registry: dict[str, Any],
) -> None:
    plan = _plan(_workflow(toy_registry), dataset="alpha", chunk_size=5)

    assert [(p.dataset, p.part, p.start, p.stop) for p in plan.partitions] == [
        ("alpha", "1", 0, 5),
        ("alpha", "2", 5, 10),
        ("alpha", "3", 0, 5),
        ("alpha", "4", 5, 7),
    ]
    assert [p.id for p in plan.partitions][0] == "events__alpha__1"


def test_other_datasets_stay_as_metadata_stubs(toy_registry: dict[str, Any]) -> None:
    plan = _plan(_workflow(toy_registry), dataset="alpha", chunk_size=5)

    datasets = plan.context["datasets"]
    assert list(datasets) == ["alpha", "beta"]
    assert datasets["alpha"]["files"] == ["a0.root", "a1.root"]
    assert datasets["beta"]["files"] == []
    assert datasets["beta"]["group"] == "data"
    assert datasets["beta"]["file_nevents"] is None


def test_dataset_chunk_size_applies_to_the_selected_dataset(
    toy_registry: dict[str, Any],
) -> None:
    plan = _plan(_workflow(toy_registry), dataset="beta")

    assert [(p.part, p.start, p.stop) for p in plan.partitions] == [
        ("1", 0, 3),
        ("2", 3, 6),
    ]


def test_cli_chunk_size_overrides_the_dataset_value(
    toy_registry: dict[str, Any],
) -> None:
    plan = _plan(_workflow(toy_registry), dataset="beta", chunk_size=6)

    assert [(p.part, p.start, p.stop) for p in plan.partitions] == [("1", 0, 6)]


def test_per_dataset_without_selection_numbers_each_dataset_from_one(
    toy_registry: dict[str, Any],
) -> None:
    plan = _plan(_workflow(toy_registry), chunk_size=5)

    assert [(p.dataset, p.part) for p in plan.partitions] == [
        ("alpha", "1"),
        ("alpha", "2"),
        ("alpha", "3"),
        ("alpha", "4"),
        ("beta", "1"),
        ("beta", "2"),
    ]


def test_dataset_requires_per_dataset_mode(toy_registry: dict[str, Any]) -> None:
    with pytest.raises(ValueError, match="not per-dataset"):
        _plan(_workflow(toy_registry, per_dataset=None), dataset="alpha")


def test_per_dataset_argument_enables_the_mode(toy_registry: dict[str, Any]) -> None:
    plan = _plan(
        _workflow(toy_registry, per_dataset=None),
        dataset="alpha",
        per_dataset=True,
        chunk_size=5,
    )

    assert plan.partitions[0].part == "1"
    assert plan.execution["partitioning"]["per_dataset"] is True


def test_per_dataset_argument_overrides_the_workflow_value(
    toy_registry: dict[str, Any],
) -> None:
    plan = _plan(_workflow(toy_registry, per_dataset=True), per_dataset=False)

    assert plan.partitions[0].part == "0_0"
    assert not plan.execution["partitioning"].get("per_dataset")


def test_unknown_dataset_lists_the_available_names(
    toy_registry: dict[str, Any],
) -> None:
    with pytest.raises(ValueError, match="Unknown dataset 'gamma'.*alpha, beta"):
        _plan(_workflow(toy_registry), dataset="gamma")


def test_old_mode_numbering_is_unchanged(toy_registry: dict[str, Any]) -> None:
    plan = _plan(_workflow(toy_registry, per_dataset=None), chunk_size=5)

    assert [(p.dataset, p.part) for p in plan.partitions][:2] == [
        ("alpha", "0_0"),
        ("alpha", "0_1"),
    ]


def test_global_observer_is_rejected_in_per_dataset_mode(
    toy_registry: dict[str, Any],
) -> None:
    workflow = _workflow(
        toy_registry,
        stages=[{"id": "Scale", "op": "toy.scale", "params": {"factor": 2}}],
        observers=[{"kind": "toy.observe", "at": ["read.events"]}],
    )

    with pytest.raises(ValueError, match="per-dataset.*aggregation"):
        _plan(workflow, dataset="alpha")


def test_run_end_sink_is_rejected_in_per_dataset_mode(
    toy_registry: dict[str, Any],
) -> None:
    workflow = _workflow(
        toy_registry,
        stages=[
            {
                "id": "Scale",
                "op": "toy.scale",
                "params": {"factor": 2},
                "write": [
                    {"kind": "toy.write", "path": "out.json", "when": "run_end"}
                ],
            }
        ],
    )

    with pytest.raises(ValueError, match="per-dataset.*run_end"):
        _plan(workflow, dataset="alpha")


def test_partition_end_writer_is_accepted_in_per_dataset_mode(
    toy_registry: dict[str, Any],
) -> None:
    workflow = _workflow(
        toy_registry,
        stages=[
            {
                "id": "Scale",
                "op": "toy.scale",
                "params": {"factor": 2},
                "write": [
                    {"kind": "toy.write", "path": "out.json", "when": "partition_end"}
                ],
            }
        ],
    )

    plan = _plan(workflow, dataset="alpha", chunk_size=5)

    assert len(plan.partitions) == 4
