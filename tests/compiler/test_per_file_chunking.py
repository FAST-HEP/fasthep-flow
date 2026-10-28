from __future__ import annotations

from typing import Any

import pytest

from hepflow.compiler.normalize import normalize_workflow
from hepflow.compiler.plan import build_plan_from_normalized


def _workflow(registry: dict[str, Any], dataset: dict[str, Any]) -> dict[str, Any]:
    return {
        "version": "1.0",
        "registry": registry,
        "data": {"datasets": [{"name": "sample", **dataset}]},
        "sources": {
            "events": {
                "kind": "toy.source",
                "stream_type": "event_stream",
                "branches": ["pt"],
            }
        },
    }


def _ranges(plan: Any) -> list[tuple[str, int | None, int | None]]:
    return [(p.part, p.start, p.stop) for p in plan.partitions]


def test_chunks_follow_each_files_own_event_count(toy_registry: dict[str, Any]) -> None:
    workflow = _workflow(
        toy_registry,
        {"files": ["a.root", "b.root"], "nevents": 17, "file_nevents": [10, 7]},
    )

    _graph, plan = build_plan_from_normalized(
        normalize_workflow(workflow), chunk_size=5
    )

    assert _ranges(plan) == [
        ("0_0", 0, 5),
        ("0_1", 5, 10),
        ("1_0", 0, 5),
        ("1_1", 5, 7),
    ]


def test_chunking_multi_file_dataset_without_file_counts_is_an_error(
    toy_registry: dict[str, Any],
) -> None:
    workflow = _workflow(toy_registry, {"files": ["a.root", "b.root"], "nevents": 17})

    with pytest.raises(ValueError, match="file_nevents"):
        build_plan_from_normalized(normalize_workflow(workflow), chunk_size=5)


def test_single_file_dataset_keeps_using_dataset_nevents(
    toy_registry: dict[str, Any],
) -> None:
    workflow = _workflow(toy_registry, {"files": ["a.root"], "nevents": 12})

    _graph, plan = build_plan_from_normalized(
        normalize_workflow(workflow), chunk_size=5
    )

    assert _ranges(plan) == [("0_0", 0, 5), ("0_1", 5, 10), ("0_2", 10, 12)]


def test_file_nevents_must_match_the_number_of_files(
    toy_registry: dict[str, Any],
) -> None:
    workflow = _workflow(
        toy_registry, {"files": ["a.root", "b.root"], "file_nevents": [10]}
    )

    with pytest.raises(ValueError, match="file_nevents"):
        normalize_workflow(workflow)


def test_unchunked_partitions_use_the_files_own_event_count(
    toy_registry: dict[str, Any],
) -> None:
    workflow = _workflow(
        toy_registry,
        {"files": ["a.root", "b.root"], "nevents": 17, "file_nevents": [10, 7]},
    )

    _graph, plan = build_plan_from_normalized(normalize_workflow(workflow))

    assert _ranges(plan) == [("0_0", 0, 10), ("1_0", 0, 7)]
