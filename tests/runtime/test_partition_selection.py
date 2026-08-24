from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from hepflow.api import compile_workflow_file, run_plan_file, run_workflow_file
from hepflow.model.plan import ExecutionPartition
from hepflow.progress import ProgressUpdate
from hepflow.runtime.partitions import select_partitions
from hepflow.utils import read_yaml


def test_select_partitions_none_returns_all() -> None:
    partitions = _partitions(3)

    assert select_partitions(partitions) == partitions


def test_select_partitions_single_number() -> None:
    partitions = _partitions(3)

    assert select_partitions(partitions, numbers=[1]) == [partitions[0]]


def test_select_partitions_list() -> None:
    partitions = _partitions(5)

    assert select_partitions(partitions, numbers=[1, 3, 5]) == [
        partitions[0],
        partitions[2],
        partitions[4],
    ]


def test_select_partitions_preserves_plan_order() -> None:
    partitions = _partitions(5)

    assert select_partitions(partitions, numbers=[5, 1, 3]) == [
        partitions[0],
        partitions[2],
        partitions[4],
    ]


def test_select_partitions_deduplicates_numbers() -> None:
    partitions = _partitions(3)

    assert select_partitions(partitions, numbers=[1, 1, 3]) == [
        partitions[0],
        partitions[2],
    ]


@pytest.mark.parametrize("numbers", [[0], [-1]])
def test_select_partitions_rejects_non_positive_numbers(numbers: list[int]) -> None:
    with pytest.raises(ValueError, match="Partition numbers are 1-based"):
        select_partitions(_partitions(3), numbers=numbers)


def test_select_partitions_rejects_number_larger_than_partition_count() -> None:
    with pytest.raises(
        ValueError,
        match=r"Requested partition 4, but this plan contains 3 partitions\.",
    ):
        select_partitions(_partitions(3), numbers=[4])


def test_select_partitions_rejects_empty_iterable() -> None:
    with pytest.raises(ValueError, match="At least one partition number"):
        select_partitions(_partitions(3), numbers=[])


def test_run_plan_file_partition_selection_summary_progress_and_compile_artifact(
    tmp_path: Path,
    toy_workflow: dict[str, Any],
) -> None:
    workflow_path = _write_partitioned_workflow(tmp_path, toy_workflow)
    build_dir = tmp_path / "build"
    compile_workflow_file(workflow_path, outdir=build_dir, chunk_size=1)
    plan_path = build_dir / "compile" / "plan.yaml"
    compiled_plan = read_yaml(plan_path)
    assert len(compiled_plan["partitions"]) == 5
    sink = _CollectSink()

    result = run_plan_file(
        plan_path,
        outdir=build_dir,
        partition_numbers=[5, 1, 3, 3],
        progress_sinks=[sink],
    )

    assert result.success is True
    assert result.summary["progress"]["counts"]["total"] == 3
    expected_partitions = [
        {
            "number": number,
            "id": compiled_plan["partitions"][number - 1]["id"],
            "dataset": compiled_plan["partitions"][number - 1]["dataset"],
        }
        for number in [1, 3, 5]
    ]
    assert result.summary["partition_selection"] == {
        "requested": [1, 3, 5],
        "total_partitions": 5,
        "selected_partitions": 3,
        "partitions": expected_partitions,
    }
    assert [
        update.snapshot.counts.total
        for update in sink.updates
        if update.event.kind == "phase_started" and update.event.phase == "executing"
    ] == [3]
    summary = read_yaml(build_dir / "run_summary.yaml")
    assert summary["partition_selection"] == result.summary["partition_selection"]
    assert len(read_yaml(plan_path)["partitions"]) == 5


def test_run_workflow_file_forwards_partition_selection(
    tmp_path: Path,
    toy_workflow: dict[str, Any],
) -> None:
    workflow_path = _write_partitioned_workflow(tmp_path, toy_workflow)
    outdir = tmp_path / "one-shot"

    result = run_workflow_file(
        workflow_path,
        outdir=outdir,
        chunk_size=1,
        partition_numbers=[2],
    )

    assert result.success is True
    assert result.summary["partition_selection"]["requested"] == [2]
    assert result.summary["partition_selection"]["selected_partitions"] == 1
    assert len(read_yaml(outdir / "compile" / "plan.yaml")["partitions"]) == 5


class _CollectSink:
    def __init__(self) -> None:
        self.updates: list[ProgressUpdate] = []

    def handle(self, update: ProgressUpdate) -> None:
        self.updates.append(update)


def _write_partitioned_workflow(
    tmp_path: Path,
    toy_workflow: dict[str, Any],
) -> Path:
    workflow = {
        **toy_workflow,
        "data": {
            "datasets": [
                {
                    "name": "toydata",
                    "files": ["toy://events"],
                    "nevents": 5,
                }
            ]
        },
    }
    path = tmp_path / "workflow.yaml"
    path.write_text(yaml.safe_dump(workflow, sort_keys=False), encoding="utf-8")
    return path


def _partitions(count: int) -> list[ExecutionPartition]:
    return [
        ExecutionPartition(
            id=f"events__toy__{index}_{index + 1}",
            dataset="toy",
            file="toy://events",
            source="events",
            part=f"{index}_{index + 1}",
            start=index,
            stop=index + 1,
        )
        for index in range(count)
    ]
