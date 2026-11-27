from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml
from tests.builders import modified, modified_stage

from hepflow.api import (
    compile_workflow_file,
    load_plan_file,
    run_plan_file,
    run_workflow_file,
)
from hepflow.utils import write_yaml


def _write_workflow(
    tmp_path: Path, toy_workflow: dict[str, Any], *, per_dataset: bool = True
) -> Path:
    workflow = modified(
        modified_stage(toy_workflow, write=None),
        data={
            "datasets": [
                {"name": "alpha", "files": ["toy://a"], "nevents": 4, "group": "mc"},
                {
                    "name": "beta",
                    "files": ["toy://b"],
                    "nevents": 3,
                    "group": "data",
                    "chunk_size": 1,
                },
            ]
        },
        execution={"partitioning": {"per_dataset": True}} if per_dataset else None,
    )
    path = tmp_path / "workflow.yaml"
    write_yaml(workflow, path)
    return path


def test_run_workflow_dataset_alone_runs_all_its_partitions(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    workflow = _write_workflow(tmp_path, toy_workflow)

    result = run_workflow_file(
        workflow, outdir=tmp_path / "out", dataset="beta", chunk_size=None
    )

    assert result.success is True
    plan = load_plan_file(tmp_path / "out" / "compile" / "plan.yaml", dataset="beta")
    assert [p.part for p in plan.partitions] == ["1", "2", "3"]
    assert result.summary["progress"]["counts"]["total"] == 3


def test_run_workflow_dataset_partition_numbers_are_local(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    workflow = _write_workflow(tmp_path, toy_workflow)

    result = run_workflow_file(
        workflow,
        outdir=tmp_path / "out",
        dataset="beta",
        partition_numbers=[3],
    )

    selection = result.summary["partition_selection"]
    assert selection["total_partitions"] == 3
    assert [p["number"] for p in selection["partitions"]] == [3]
    assert selection["partitions"][0]["dataset"] == "beta"


def test_per_dataset_override_enables_the_mode(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    workflow = _write_workflow(tmp_path, toy_workflow, per_dataset=False)

    with pytest.raises(ValueError, match="not per-dataset"):
        run_workflow_file(workflow, outdir=tmp_path / "a", dataset="beta")

    result = run_workflow_file(
        workflow, outdir=tmp_path / "b", dataset="beta", per_dataset=True
    )
    assert result.success is True


def test_global_partition_on_multi_dataset_per_dataset_workflow_is_ambiguous(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    workflow = _write_workflow(tmp_path, toy_workflow)

    with pytest.raises(ValueError, match=r"ambiguous.*--dataset"):
        run_workflow_file(workflow, outdir=tmp_path / "out", partition_numbers=[2])


def test_partition_on_single_dataset_per_dataset_workflow_is_allowed(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    path = _write_workflow(tmp_path, toy_workflow)
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    doc["data"]["datasets"] = [doc["data"]["datasets"][1]]
    write_yaml(doc, path)

    result = run_workflow_file(path, outdir=tmp_path / "out", partition_numbers=[2])

    assert result.summary["partition_selection"]["requested"] == [2]


def test_out_of_range_partition_names_dataset_and_chunk_size(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    workflow = _write_workflow(tmp_path, toy_workflow)

    with pytest.raises(
        ValueError,
        match=r"Requested partition 9.*dataset 'beta'.*chunk size 1.*1\.\.3",
    ):
        run_workflow_file(
            workflow,
            outdir=tmp_path / "out",
            dataset="beta",
            partition_numbers=[9],
        )


def test_run_plan_selects_dataset_with_local_numbers(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    workflow = _write_workflow(tmp_path, toy_workflow)
    out = tmp_path / "build"
    compile_workflow_file(workflow, outdir=out, chunk_size=2)
    plan = out / "compile" / "plan.yaml"

    result = run_plan_file(
        plan, outdir=out, dataset="alpha", partition_numbers=[2], chunk_size=2
    )

    selection = result.summary["partition_selection"]
    assert selection["total_partitions"] == 2
    assert selection["partitions"][0]["dataset"] == "alpha"


def test_run_plan_rejects_other_chunk_size(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    workflow = _write_workflow(tmp_path, toy_workflow)
    out = tmp_path / "build"
    compile_workflow_file(workflow, outdir=out, chunk_size=2)

    with pytest.raises(ValueError, match=r"compiled with chunk size 2.*recompile"):
        run_plan_file(
            out / "compile" / "plan.yaml",
            outdir=out,
            dataset="alpha",
            chunk_size=3,
        )


def test_run_plan_dataset_requires_per_dataset_plan(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    workflow = _write_workflow(tmp_path, toy_workflow, per_dataset=False)
    out = tmp_path / "build"
    compile_workflow_file(workflow, outdir=out)

    with pytest.raises(ValueError, match="not per-dataset"):
        run_plan_file(out / "compile" / "plan.yaml", outdir=out, dataset="alpha")


def test_per_dataset_run_leaves_final_product_manifests_alone(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    workflow = _write_workflow(tmp_path, toy_workflow)
    out = tmp_path / "out"
    manifest = out / "artifacts" / "histograms" / "manifest.json"
    manifest.parent.mkdir(parents=True)
    manifest.write_text("{}", encoding="utf-8")

    run_workflow_file(workflow, outdir=out, dataset="beta")

    assert manifest.is_file()


def test_old_mode_run_still_resets_final_product_manifests(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    workflow = _write_workflow(tmp_path, toy_workflow, per_dataset=False)
    out = tmp_path / "out"
    manifest = out / "artifacts" / "histograms" / "manifest.json"
    manifest.parent.mkdir(parents=True)
    manifest.write_text("{}", encoding="utf-8")

    run_workflow_file(workflow, outdir=out)

    assert not manifest.exists()


def test_invalid_dataset_selection_writes_nothing(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    workflow = _write_workflow(tmp_path, toy_workflow, per_dataset=False)
    out = tmp_path / "out"

    with pytest.raises(ValueError, match="not per-dataset"):
        compile_workflow_file(workflow, outdir=out, dataset="alpha")

    assert not out.exists()
