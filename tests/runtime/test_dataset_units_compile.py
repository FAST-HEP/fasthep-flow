from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from tests.runtime.test_per_dataset_run import _write_workflow

from hepflow.api import compile_workflow_file, load_plan_file, run_plan_file
from hepflow.compiler.dataset_units import PublishConflict
from hepflow.utils import read_yaml


def _compile(workflow: Path, out: Path, dataset: str, **kwargs: Any) -> Any:
    return compile_workflow_file(workflow, outdir=out, dataset=dataset, **kwargs)


def test_shared_plan_holds_metadata_only_dataset_records(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    workflow = _write_workflow(tmp_path, toy_workflow)
    out = tmp_path / "build"

    _compile(workflow, out, "alpha", chunk_size=2)

    plan = read_yaml(out / "compile" / "plan.yaml")
    assert plan["partitions"] == []
    assert list(plan["context"]["datasets"]) == ["alpha", "beta"]
    for record in plan["context"]["datasets"].values():
        assert "files" not in record
        assert "nevents" not in record
    assert plan["context"]["datasets"]["beta"]["group"] == "data"
    assert "chunk_sizes" not in plan["context"]
    partitioning = plan["execution"]["partitioning"]
    assert partitioning["per_dataset"] is True
    assert partitioning["manifest"] == "datasets/{dataset}/manifest.yaml"


def test_dataset_unit_holds_the_frozen_inputs(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    workflow = _write_workflow(tmp_path, toy_workflow)
    out = tmp_path / "build"

    _compile(workflow, out, "alpha", chunk_size=2)

    unit = out / "compile" / "datasets" / "alpha"
    manifest = read_yaml(unit / "manifest.yaml")
    assert manifest["dataset"] == "alpha"
    assert manifest["files"] == ["toy://a"]
    assert manifest["chunk_size"] == 2
    normalized = read_yaml(unit / "normalized.yaml")
    datasets = {d["name"]: d for d in normalized["data"]["datasets"]}
    assert datasets["alpha"]["files"] == ["toy://a"]
    assert "files" not in datasets["beta"]
    assert not (out / "compile" / "datasets" / "beta").exists()


def test_two_datasets_compile_into_one_root_with_identical_shared_files(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    workflow = _write_workflow(tmp_path, toy_workflow)
    out = tmp_path / "build"

    _compile(workflow, out, "alpha", chunk_size=2)
    shared = (out / "compile" / "plan.yaml").read_bytes()
    _compile(workflow, out, "beta")

    assert (out / "compile" / "plan.yaml").read_bytes() == shared
    for name, chunk_size in (("alpha", 2), ("beta", 1)):
        manifest = read_yaml(out / "compile" / "datasets" / name / "manifest.yaml")
        assert manifest["chunk_size"] == chunk_size


def test_compiling_the_same_dataset_again_is_a_noop(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    workflow = _write_workflow(tmp_path, toy_workflow)
    out = tmp_path / "build"
    _compile(workflow, out, "alpha", chunk_size=2)
    manifest = out / "compile" / "datasets" / "alpha" / "manifest.yaml"
    inode = manifest.stat().st_ino

    _compile(workflow, out, "alpha", chunk_size=2)

    assert manifest.stat().st_ino == inode


def test_a_different_chunk_size_conflicts_with_the_existing_unit(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    workflow = _write_workflow(tmp_path, toy_workflow)
    out = tmp_path / "build"
    _compile(workflow, out, "alpha", chunk_size=2)

    with pytest.raises(PublishConflict, match=r"manifest\.yaml"):
        _compile(workflow, out, "alpha", chunk_size=3)

    manifest = read_yaml(out / "compile" / "datasets" / "alpha" / "manifest.yaml")
    assert manifest["chunk_size"] == 2


def test_load_plan_assembles_partitions_from_the_manifest(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    workflow = _write_workflow(tmp_path, toy_workflow)
    out = tmp_path / "build"
    _compile(workflow, out, "alpha", chunk_size=2)
    _compile(workflow, out, "beta")

    alpha = load_plan_file(out / "compile" / "plan.yaml", dataset="alpha")
    both = load_plan_file(out / "compile" / "plan.yaml")

    assert [(p.dataset, p.part, p.start, p.stop) for p in alpha.partitions] == [
        ("alpha", "1", 0, 2),
        ("alpha", "2", 2, 4),
    ]
    assert [(p.dataset, p.part) for p in both.partitions] == [
        ("alpha", "1"),
        ("alpha", "2"),
        ("beta", "1"),
        ("beta", "2"),
        ("beta", "3"),
    ]
    assert alpha.context["datasets"]["alpha"]["files"] == ["toy://a"]
    assert alpha.context["datasets"]["beta"].get("files", []) == []


def test_load_plan_rejects_a_dataset_without_a_unit(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    workflow = _write_workflow(tmp_path, toy_workflow)
    out = tmp_path / "build"
    _compile(workflow, out, "alpha", chunk_size=2)

    with pytest.raises(ValueError, match=r"No compiled unit.*'beta'.*alpha"):
        load_plan_file(out / "compile" / "plan.yaml", dataset="beta")


def test_run_plan_runs_one_dataset_from_the_shared_plan(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    workflow = _write_workflow(tmp_path, toy_workflow)
    out = tmp_path / "build"
    _compile(workflow, out, "beta")

    result = run_plan_file(out / "compile" / "plan.yaml", outdir=out, dataset="beta")

    assert result.success is True
    assert result.summary["progress"]["counts"]["total"] == 3


def test_run_plan_checks_chunk_size_against_the_manifest(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    workflow = _write_workflow(tmp_path, toy_workflow)
    out = tmp_path / "build"
    _compile(workflow, out, "alpha", chunk_size=2)

    with pytest.raises(ValueError, match=r"compiled with chunk size 2.*recompile"):
        run_plan_file(
            out / "compile" / "plan.yaml", outdir=out, dataset="alpha", chunk_size=3
        )
