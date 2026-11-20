from __future__ import annotations

import copy
import multiprocessing
import shutil
from pathlib import Path
from typing import Any

import pytest
from tests.runtime.test_per_dataset_run import _write_workflow

from hepflow.api import (
    compile_workflow_file,
    diff_plan_files,
    load_plan_file,
    run_plan_file,
)
from hepflow.build_layout import BuildPaths
from hepflow.compiler.dataset_units import PublishConflict, unit_dir
from hepflow.runtime.provenance.store import _workflow_references
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


def test_compile_without_a_dataset_writes_a_unit_for_each_dataset(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    workflow = _write_workflow(tmp_path, toy_workflow)
    out = tmp_path / "build"

    compile_workflow_file(workflow, outdir=out, chunk_size=2)

    for name in ("alpha", "beta"):
        assert (out / "compile" / "datasets" / name / "manifest.yaml").is_file()
    assert not (out / "compile" / "normalized.yaml").exists()


def _compile_in_process(args: tuple[str, str, str, int | None]) -> str:
    workflow, out, dataset, chunk_size = args
    compile_workflow_file(
        workflow, outdir=out, dataset=dataset, chunk_size=chunk_size
    )
    return dataset


def test_concurrent_compiles_publish_identical_shared_files(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    workflow = _write_workflow(tmp_path, toy_workflow)
    out = tmp_path / "build"
    jobs = [(str(workflow), str(out), name, None) for name in ["alpha", "beta"] * 3]

    with multiprocessing.get_context("fork").Pool(4) as pool:
        done = pool.map(_compile_in_process, jobs)

    assert sorted(done) == sorted(name for _, _, name, _ in jobs)
    plan = load_plan_file(out / "compile" / "plan.yaml")
    assert {p.dataset for p in plan.partitions} == {"alpha", "beta"}
    assert not list((out / "compile").glob(".*"))


def test_old_global_mode_still_writes_one_plan_with_all_partitions(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    workflow = _write_workflow(tmp_path, toy_workflow, per_dataset=False)
    out = tmp_path / "build"

    compile_workflow_file(workflow, outdir=out, chunk_size=2)

    plan = read_yaml(out / "compile" / "plan.yaml")
    assert len(plan["partitions"]) > 0
    assert (out / "compile" / "normalized.yaml").is_file()
    assert not (out / "compile" / "datasets").exists()


def test_variations_get_their_own_shared_plan_and_units(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    toy_workflow["systematics"] = {
        "include_nominal": True,
        "variations": [
            {
                "name": "scale_up",
                "group": "scale",
                "direction": "up",
                "applies_to": "mc",
                "weight": {"multiply": "w_up"},
            }
        ],
    }
    workflow = _write_workflow(tmp_path, toy_workflow)
    out = tmp_path / "build"

    _compile(workflow, out, "alpha", chunk_size=2)
    _compile(workflow, out, "beta")

    for variation in ("nominal", "scale_up"):
        root = out / "compile" / variation
        assert read_yaml(root / "plan.yaml")["partitions"] == []
        for name in ("alpha", "beta"):
            assert (root / "datasets" / name / "manifest.yaml").is_file()
    plan = load_plan_file(out / "compile" / "scale_up" / "plan.yaml", dataset="alpha")
    assert [p.part for p in plan.partitions] == ["1", "2"]


def test_run_plan_ignores_a_damaged_sibling_unit(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    workflow = _write_workflow(tmp_path, toy_workflow)
    out = tmp_path / "build"
    _compile(workflow, out, "alpha", chunk_size=2)
    _compile(workflow, out, "beta")
    beta = out / "compile" / "datasets" / "beta" / "manifest.yaml"
    beta.write_text(beta.read_text().replace("file_nevents", "file_nevents_x"))

    result = run_plan_file(out / "compile" / "plan.yaml", outdir=out, dataset="alpha")

    assert result.success is True


def test_shared_plan_names_no_location_of_the_workflow(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    workflow = _write_workflow(tmp_path, toy_workflow)
    out = tmp_path / "build"
    _compile(workflow, out, "alpha", chunk_size=2)

    text = (out / "compile" / "plan.yaml").read_text(encoding="utf-8")
    plan = read_yaml(out / "compile" / "plan.yaml")

    assert str(tmp_path) not in text
    assert str(Path.cwd()) not in text
    assert "workflow_path" not in plan["context"]
    assert "project_root" not in plan["context"]
    assert "root" not in plan["provenance"]
    assert "inputs" not in plan["provenance"]
    assert len(plan["context"]["workflow_hash"]) == 64


def test_compiles_from_another_path_form_publish_the_same_shared_files(
    tmp_path: Path,
    toy_workflow: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workflow = _write_workflow(tmp_path, toy_workflow)
    out = tmp_path / "build"
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()

    _compile(workflow.resolve(), out, "alpha", chunk_size=2)
    shared = {
        path.name: path.read_bytes()
        for path in (out / "compile").iterdir()
        if path.is_file()
    }
    monkeypatch.chdir(elsewhere)
    _compile(Path("..") / workflow.name, out, "beta")

    for name, content in shared.items():
        assert (out / "compile" / name).read_bytes() == content, name
    unit = read_yaml(out / "compile" / "datasets" / "beta" / "invocation.yaml")
    assert unit["workflow_path"].endswith("workflow.yaml")


def test_a_changed_workflow_conflicts_in_the_same_root(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    workflow = _write_workflow(tmp_path, toy_workflow)
    out = tmp_path / "build"
    _compile(workflow, out, "alpha", chunk_size=2)
    changed = copy.deepcopy(toy_workflow)
    changed["analysis"]["stages"][0]["params"]["factor"] = 3
    workflow = _write_workflow(tmp_path, changed)

    with pytest.raises(PublishConflict):
        _compile(workflow, out, "beta")


def test_load_plan_restores_the_compile_location_from_the_unit(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    workflow = _write_workflow(tmp_path, toy_workflow)
    out = tmp_path / "build"
    _compile(workflow, out, "alpha", chunk_size=2)

    one = load_plan_file(out / "compile" / "plan.yaml", dataset="alpha")
    none = load_plan_file(out / "compile" / "plan.yaml")

    assert one.context["workflow_path"] == str(workflow)
    assert "project_root" in one.context
    assert "workflow_path" not in none.context


def _unit(out: Path, name: str) -> Path:
    return out / "compile" / "datasets" / name


def test_unit_is_published_by_rename_and_names_no_staging_path(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    workflow = _write_workflow(tmp_path, toy_workflow)
    out = tmp_path / "build"
    _compile(workflow, out, "alpha", chunk_size=2)

    assert sorted(p.name for p in (out / "compile" / "datasets").iterdir()) == ["alpha"]
    for path in _unit(out, "alpha").rglob("*"):
        if path.is_file():
            assert ".tmp-" not in path.read_text(encoding="utf-8", errors="ignore")


def test_an_interrupted_unit_is_redone_without_touching_other_units(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    workflow = _write_workflow(tmp_path, toy_workflow)
    out = tmp_path / "build"
    _compile(workflow, out, "alpha", chunk_size=2)
    _compile(workflow, out, "beta")
    beta_manifest = (_unit(out, "beta") / "manifest.yaml").read_bytes()
    # An older, crashed compile left a unit directory without its manifest.
    (_unit(out, "alpha") / "manifest.yaml").unlink()
    (_unit(out, "alpha") / "stale.txt").write_text("left over", encoding="utf-8")

    _compile(workflow, out, "alpha", chunk_size=2)

    assert (_unit(out, "alpha") / "manifest.yaml").is_file()
    assert not (_unit(out, "alpha") / "stale.txt").exists()
    assert (_unit(out, "beta") / "manifest.yaml").read_bytes() == beta_manifest


def test_a_stale_staging_directory_does_not_hide_or_break_a_unit(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    workflow = _write_workflow(tmp_path, toy_workflow)
    out = tmp_path / "build"
    stale = out / "compile" / "datasets" / ".alpha.tmp-dead"
    stale.mkdir(parents=True)
    (stale / "normalized.yaml").write_text("half", encoding="utf-8")

    _compile(workflow, out, "alpha", chunk_size=2)

    plan = load_plan_file(out / "compile" / "plan.yaml")
    assert [p.dataset for p in plan.partitions] == ["alpha", "alpha"]


def test_a_bad_dataset_name_fails_before_anything_is_published(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    with pytest.raises(ValueError, match="cannot name a unit directory"):
        unit_dir(tmp_path, "a/b")
    with pytest.raises(ValueError, match="cannot name a unit directory"):
        unit_dir(tmp_path, "..")


def test_run_plan_without_any_unit_names_the_missing_manifest(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    workflow = _write_workflow(tmp_path, toy_workflow)
    out = tmp_path / "build"
    _compile(workflow, out, "alpha", chunk_size=2)
    shutil.rmtree(out / "compile" / "datasets")

    with pytest.raises(ValueError, match=r"No compiled dataset unit.*compile --dataset"):
        run_plan_file(out / "compile" / "plan.yaml", outdir=out)


def test_run_summary_carries_the_bundle_id_of_the_unit(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    workflow = _write_workflow(tmp_path, toy_workflow)
    out = tmp_path / "build"
    _compile(workflow, out, "alpha", chunk_size=2)
    bundle_id = read_yaml(_unit(out, "alpha") / "inputs.manifest.yaml")["bundle_id"]

    result = run_plan_file(out / "compile" / "plan.yaml", outdir=out, dataset="alpha")

    assert result.summary["bundle_id"] == bundle_id


def test_provenance_points_at_the_normalized_workflow_of_the_unit(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    workflow = _write_workflow(tmp_path, toy_workflow)
    out = tmp_path / "build"
    _compile(workflow, out, "alpha", chunk_size=2)

    plan = load_plan_file(out / "compile" / "plan.yaml", dataset="alpha")
    ref = _workflow_references(BuildPaths.from_plan(plan, outdir=out), plan)["normalized"]

    assert ref == "compile/datasets/alpha/normalized.yaml"
    assert (out / ref).is_file()


def test_diff_of_two_shared_plans_still_works(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    workflow = _write_workflow(tmp_path, toy_workflow)
    _compile(workflow, tmp_path / "one", "alpha", chunk_size=2)
    _compile(workflow, tmp_path / "two", "beta")
    changed = copy.deepcopy(toy_workflow)
    changed["analysis"]["stages"][0]["params"]["factor"] = 3
    (tmp_path / "other").mkdir()
    changed_file = _write_workflow(tmp_path / "other", changed)
    _compile(changed_file, tmp_path / "three", "alpha", chunk_size=2)

    same = diff_plan_files(
        tmp_path / "one" / "compile" / "plan.yaml",
        tmp_path / "two" / "compile" / "plan.yaml",
    )
    different = diff_plan_files(
        tmp_path / "one" / "compile" / "plan.yaml",
        tmp_path / "three" / "compile" / "plan.yaml",
    )

    assert same.equal is True
    assert different.equal is False
