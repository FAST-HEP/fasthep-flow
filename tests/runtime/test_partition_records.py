from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml
from tests.runtime.test_per_dataset_run import _write_workflow

from hepflow.api import compile_workflow_file, run_plan_file
from hepflow.backends.local import LocalBackend
from hepflow.compiler.dataset_units import PublishConflict


def _compile(tmp_path: Path, toy_workflow: dict[str, Any], *datasets: str) -> tuple[Path, Path]:
    workflow = _write_workflow(tmp_path, toy_workflow)
    out = tmp_path / "out"
    for name in datasets:
        compile_workflow_file(workflow, outdir=out, dataset=name)
    return out, out / "compile" / "plan.yaml"


def _summary(out: Path, dataset: str, number: int) -> dict[str, Any]:
    path = out / "runs" / dataset / str(number) / "run_summary.yaml"
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def test_every_partition_gets_its_own_summary_and_the_run_root_a_binding(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    out, plan = _compile(tmp_path, toy_workflow, "beta")

    run_plan_file(plan, outdir=out, dataset="beta")

    root = out / "runs" / "beta"
    assert sorted(p.name for p in root.iterdir()) == ["1", "2", "3", "binding.json"]
    for number in (1, 2, 3):
        summary = _summary(out, "beta", number)
        assert (summary["dataset"], summary["partition"], summary["partitions"]) == (
            "beta",
            number,
            3,
        )
        assert summary["chunk_size"] == 1
        assert summary["success"] is True
        assert len(summary["plan_hash"]) == 64
        assert len(summary["manifest_hash"]) == 64
        assert (root / str(number) / "attempts" / summary["attempt"]).is_dir()


def test_selected_partitions_only_leave_their_own_records(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    out, plan = _compile(tmp_path, toy_workflow, "beta")

    run_plan_file(plan, outdir=out, dataset="beta", partition_numbers=[2])

    assert sorted(p.name for p in (out / "runs" / "beta").iterdir()) == ["2", "binding.json"]


def test_without_a_dataset_each_dataset_gets_a_run_root(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    out, plan = _compile(tmp_path, toy_workflow, "alpha", "beta")

    run_plan_file(plan, outdir=out)

    assert sorted(p.name for p in (out / "runs").iterdir()) == ["alpha", "beta"]
    assert (out / "runs" / "alpha" / "binding.json").is_file()
    assert _summary(out, "alpha", 1)["partitions"] == 1


def test_a_rerun_replaces_the_summary_and_keeps_the_earlier_attempt(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    out, plan = _compile(tmp_path, toy_workflow, "beta")
    run_plan_file(plan, outdir=out, dataset="beta", partition_numbers=[1])
    first = _summary(out, "beta", 1)["attempt"]

    run_plan_file(plan, outdir=out, dataset="beta", partition_numbers=[1])

    second = _summary(out, "beta", 1)["attempt"]
    assert second != first
    attempts = sorted(p.name for p in (out / "runs" / "beta" / "1" / "attempts").iterdir())
    assert attempts == sorted([first, second])


def test_a_changed_plan_fails_before_any_partition_runs(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    out, plan = _compile(tmp_path, toy_workflow, "beta")
    run_plan_file(plan, outdir=out, dataset="beta", partition_numbers=[1])
    doc = yaml.safe_load(plan.read_text(encoding="utf-8"))
    doc["marker"] = "changed"  # same manifest, different plan
    plan.write_text(yaml.safe_dump(doc, sort_keys=False), encoding="utf-8")

    with pytest.raises(PublishConflict, match="plan_hash"):
        run_plan_file(plan, outdir=out, dataset="beta", partition_numbers=[2])

    assert sorted(p.name for p in (out / "runs" / "beta").iterdir()) == ["1", "binding.json"]


def test_a_failed_partition_leaves_no_summary_and_stops_the_run(
    tmp_path: Path, toy_workflow: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    out, plan = _compile(tmp_path, toy_workflow, "beta")
    real = LocalBackend.run

    def run(self: LocalBackend, plan: Any, **kwargs: Any) -> Any:
        if plan.partitions[0].part == "2":
            raise RuntimeError("boom")
        return real(self, plan, **kwargs)

    monkeypatch.setattr(LocalBackend, "run", run)

    with pytest.raises(RuntimeError, match="boom"):
        run_plan_file(plan, outdir=out, dataset="beta")

    root = out / "runs" / "beta"
    assert (root / "1" / "run_summary.yaml").is_file()
    assert not (root / "2" / "run_summary.yaml").exists()
    assert not (root / "3").exists()


def test_a_partition_run_does_not_delete_the_product_manifests_of_other_jobs(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    out, plan = _compile(tmp_path, toy_workflow, "beta")
    other = out / "artifacts" / "histograms" / "manifest.json"
    other.parent.mkdir(parents=True, exist_ok=True)
    other.write_text("{}", encoding="utf-8")

    run_plan_file(plan, outdir=out, dataset="beta", partition_numbers=[1])

    assert other.is_file()


def _compile_with_writer(tmp_path: Path, toy_workflow: dict[str, Any]) -> tuple[Path, Path]:
    workflow = _write_workflow(tmp_path, toy_workflow)
    doc = yaml.safe_load(workflow.read_text(encoding="utf-8"))
    doc["analysis"]["stages"][0]["write"] = [
        {"kind": "toy.write_manifest", "path": "skims/part.toy"}
    ]
    workflow.write_text(yaml.safe_dump(doc, sort_keys=False), encoding="utf-8")
    out = tmp_path / "out"
    compile_workflow_file(workflow, outdir=out, dataset="beta")
    return out, out / "compile" / "plan.yaml"


def test_partition_files_are_named_by_n_and_leave_no_temporary_files(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    out, plan = _compile_with_writer(tmp_path, toy_workflow)

    run_plan_file(plan, outdir=out, dataset="beta")

    files = out / "artifacts" / "files" / "skims" / "part" / "beta"
    assert sorted(p.name for p in files.iterdir()) == ["1.toy", "2.toy", "3.toy"]
    assert not [p for p in out.rglob("*") if ".tmp-" in p.name and "attempts" not in p.parts]


def test_a_failed_writer_leaves_no_partial_file_and_a_rerun_replaces_the_file(
    tmp_path: Path, toy_workflow: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    from tests.toy_components import sinks  # noqa: PLC0415

    out, plan = _compile_with_writer(tmp_path, toy_workflow)
    real = sinks.run_toy_write_manifest

    def failing(**kwargs: Any) -> Any:
        real(**kwargs)
        raise RuntimeError("writer died after writing")

    monkeypatch.setattr(sinks, "run_toy_write_manifest", failing)
    with pytest.raises(RuntimeError, match="writer died"):
        run_plan_file(plan, outdir=out, dataset="beta", partition_numbers=[1])
    files = out / "artifacts" / "files" / "skims" / "part" / "beta"
    assert not files.exists() or list(files.iterdir()) == []

    monkeypatch.setattr(sinks, "run_toy_write_manifest", real)
    run_plan_file(plan, outdir=out, dataset="beta", partition_numbers=[1])
    run_plan_file(plan, outdir=out, dataset="beta", partition_numbers=[1])
    assert [p.name for p in files.iterdir()] == ["1.toy"]


def test_records_of_a_partition_are_written_below_its_run_directory(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    out, plan = _compile_with_writer(tmp_path, toy_workflow)

    run_plan_file(plan, outdir=out, dataset="beta", partition_numbers=[2])

    run_dir = out / "runs" / "beta" / "2"
    manifest = yaml.safe_load((run_dir / "writers" / "part" / "manifest.json").read_text())
    assert [entry["path"] for entry in manifest["datasets"]["beta"]["files"]] == [
        "artifacts/files/skims/part/beta/2.toy"
    ]
    assert (run_dir / "provenance" / "manifest.json").is_file()
    assert not (out / "artifacts" / "files" / "part" / "manifest.json").exists()
    assert not [p for p in (out / "artifacts" / "provenance").rglob("*") if p.is_file()]
