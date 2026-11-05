from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from hepflow import api
from hepflow.compiler.plan_diff.model import DiffSection


def _compile(tmp_path: Path, name: str, workflow: dict[str, Any]) -> Path:
    directory = tmp_path / name
    directory.mkdir()
    path = directory / "wf.yaml"
    path.write_text(yaml.safe_dump(workflow, sort_keys=False), encoding="utf-8")
    api.compile_workflow_file(path, outdir=directory / "build", root=directory)
    return directory / "build" / "compile" / "plan.yaml"


def test_diff_plan_files_returns_report(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    old = _compile(tmp_path, "old", toy_workflow)
    new = _compile(tmp_path, "new", toy_workflow)

    report = api.diff_plan_files(old, new)

    assert isinstance(report, api.PlanDiffReport)
    assert report.semantically_equal
    assert {e.section for e in report.entries} == {DiffSection.PROVENANCE}
    assert report.to_dict()["classifier_version"] >= 1


def test_public_names_are_exported_from_hepflow_api() -> None:
    for name in (
        "DiffKind",
        "DiffSection",
        "EmptyScopeError",
        "InvalidPlan",
        "PlanDiffEntry",
        "PlanDiffError",
        "PlanDiffReport",
        "PlanDiffScope",
        "PlanDiffWarning",
        "diff_plan_files",
    ):
        assert name in api.__all__
        assert hasattr(api, name)


def test_internal_registries_are_not_public() -> None:
    for name in ("classify", "classifier_table", "identity_spec", "validate_plan"):
        assert not hasattr(api, name)
        assert name not in api.__all__
