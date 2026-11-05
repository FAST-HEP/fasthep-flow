from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
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


def test_diff_plan_files_missing_file_is_input_not_found(tmp_path: Path) -> None:
    with pytest.raises(api.InputNotFound):
        api.diff_plan_files(tmp_path / "nope.yaml", tmp_path / "nope2.yaml")


def test_diff_plan_files_directory_is_invalid_plan(tmp_path: Path) -> None:
    plan = tmp_path / "plan.yaml"
    plan.write_text("context: {}\n", encoding="utf-8")
    with pytest.raises(api.InvalidPlan):
        api.diff_plan_files(plan, tmp_path)


def test_resolve_plan_input_is_public_and_returns_a_path(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    plan = _compile(tmp_path, "one", toy_workflow)
    build_root = plan.parent.parent

    resolved = api.resolve_plan_input(build_root)

    assert resolved == plan
    assert api.diff_plan_files(resolved, resolved).equal


def test_new_errors_are_exported() -> None:
    for name in (
        "AmbiguousPlans",
        "InputNotFound",
        "NoPlanFound",
        "resolve_plan_input",
    ):
        assert name in api.__all__
        assert hasattr(api, name)
