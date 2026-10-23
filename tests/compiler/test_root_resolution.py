from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from hepflow.api import compile_workflow_file, normalise_workflow_file


def _write_profile(path: Path, marker: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump({"execution": {"config": {marker: True}}}), encoding="utf-8"
    )


def _entry(project: Path, toy_workflow: dict[str, Any], profiles: list[str]) -> Path:
    entry = project / "entry" / "workflow.yaml"
    entry.parent.mkdir(parents=True, exist_ok=True)
    workflow = {**toy_workflow, "use": {"profiles": profiles}}
    entry.write_text(yaml.safe_dump(workflow, sort_keys=False), encoding="utf-8")
    return entry


def test_profile_path_resolves_against_root(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    project = tmp_path / "proj"
    _write_profile(project / "profiles" / "x.yaml", "from_root")
    entry = _entry(project, toy_workflow, ["profiles/x.yaml"])

    normalized = normalise_workflow_file(entry, outdir=tmp_path / "build", root=project)

    assert normalized["execution"]["config"]["from_root"] is True


def test_profile_path_falls_back_to_entry_dir_with_deprecation_warning(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    project = tmp_path / "proj"
    _write_profile(project / "entry" / "shared" / "x.yaml", "from_entry")
    entry = _entry(project, toy_workflow, ["shared/x.yaml"])

    with pytest.warns(FutureWarning, match="entry"):
        normalized = normalise_workflow_file(
            entry, outdir=tmp_path / "build", root=project
        )

    assert normalized["execution"]["config"]["from_entry"] is True


def test_named_local_profile_falls_back_to_entry_dir_with_deprecation_warning(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    project = tmp_path / "proj"
    _write_profile(
        project / "entry" / ".hepflow" / "profiles" / "site.yaml", "from_named"
    )
    entry = _entry(project, toy_workflow, ["site"])

    with pytest.warns(FutureWarning, match="entry"):
        normalized = normalise_workflow_file(
            entry, outdir=tmp_path / "build", root=project
        )

    assert normalized["execution"]["config"]["from_named"] is True


def test_root_defaults_to_cwd(
    tmp_path: Path, toy_workflow: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    project = tmp_path / "proj"
    _write_profile(project / "profiles" / "x.yaml", "from_cwd")
    entry = _entry(project, toy_workflow, ["profiles/x.yaml"])
    monkeypatch.chdir(project)

    normalized = normalise_workflow_file(entry, outdir=tmp_path / "build")

    assert normalized["execution"]["config"]["from_cwd"] is True
    assert normalized["provenance"]["root"] == str(project)


def test_missing_root_raises_file_not_found_error(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    entry = _entry(tmp_path / "proj", toy_workflow, [])

    with pytest.raises(FileNotFoundError, match="root"):
        normalise_workflow_file(
            entry, outdir=tmp_path / "build", root=tmp_path / "nope"
        )


def test_root_that_is_a_file_raises_file_not_found_error(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    entry = _entry(tmp_path / "proj", toy_workflow, [])

    with pytest.raises(FileNotFoundError, match="root"):
        normalise_workflow_file(entry, outdir=tmp_path / "build", root=entry)


def test_root_is_recorded_in_provenance_and_plan_context(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    project = tmp_path / "proj"
    entry = _entry(project, toy_workflow, [])

    plan = compile_workflow_file(entry, outdir=tmp_path / "build", root=project)

    assert plan.context["project_root"] == str(project)
    normalized = yaml.safe_load(
        (tmp_path / "build" / "compile" / "normalized.yaml").read_text(encoding="utf-8")
    )
    assert normalized["provenance"]["root"] == str(project)
    assert normalized["project_root"] == str(project)
