from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

import hepflow
from hepflow.api import compile_workflow_file, normalise_workflow_file
from hepflow.compiler.bundle import plan_hash
from hepflow.provenance.compilation_info import compilation_provenance
from hepflow.utils import read_yaml


def _entry(project: Path, toy_workflow: dict[str, Any]) -> Path:
    entry = project / "entry" / "workflow.yaml"
    entry.parent.mkdir(parents=True, exist_ok=True)
    entry.write_text(yaml.safe_dump(toy_workflow, sort_keys=False), encoding="utf-8")
    return entry


def test_compilation_provenance_reports_installed_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(hepflow, "__version__", "1.2.3")

    assert compilation_provenance() == {"flow_version": "1.2.3"}


def test_normalised_workflow_records_flow_version(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    project = tmp_path / "proj"
    entry = _entry(project, toy_workflow)

    normalized = normalise_workflow_file(entry, outdir=tmp_path / "build", root=project)

    assert normalized["provenance"]["compilation"] == {
        "flow_version": hepflow.__version__
    }


def test_unknown_version_does_not_fail_compile(
    tmp_path: Path, toy_workflow: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(hepflow, "__version__", "0+unknown")
    project = tmp_path / "proj"
    entry = _entry(project, toy_workflow)

    normalized = normalise_workflow_file(entry, outdir=tmp_path / "build", root=project)

    assert normalized["provenance"]["compilation"] == {"flow_version": "0+unknown"}


def test_compiled_plan_yaml_records_flow_version(
    tmp_path: Path, toy_workflow_path: Path
) -> None:
    build = tmp_path / "build"

    compile_workflow_file(toy_workflow_path, outdir=build)

    plan = read_yaml(build / "compile" / "plan.yaml")
    assert plan["provenance"]["compilation"]["flow_version"] == hepflow.__version__


def test_plan_hash_changes_with_flow_version(
    tmp_path: Path, toy_workflow_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    hashes = []
    for version in ("1", "2"):
        monkeypatch.setattr(hepflow, "__version__", version)
        build = tmp_path / f"build-{version}"
        compile_workflow_file(toy_workflow_path, outdir=build)
        hashes.append(
            plan_hash(build / "compile" / "plan.yaml", str(tmp_path))["sha256"]
        )

    assert hashes[0] != hashes[1]
