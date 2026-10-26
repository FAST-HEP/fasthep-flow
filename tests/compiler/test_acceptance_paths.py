from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from hepflow.api import compile_workflow_file


def _project(tmp_path: Path, toy_workflow: dict[str, Any]) -> Path:
    project = tmp_path / "proj"
    (project / "data").mkdir(parents=True)
    (project / "data" / "x.json").write_text('{"x": 1}', encoding="utf-8")
    workflow = {
        **toy_workflow,
        "execution": {
            "resources": {"kfactor": {"kind": "file", "path": "data/x.json"}},
        },
    }
    entry = project / "entry" / "workflow.yaml"
    entry.parent.mkdir()
    entry.write_text(yaml.safe_dump(workflow, sort_keys=False), encoding="utf-8")
    return entry


def test_compile_from_another_cwd_with_root_gives_same_plan_paths(
    tmp_path: Path, toy_workflow: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    entry = _project(tmp_path, toy_workflow)
    project = entry.parent.parent
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()

    monkeypatch.chdir(project)
    compile_workflow_file(entry, outdir=tmp_path / "build-cwd")
    monkeypatch.chdir(elsewhere)
    compile_workflow_file(entry, outdir=tmp_path / "build-root", root=project)

    def resources(build: str) -> dict[str, Any]:
        plan = yaml.safe_load(
            (tmp_path / build / "compile" / "plan.yaml").read_text(encoding="utf-8")
        )
        return plan["execution"]["resources"]

    expected = {"kfactor": {"kind": "file", "path": str(project / "data" / "x.json")}}
    assert resources("build-cwd") == expected
    assert resources("build-root") == expected
    manifests = [
        yaml.safe_load(
            (tmp_path / build / "compile" / "inputs.manifest.yaml").read_text()
        )
        for build in ("build-cwd", "build-root")
    ]
    # The invocation (outdir, timestamp, hostname) differs on every compile by design.
    for manifest in manifests:
        manifest.pop("invocation")
    assert manifests[0] == manifests[1]
    assert manifests[0]["root"] == str(project)
