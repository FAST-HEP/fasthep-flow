from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from hepflow.api import compile_workflow_file, run_plan_file


def _summary(build_dir: Path) -> dict[str, Any]:
    text = (build_dir / "run_summary.yaml").read_text(encoding="utf-8")
    return yaml.safe_load(text)


def test_run_summary_has_bundle_id(tmp_path: Path, toy_workflow_path: Path) -> None:
    build_dir = tmp_path / "build"
    compile_workflow_file(toy_workflow_path, outdir=build_dir)
    manifest = yaml.safe_load(
        (build_dir / "compile" / "inputs.manifest.yaml").read_text(encoding="utf-8")
    )

    run_plan_file(build_dir / "compile" / "plan.yaml", outdir=build_dir)

    assert manifest["bundle_id"]
    assert _summary(build_dir)["bundle_id"] == manifest["bundle_id"]


def test_run_summary_without_manifest_has_no_bundle_id(
    tmp_path: Path, toy_workflow_path: Path
) -> None:
    build_dir = tmp_path / "build"
    compile_workflow_file(toy_workflow_path, outdir=build_dir)
    (build_dir / "compile" / "inputs.manifest.yaml").unlink()

    result = run_plan_file(build_dir / "compile" / "plan.yaml", outdir=build_dir)

    assert result.success
    assert "bundle_id" not in _summary(build_dir)


def test_run_summary_with_malformed_manifest_has_no_bundle_id(
    tmp_path: Path, toy_workflow_path: Path
) -> None:
    build_dir = tmp_path / "build"
    compile_workflow_file(toy_workflow_path, outdir=build_dir)
    (build_dir / "compile" / "inputs.manifest.yaml").write_text(
        "bundle_id: [unclosed\n", encoding="utf-8"
    )

    result = run_plan_file(build_dir / "compile" / "plan.yaml", outdir=build_dir)

    assert result.success
    assert "bundle_id" not in _summary(build_dir)
