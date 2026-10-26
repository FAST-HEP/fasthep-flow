from __future__ import annotations

import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest
from tests.bundle_helpers import OUTDIR_ARTIFACT_HOOK, make_project, read_manifest

from hepflow.api import compile_from_bundle, compile_workflow_file
from hepflow.compiler.recompile import BundleReport, BundleVerificationError

# Run the real flow.software capture: the round trip must survive it.
REAL_SOFTWARE_CAPTURE = True


def _snapshot_compile(entry: Path, out: Path, **kwargs: Any) -> dict[str, Any]:
    compile_workflow_file(entry, outdir=out, root=entry.parent, snapshot=True, **kwargs)
    return read_manifest(out / "compile")


def test_round_trip_reproduces_plan_hash_and_bundle_id(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    entry = make_project(tmp_path, toy_workflow)
    recorded = _snapshot_compile(entry, tmp_path / "build", chunk_size=3)

    report = compile_from_bundle(tmp_path / "build" / "compile", tmp_path / "again")

    assert isinstance(report, BundleReport)
    assert report.recorded_plan_hash == recorded["plan_hash"]["sha256"]
    assert report.recompiled_plan_hash == recorded["plan_hash"]["sha256"]
    assert report.plan_hash_match
    assert report.recorded_bundle_id == recorded["bundle_id"]
    assert report.rebased_bundle_id == recorded["bundle_id"]
    assert report.bundle_id_match
    assert report.warnings == []
    assert report.hook_artifacts == []
    assert report.reproduced
    new = read_manifest(tmp_path / "again" / "compile")
    snapshot_root = str(tmp_path / "build" / "compile" / "inputs")
    assert new["root"] == snapshot_root
    assert new["invocation"]["chunk_size"] == 3
    assert (
        new["invocation"]["caller"]["from_bundle"]["bundle_id"] == recorded["bundle_id"]
    )
    as_dict = report.to_dict()
    assert as_dict["plan_hash"]["match"] is True
    assert as_dict["checks"] == []


def test_build_directory_is_accepted_as_bundle(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    entry = make_project(tmp_path, toy_workflow)
    _snapshot_compile(entry, tmp_path / "build")

    report = compile_from_bundle(tmp_path / "build", tmp_path / "again")

    assert report.plan_hash_match
    assert report.bundle_dir == str(tmp_path / "build" / "compile")


def _outside_project(tmp_path: Path, toy_workflow: dict[str, Any]) -> tuple[Path, Path]:
    outside = tmp_path / "shared" / "y.json"
    entry = make_project(
        tmp_path,
        toy_workflow,
        files={"data/x.json": b"{}", str(outside): b'{"y": 1}'},
        resources={"kfactor": "data/x.json", "remote": str(outside)},
    )
    return entry, outside


def test_changed_non_copied_file_is_a_warning(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    entry, outside = _outside_project(tmp_path, toy_workflow)
    _snapshot_compile(entry, tmp_path / "build")
    outside.write_bytes(b'{"y": 2}')

    report = compile_from_bundle(tmp_path / "build" / "compile", tmp_path / "again")

    (check,) = report.checks
    assert check.path == str(outside)
    assert check.status == "changed"
    assert check.actual_sha256 != check.expected_sha256
    assert len(report.warnings) == 1
    assert "changed" in report.warnings[0]
    assert not report.reproduced
    assert (tmp_path / "again" / "compile" / "plan.yaml").is_file()


def test_strict_raises_before_recompiling(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    entry, outside = _outside_project(tmp_path, toy_workflow)
    _snapshot_compile(entry, tmp_path / "build")
    outside.write_bytes(b'{"y": 2}')

    with pytest.raises(BundleVerificationError, match="changed") as caught:
        compile_from_bundle(
            tmp_path / "build" / "compile", tmp_path / "again", strict=True
        )

    assert [check.status for check in caught.value.checks] == ["changed"]
    assert not (tmp_path / "again" / "compile" / "plan.yaml").exists()


def test_strict_passes_when_everything_verifies(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    entry, _outside = _outside_project(tmp_path, toy_workflow)
    _snapshot_compile(entry, tmp_path / "build")

    report = compile_from_bundle(
        tmp_path / "build" / "compile", tmp_path / "again", strict=True
    )

    assert [check.status for check in report.checks] == ["ok"]
    assert report.reproduced


def test_missing_non_copied_file_is_unavailable(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    entry, outside = _outside_project(tmp_path, toy_workflow)
    _snapshot_compile(entry, tmp_path / "build")
    outside.rename(outside.with_suffix(".moved"))

    with pytest.warns(UserWarning, match="unavailable at compile time"):
        report = compile_from_bundle(tmp_path / "build" / "compile", tmp_path / "again")

    assert [check.status for check in report.checks] == ["unavailable"]
    assert "unavailable" in report.warnings[0]


def test_drifted_hook_artifact_is_reported(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    entry = make_project(
        tmp_path,
        toy_workflow,
        extra_hooks={"toy.outdir_artifact": OUTDIR_ARTIFACT_HOOK},
    )
    _snapshot_compile(entry, tmp_path / "build")

    report = compile_from_bundle(tmp_path / "build" / "compile", tmp_path / "again")

    assert report.plan_hash_match
    assert [(c.hook, c.artifact, c.status) for c in report.hook_artifacts] == [
        ("toy.outdir_artifact", "outdir_artifact", "changed")
    ]
    assert not report.reproduced


def test_bundle_without_snapshot_is_rejected(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    entry = make_project(tmp_path, toy_workflow)
    compile_workflow_file(entry, outdir=tmp_path / "build", root=entry.parent)

    with pytest.raises(ValueError, match="no snapshot"):
        compile_from_bundle(tmp_path / "build" / "compile", tmp_path / "again")


def test_missing_manifest_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match=r"inputs\.manifest\.yaml"):
        compile_from_bundle(tmp_path, tmp_path / "again")


def test_outdir_containing_the_bundle_is_rejected(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    entry = make_project(tmp_path, toy_workflow)
    _snapshot_compile(entry, tmp_path / "build")

    with pytest.raises(ValueError, match="contains the bundle"):
        compile_from_bundle(tmp_path / "build" / "compile", tmp_path / "build")
    assert (tmp_path / "build" / "compile" / "inputs" / "workflow.yaml").is_file()


def test_systematics_variation_bundle(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    entry = make_project(tmp_path, toy_workflow, systematics=True)
    compile_workflow_file(
        entry, outdir=tmp_path / "build", root=entry.parent, snapshot=True
    )
    bundle = tmp_path / "build" / "compile" / "trigger_eff_up"

    report = compile_from_bundle(bundle, tmp_path / "again")

    assert report.compile_dir == str(tmp_path / "again" / "compile" / "trigger_eff_up")
    assert report.recorded_bundle_id == read_manifest(bundle)["bundle_id"]
    assert report.plan_hash_match
    assert report.bundle_id_match
    assert report.reproduced


@pytest.mark.skipif(shutil.which("git") is None, reason="git is not installed")
def test_git_checkout_root_changes_only_the_bundle_id(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    entry = make_project(tmp_path, toy_workflow, git=True)
    subprocess.run(["git", "init", "-q", str(entry.parent)], check=True)
    _snapshot_compile(entry, tmp_path / "build")

    report = compile_from_bundle(tmp_path / "build" / "compile", tmp_path / "again")

    # The snapshot directory is not the original checkout: software.json records another
    # git state, so the bundle differs while the plan is the same.
    assert report.plan_hash_match
    assert not report.bundle_id_match
    assert [(c.hook, c.artifact) for c in report.hook_artifacts] == [
        ("flow.software", "software")
    ]
