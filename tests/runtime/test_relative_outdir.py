from __future__ import annotations

import json
import shutil
import warnings
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest
import yaml

from hepflow.api import run_workflow_file
from hepflow.runtime.provenance.inspect import _load_bundle


@pytest.fixture(autouse=True)
def _warnings_are_errors() -> Any:
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        yield


def _workflow(toy_workflow: dict[str, Any], tmp_path: Path) -> Path:
    workflow = deepcopy(toy_workflow)
    workflow["analysis"]["stages"][0]["write"] = [
        {"kind": "toy.write_path", "path": "plain.json", "when": "final"},
        {"kind": "toy.write_manifest", "path": "skims/part.toy", "when": "final"},
    ]
    path = tmp_path / "workflow.yaml"
    path.write_text(yaml.safe_dump(workflow, sort_keys=False), encoding="utf-8")
    return path


def _records(outdir: Path) -> dict[str, dict[str, Any]]:
    """Every provenance record, keyed by the node that produced the artifact."""
    _manifest, _execution, records = _load_bundle(outdir)
    return {
        str(record["producer"]["node_id"]): record["artifact"]
        for record in records.values()
    }


def _run(toy_workflow: dict[str, Any], tmp_path: Path, outdir: str | Path) -> Path:
    workflow_path = _workflow(toy_workflow, tmp_path)
    result = run_workflow_file(workflow_path, outdir=outdir, root=tmp_path)
    assert result.success
    return Path(outdir)


def test_relative_outdir_record_paths(
    toy_workflow: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    outdir = _run(toy_workflow, tmp_path, "out")

    artifacts = _records(outdir)
    plain = artifacts["write.Scale.0"]
    assert plain["path"] == "artifacts/files/plain.json"
    assert plain["path_type"] == "relative_to_outdir"
    assert artifacts["write.Scale.1"]["path"] == "artifacts/files/skims/part.toy"
    assert not Path(plain["path"]).is_absolute()
    assert (outdir / plain["path"]).is_file()
    for artifact in artifacts.values():
        if artifact["path_type"] == "relative_to_outdir":
            assert (outdir / artifact["path"]).is_file()


def test_relative_equals_absolute_outdir(
    toy_workflow: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    relative = _records(_run(toy_workflow, tmp_path, "out-rel"))
    absolute = _records(_run(toy_workflow, tmp_path, tmp_path / "out-abs"))

    assert relative == absolute


def test_writer_manifest_checksums_with_relative_outdir(
    toy_workflow: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    outdir = _run(toy_workflow, tmp_path, "out")

    manifests = list(outdir.glob("artifacts/files/*/manifest.json"))
    assert manifests
    for manifest_path in manifests:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        for dataset in manifest["datasets"].values():
            for entry in dataset["files"]:
                assert entry["size_bytes"] == 10
                assert (outdir / entry["path"]).stat().st_size == entry["size_bytes"]


def test_bundle_is_read_relocatable(
    toy_workflow: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    outdir = _run(toy_workflow, tmp_path, "out")
    copy = tmp_path / "elsewhere" / "copy"
    shutil.copytree(outdir, copy)
    shutil.rmtree(outdir)
    elsewhere = tmp_path / "away"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)

    manifest, _execution, records = _load_bundle(copy)

    assert records
    for entry in manifest["records"]:
        if entry["artifact"] and not Path(entry["artifact"]).is_absolute():
            assert (copy / entry["artifact"]).is_file()
    for record in records.values():
        artifact = record["artifact"]
        if artifact["path_type"] == "relative_to_outdir":
            assert (copy / artifact["path"]).is_file()

