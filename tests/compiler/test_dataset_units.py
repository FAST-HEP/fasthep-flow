from __future__ import annotations

import os
from pathlib import Path

import pytest

from hepflow.compiler.dataset_units import (
    PublishConflict,
    dataset_manifest,
    manifest_hash,
    publish_if_absent,
    read_manifest,
    workflow_content_hash,
    write_manifest,
)


def test_publish_creates_the_file_and_leaves_no_temp_files(tmp_path: Path) -> None:
    target = tmp_path / "plan.yaml"

    assert publish_if_absent(target, b"a: 1\n") is True

    assert target.read_bytes() == b"a: 1\n"
    assert [p.name for p in tmp_path.iterdir()] == ["plan.yaml"]


def test_publishing_identical_content_again_is_a_noop(tmp_path: Path) -> None:
    target = tmp_path / "plan.yaml"
    publish_if_absent(target, b"a: 1\n")
    inode = target.stat().st_ino

    assert publish_if_absent(target, b"a: 1\n") is False

    assert target.stat().st_ino == inode
    assert [p.name for p in tmp_path.iterdir()] == ["plan.yaml"]


def test_conflicting_content_fails_and_keeps_the_winner(tmp_path: Path) -> None:
    target = tmp_path / "plan.yaml"
    publish_if_absent(target, b"a: 1\n")

    with pytest.raises(PublishConflict, match=r"plan\.yaml.*sha256"):
        publish_if_absent(target, b"a: 2\n")

    assert target.read_bytes() == b"a: 1\n"
    assert [p.name for p in tmp_path.iterdir()] == ["plan.yaml"]


def test_publish_creates_missing_parent_directories(tmp_path: Path) -> None:
    target = tmp_path / "compile" / "nominal" / "plan.yaml"

    publish_if_absent(target, b"x\n")

    assert target.read_bytes() == b"x\n"


def test_publish_never_replaces_an_existing_inode(tmp_path: Path) -> None:
    target = tmp_path / "plan.yaml"
    target.write_bytes(b"other\n")
    inode = target.stat().st_ino

    with pytest.raises(PublishConflict):
        publish_if_absent(target, b"mine\n")

    assert target.stat().st_ino == inode


def test_manifest_records_frozen_partition_inputs() -> None:
    record = {
        "name": "alpha",
        "files": ["a0.root", "a1.root"],
        "nevents": 17,
        "file_nevents": [10, 7],
    }

    manifest = dataset_manifest(record, chunk_size=5)

    assert manifest["version"] == 1
    assert manifest["dataset"] == "alpha"
    assert manifest["files"] == ["a0.root", "a1.root"]
    assert manifest["file_nevents"] == [10, 7]
    assert manifest["counted"] == [True, True]
    assert manifest["chunk_size"] == 5
    assert manifest["record_hash"] == manifest_hash(manifest)


def test_manifest_flags_files_without_a_count() -> None:
    record = {
        "name": "alpha",
        "files": ["a0.root", "a1.root"],
        "file_nevents": [10, None],
    }

    manifest = dataset_manifest(record, chunk_size=5)

    assert manifest["counted"] == [True, False]


def test_manifest_hash_ignores_its_own_hash_field() -> None:
    record = {"name": "alpha", "files": ["a.root"], "file_nevents": [3]}
    manifest = dataset_manifest(record, chunk_size=2)

    assert manifest_hash({**manifest, "record_hash": "stale"}) == manifest["record_hash"]
    other = dataset_manifest(record, chunk_size=3)
    assert other["record_hash"] != manifest["record_hash"]


def test_manifest_round_trips_and_is_published_once(tmp_path: Path) -> None:
    record = {"name": "alpha", "files": ["a.root"], "file_nevents": [3]}
    manifest = dataset_manifest(record, chunk_size=2)
    target = tmp_path / "datasets" / "alpha" / "manifest.yaml"

    assert write_manifest(target, manifest) is True
    assert write_manifest(target, manifest) is False

    assert read_manifest(target) == manifest
    with pytest.raises(PublishConflict):
        write_manifest(target, dataset_manifest(record, chunk_size=3))


def test_published_file_follows_the_umask_not_mkstemp_mode(tmp_path: Path) -> None:
    old = os.umask(0o022)
    try:
        publish_if_absent(tmp_path / "plan.yaml", b"x")
    finally:
        os.umask(old)

    assert (tmp_path / "plan.yaml").stat().st_mode & 0o777 == 0o644


def _normalized() -> dict[str, object]:
    return {
        "workflow_path": "/a/workflow.yaml",
        "project_root": "/a",
        "provenance": {"root": "/a", "inputs": [{"declared": "/a/workflow.yaml"}]},
        "data": {
            "datasets": [
                {"name": "x", "group": "mc", "files": ["f1"], "nevents": 5},
                {"name": "y", "group": "mc", "files": ["f2"], "nevents": 7},
            ]
        },
        "analysis": {"stages": [{"id": "s", "params": {"factor": 2}}]},
    }


def test_workflow_hash_ignores_location_and_file_data() -> None:
    other = _normalized()
    other["workflow_path"] = "../b/workflow.yaml"
    other["project_root"] = "/b"
    other["provenance"] = {"root": "/b", "inputs": []}
    other["data"] = {
        "datasets": [
            {"name": "x", "group": "mc", "files": ["other"]},
            {"name": "y", "group": "mc"},
        ]
    }

    assert workflow_content_hash(other) == workflow_content_hash(_normalized())


def test_workflow_hash_follows_the_workflow_content() -> None:
    changed = _normalized()
    changed["analysis"] = {"stages": [{"id": "s", "params": {"factor": 3}}]}
    regrouped = _normalized()
    regrouped["data"]["datasets"][0]["group"] = "data"  # type: ignore[index]

    assert workflow_content_hash(changed) != workflow_content_hash(_normalized())
    assert workflow_content_hash(regrouped) != workflow_content_hash(_normalized())
