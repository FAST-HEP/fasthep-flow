from __future__ import annotations

import copy
import hashlib
from pathlib import Path
from typing import Any

import pytest
import yaml

from hepflow.api import normalise_workflow_file
from hepflow.compiler.bundle import SOURCE_ROLES
from hepflow.compiler.resource_paths import resolve_resource_paths

REGISTRY: dict[str, Any] = {
    "resource_kinds": {
        "file": {"path_fields": ["path"]},
        "data_quality_mask": {"path_fields": ["paths"]},
    }
}


def _write(path: Path, content: bytes = b"{}") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return path


def test_path_field_is_resolved_and_recorded(tmp_path: Path) -> None:
    content = b'{"corrections": []}'
    _write(tmp_path / "data" / "x.json", content)
    resources = {"kfactor": {"kind": "file", "path": "data/x.json", "extra": 1}}

    resolved, entries = resolve_resource_paths(
        resources, registry=REGISTRY, root=tmp_path, entry_dir=None
    )

    assert resolved["kfactor"] == {
        "kind": "file",
        "path": str(tmp_path / "data" / "x.json"),
        "extra": 1,
    }
    assert len(entries) == 1
    entry = entries[0]
    assert entry.role == "resource:kfactor.path"
    assert entry.declared == "data/x.json"
    assert entry.resolved == str(tmp_path / "data" / "x.json")
    assert entry.sha256 == hashlib.sha256(content).hexdigest()


def test_paths_list_field_resolves_every_item(tmp_path: Path) -> None:
    for name in ("a", "b", "c"):
        _write(tmp_path / "masks" / f"{name}.json", name.encode())
    resources = {
        "cms.lumimask": {
            "kind": "data_quality_mask",
            "paths": ["masks/a.json", "masks/b.json", "masks/c.json"],
        }
    }

    resolved, entries = resolve_resource_paths(
        resources, registry=REGISTRY, root=tmp_path, entry_dir=None
    )

    assert resolved["cms.lumimask"]["paths"] == [
        str(tmp_path / "masks" / f"{name}.json") for name in ("a", "b", "c")
    ]
    assert [e.role for e in entries] == [
        "resource:cms.lumimask.paths[0]",
        "resource:cms.lumimask.paths[1]",
        "resource:cms.lumimask.paths[2]",
    ]


def test_unregistered_kind_is_left_untouched_with_warning(tmp_path: Path) -> None:
    resources = {"r": {"kind": "mystery", "path": "data/x.json"}}

    with pytest.warns(UserWarning, match="declares no path fields"):
        resolved, entries = resolve_resource_paths(
            resources, registry=REGISTRY, root=tmp_path, entry_dir=None
        )

    assert resolved == resources
    assert entries == []


def test_resource_without_kind_is_left_untouched(tmp_path: Path) -> None:
    resources = {"default": {"cpus": 1, "memory": "2GB", "path": "not/a/path"}}

    resolved, entries = resolve_resource_paths(
        resources, registry=REGISTRY, root=tmp_path, entry_dir=None
    )

    assert resolved == resources
    assert entries == []


def test_missing_file_names_resource_and_field(tmp_path: Path) -> None:
    resources = {"cms.lumimask": {"kind": "data_quality_mask", "paths": ["nope.json"]}}

    with pytest.raises(FileNotFoundError) as excinfo:
        resolve_resource_paths(
            resources, registry=REGISTRY, root=tmp_path, entry_dir=None
        )

    message = str(excinfo.value)
    assert "cms.lumimask" in message
    assert "paths" in message
    assert "nope.json" in message


def test_empty_path_item_raises_value_error(tmp_path: Path) -> None:
    _write(tmp_path / "a.json")
    resources = {"m": {"kind": "data_quality_mask", "paths": ["a.json", ""]}}

    with pytest.raises(ValueError, match=r"resource:m\.paths\[1\]"):
        resolve_resource_paths(
            resources, registry=REGISTRY, root=tmp_path, entry_dir=None
        )


def test_non_string_path_value_raises_type_error(tmp_path: Path) -> None:
    resources = {"r": {"kind": "file", "path": 3}}

    with pytest.raises(TypeError, match=r"resource 'r'.*'path'"):
        resolve_resource_paths(
            resources, registry=REGISTRY, root=tmp_path, entry_dir=None
        )


def test_same_file_declared_twice_gives_two_entries(tmp_path: Path) -> None:
    _write(tmp_path / "x.json", b"x")
    resources = {
        "one": {"kind": "file", "path": "x.json"},
        "two": {"kind": "file", "path": "x.json"},
    }

    resolved, entries = resolve_resource_paths(
        resources, registry=REGISTRY, root=tmp_path, entry_dir=None
    )

    assert (
        resolved["one"]["path"] == resolved["two"]["path"] == str(tmp_path / "x.json")
    )
    assert sorted(e.role for e in entries) == ["resource:one.path", "resource:two.path"]


def test_uri_and_absolute_values_are_not_rewritten(tmp_path: Path) -> None:
    outside = _write(tmp_path / "outside" / "abs.json", b"abs")
    root = tmp_path / "root"
    root.mkdir()
    resources = {
        "remote": {"kind": "file", "path": "root://x//store/f.json"},
        "abs": {"kind": "file", "path": str(outside)},
    }

    resolved, entries = resolve_resource_paths(
        resources, registry=REGISTRY, root=root, entry_dir=None
    )

    assert resolved["remote"]["path"] == "root://x//store/f.json"
    assert resolved["abs"]["path"] == str(outside)
    assert {e.origin for e in entries} == {"uri", "absolute"}


def test_input_resources_are_not_mutated(tmp_path: Path) -> None:
    _write(tmp_path / "x.json")
    resources = {"r": {"kind": "file", "path": "x.json"}}
    before = copy.deepcopy(resources)

    resolve_resource_paths(resources, registry=REGISTRY, root=tmp_path, entry_dir=None)

    assert resources == before


def test_malformed_kind_declaration_raises_value_error(tmp_path: Path) -> None:
    registry = {"resource_kinds": {"file": {"path_fields": "path"}}}
    resources = {"r": {"kind": "file", "path": "x.json"}}

    with pytest.raises(ValueError, match="path_fields"):
        resolve_resource_paths(
            resources, registry=registry, root=tmp_path, entry_dir=None
        )


def test_compile_resolves_resources_and_records_inputs(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    content = b'{"k": 1}'
    _write(tmp_path / "data" / "kfactor.json", content)
    workflow = {
        **toy_workflow,
        "execution": {
            "resources": {
                "kfactor": {"kind": "file", "path": "data/kfactor.json"},
                "default": {"cpus": 1, "memory": "2GB"},
            }
        },
    }
    entry = tmp_path / "entry" / "workflow.yaml"
    entry.parent.mkdir()
    entry.write_text(yaml.safe_dump(workflow, sort_keys=False), encoding="utf-8")

    normalized = normalise_workflow_file(
        entry, outdir=tmp_path / "build", root=tmp_path
    )

    resources = normalized["execution"]["resources"]
    assert resources["kfactor"]["path"] == str(tmp_path / "data" / "kfactor.json")
    assert resources["default"] == {"cpus": 1, "memory": "2GB"}
    inputs = [
        i for i in normalized["provenance"]["inputs"] if i["role"] not in SOURCE_ROLES
    ]
    assert inputs == [
        {
            "role": "resource:kfactor.path",
            "declared": "data/kfactor.json",
            "resolved": str(tmp_path / "data" / "kfactor.json"),
            "origin": "root",
            "sha256": hashlib.sha256(content).hexdigest(),
            "size": len(content),
            "note": None,
        }
    ]
