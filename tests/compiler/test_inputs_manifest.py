from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import yaml

from hepflow.api import compile_workflow_file
from hepflow.compiler.inputs_manifest import write_inputs_manifest


def _write(path: Path, content: bytes) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return path


def _manifest(outdir: Path) -> dict[str, Any]:
    text = (outdir / "compile" / "inputs.manifest.yaml").read_text(encoding="utf-8")
    return yaml.safe_load(text)


def _row(role: str, declared: str) -> dict[str, Any]:
    return {
        "role": role,
        "declared": declared,
        "resolved": f"/r/{declared}",
        "origin": "root",
        "sha256": "abc",
        "size": 3,
        "note": None,
    }


def test_write_inputs_manifest_sorts_inputs_and_records_root(tmp_path: Path) -> None:
    path = write_inputs_manifest(
        tmp_path,
        root="/r",
        inputs=[_row("resource:b.path", "b"), _row("param:a.path", "a")],
    )

    assert path == tmp_path / "inputs.manifest.yaml"
    manifest = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert manifest["version"] == 1
    assert manifest["root"] == "/r"
    assert [row["role"] for row in manifest["inputs"]] == [
        "param:a.path",
        "resource:b.path",
    ]


def test_write_inputs_manifest_keeps_only_the_documented_keys(tmp_path: Path) -> None:
    row = {**_row("r", "x"), "internal": "dropped"}

    write_inputs_manifest(tmp_path, root="/r", inputs=[row])

    written = yaml.safe_load(
        (tmp_path / "inputs.manifest.yaml").read_text(encoding="utf-8")
    )["inputs"][0]
    assert set(written) == {
        "role",
        "declared",
        "resolved",
        "origin",
        "sha256",
        "size",
        "note",
    }


def test_workflow_without_inputs_writes_empty_list_and_root(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    entry = tmp_path / "workflow.yaml"
    entry.write_text(yaml.safe_dump(toy_workflow, sort_keys=False), encoding="utf-8")

    compile_workflow_file(entry, outdir=tmp_path / "build", root=tmp_path)

    manifest = _manifest(tmp_path / "build")
    assert manifest == {"version": 1, "root": str(tmp_path), "inputs": []}


def _project(tmp_path: Path) -> tuple[Path, dict[str, bytes]]:
    contents = {
        "kfactor": b'{"kfactor": 1}',
        "x": b'{"x": 1}',
        "map": yaml.safe_dump({"version": 1, "fields": {"pt": {}}}).encode(),
    }
    _write(tmp_path / "data" / "kfactor.json", contents["kfactor"])
    _write(tmp_path / "data" / "x.json", contents["x"])
    _write(tmp_path / "schemas" / "map.yaml", contents["map"])
    workflow = {
        "version": "1.0",
        "use": {"profiles": ["tests.toy_components:registry"]},
        "sources": {"events": {"kind": "toy.source"}},
        "execution": {
            "resources": {"kfactor": {"kind": "file", "path": "data/kfactor.json"}}
        },
        "analysis": {
            "stages": [
                {
                    "id": "PathStage",
                    "op": "toy.path_param",
                    "params": {"path": "data/x.json"},
                },
                {
                    "id": "Config",
                    "op": "toy.mapping_config",
                    "params": {"config": "schemas/map.yaml"},
                },
            ]
        },
    }
    entry = tmp_path / "entry" / "workflow.yaml"
    entry.parent.mkdir(parents=True, exist_ok=True)
    entry.write_text(yaml.safe_dump(workflow, sort_keys=False), encoding="utf-8")
    return entry, contents


def test_manifest_lists_resources_params_and_mapping_files_with_hashes(
    tmp_path: Path,
) -> None:
    entry, contents = _project(tmp_path)

    compile_workflow_file(entry, outdir=tmp_path / "build", root=tmp_path)

    manifest = _manifest(tmp_path / "build")
    assert manifest["root"] == str(tmp_path)
    by_role = {row["role"]: row for row in manifest["inputs"]}
    assert sorted(by_role) == [
        "param:Config.config",
        "param:PathStage.path",
        "resource:kfactor.path",
    ]
    for role, key, declared, relative in [
        ("resource:kfactor.path", "kfactor", "data/kfactor.json", "data/kfactor.json"),
        ("param:PathStage.path", "x", "data/x.json", "data/x.json"),
        ("param:Config.config", "map", "schemas/map.yaml", "schemas/map.yaml"),
    ]:
        row = by_role[role]
        assert row["declared"] == declared
        assert row["resolved"] == str(tmp_path / relative)
        assert row["origin"] == "root"
        assert row["sha256"] == hashlib.sha256(contents[key]).hexdigest()
        assert row["size"] == len(contents[key])


def test_manifest_is_deterministic(tmp_path: Path) -> None:
    entry, _ = _project(tmp_path)

    compile_workflow_file(entry, outdir=tmp_path / "build-1", root=tmp_path)
    compile_workflow_file(entry, outdir=tmp_path / "build-2", root=tmp_path)

    first = (tmp_path / "build-1" / "compile" / "inputs.manifest.yaml").read_text()
    second = (tmp_path / "build-2" / "compile" / "inputs.manifest.yaml").read_text()
    assert first == second


def test_write_inputs_manifest_collapses_rows_with_the_same_role_and_resolved_path(
    tmp_path: Path,
) -> None:
    # A param that is both ``kind: path`` and loaded by flow.load_mapping is recorded twice:
    # once with the authored text, once with the already-absolute path.
    authored = {
        **_row("param:Config.config", "schemas/map.yaml"),
        "resolved": "/r/map.yaml",
    }
    absolute = {**_row("param:Config.config", "/r/map.yaml"), "resolved": "/r/map.yaml"}

    write_inputs_manifest(tmp_path, root="/r", inputs=[authored, absolute])

    rows = yaml.safe_load(
        (tmp_path / "inputs.manifest.yaml").read_text(encoding="utf-8")
    )["inputs"]
    assert [row["declared"] for row in rows] == ["schemas/map.yaml"]


def test_write_inputs_manifest_keeps_distinct_files_under_one_role(
    tmp_path: Path,
) -> None:
    first = {**_row("resource:m.paths[0]", "a"), "resolved": "/r/a"}
    second = {**_row("resource:m.paths[0]", "b"), "resolved": "/r/b"}

    write_inputs_manifest(tmp_path, root="/r", inputs=[first, second])

    rows = yaml.safe_load(
        (tmp_path / "inputs.manifest.yaml").read_text(encoding="utf-8")
    )["inputs"]
    assert [row["resolved"] for row in rows] == ["/r/a", "/r/b"]
