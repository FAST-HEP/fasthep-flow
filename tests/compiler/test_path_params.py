from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import pytest
import yaml

from hepflow.api import normalise_workflow_file
from hepflow.compiler.bundle import SOURCE_ROLES
from hepflow.compiler.hooks.load_mapping import load_mapping
from hepflow.compiler.hooks.model import ParamCompileHookContext
from hepflow.compiler.input_paths import resolve_path_value


def _write(path: Path, content: bytes = b"{}") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return path


def _compile(
    tmp_path: Path, toy_workflow: dict[str, Any], params: dict[str, Any]
) -> dict[str, Any]:
    workflow = {
        **toy_workflow,
        "analysis": {
            "stages": [{"id": "PathStage", "op": "toy.path_param", "params": params}]
        },
    }
    entry = tmp_path / "entry" / "workflow.yaml"
    entry.parent.mkdir(parents=True, exist_ok=True)
    entry.write_text(yaml.safe_dump(workflow, sort_keys=False), encoding="utf-8")
    return normalise_workflow_file(entry, outdir=tmp_path / "build", root=tmp_path)


def _stage_params(normalized: dict[str, Any]) -> dict[str, Any]:
    return normalized["analysis"]["stages"][0]["params"]


def test_resolve_path_value_handles_strings_lists_and_rejects_other_types(
    tmp_path: Path,
) -> None:
    _write(tmp_path / "a.json", b"a")
    _write(tmp_path / "b.json", b"b")

    one, entries_one = resolve_path_value(
        "a.json", role="r", root=tmp_path, entry_dir=None
    )
    many, entries_many = resolve_path_value(
        ["a.json", "b.json"], role="r", root=tmp_path, entry_dir=None
    )

    assert one == str(tmp_path / "a.json")
    assert [e.role for e in entries_one] == ["r"]
    assert many == [str(tmp_path / "a.json"), str(tmp_path / "b.json")]
    assert [e.role for e in entries_many] == ["r[0]", "r[1]"]
    with pytest.raises(TypeError, match="my label"):
        resolve_path_value(3, role="r", root=tmp_path, entry_dir=None, label="my label")


def test_path_param_is_resolved_against_root_and_recorded(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    content = b'{"x": 1}'
    _write(tmp_path / "data" / "x.json", content)

    normalized = _compile(tmp_path, toy_workflow, {"path": "data/x.json"})

    assert _stage_params(normalized)["path"] == str(tmp_path / "data" / "x.json")
    assert [
        i for i in normalized["provenance"]["inputs"] if i["role"] not in SOURCE_ROLES
    ] == [
        {
            "role": "param:PathStage.path",
            "declared": "data/x.json",
            "resolved": str(tmp_path / "data" / "x.json"),
            "origin": "root",
            "sha256": hashlib.sha256(content).hexdigest(),
            "size": len(content),
            "note": None,
        }
    ]


def test_path_list_param_is_resolved_item_by_item(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    _write(tmp_path / "a.json", b"a")
    _write(tmp_path / "b.json", b"b")

    normalized = _compile(tmp_path, toy_workflow, {"paths": ["a.json", "b.json"]})

    assert _stage_params(normalized)["paths"] == [
        str(tmp_path / "a.json"),
        str(tmp_path / "b.json"),
    ]
    assert [
        i["role"]
        for i in normalized["provenance"]["inputs"]
        if i["role"] not in SOURCE_ROLES
    ] == [
        "param:PathStage.paths[0]",
        "param:PathStage.paths[1]",
    ]


def test_output_path_param_is_not_resolved(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    normalized = _compile(tmp_path, toy_workflow, {"out_path": "out/result.json"})

    assert _stage_params(normalized)["out_path"] == "out/result.json"
    assert [
        i for i in normalized["provenance"]["inputs"] if i["role"] not in SOURCE_ROLES
    ] == []


def test_missing_path_param_names_stage_and_param(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    with pytest.raises(FileNotFoundError) as excinfo:
        _compile(tmp_path, toy_workflow, {"path": "data/missing.json"})

    assert "PathStage" in str(excinfo.value)
    assert "path" in str(excinfo.value)


def _mapping_file(directory: Path, name: str = "map.yaml") -> bytes:
    content = yaml.safe_dump({"fields": {"pt": {}}}).encode()
    _write(directory / name, content)
    return content


def test_load_mapping_reads_relative_to_project_root(tmp_path: Path) -> None:
    root = tmp_path / "root"
    entry_dir = root / "skim"
    entry_dir.mkdir(parents=True)
    content = _mapping_file(root, "schemas.yaml")

    result = load_mapping(
        value="schemas.yaml",
        options={"formats": ["yaml"]},
        context=ParamCompileHookContext(
            workflow_dir=str(entry_dir), project_root=str(root)
        ),
    )

    assert result.value == {"fields": {"pt": {}}}
    assert result.provenance["resolved_path"] == str(root / "schemas.yaml")
    assert result.provenance["sha256"] == hashlib.sha256(content).hexdigest()
    assert result.provenance["size"] == len(content)
    assert result.provenance["origin"] == "root"
    assert result.provenance["input"] == "schemas.yaml"


def test_load_mapping_falls_back_to_entry_dir_with_deprecation_warning(
    tmp_path: Path,
) -> None:
    root = tmp_path / "root"
    entry_dir = root / "skim"
    _mapping_file(entry_dir, "schemas.yaml")

    with pytest.warns(FutureWarning, match="entry"):
        result = load_mapping(
            value="schemas.yaml",
            options={"formats": ["yaml"]},
            context=ParamCompileHookContext(
                workflow_dir=str(entry_dir), project_root=str(root)
            ),
        )

    assert result.provenance["origin"] == "entry_fallback"
    assert result.provenance["resolved_path"] == str(entry_dir / "schemas.yaml")


def test_load_mapping_without_project_root_resolves_against_workflow_dir(
    tmp_path: Path,
) -> None:
    _mapping_file(tmp_path, "schemas.yaml")

    result = load_mapping(
        value="schemas.yaml",
        options={"formats": ["yaml"]},
        context=ParamCompileHookContext(workflow_dir=str(tmp_path)),
    )

    assert result.provenance["resolved_path"] == str(tmp_path / "schemas.yaml")


def test_load_mapping_missing_absolute_file_raises_without_warning(
    tmp_path: Path,
) -> None:
    with pytest.raises(FileNotFoundError, match=r"flow\.load_mapping file not found"):
        load_mapping(
            value=str(tmp_path / "nope.yaml"),
            options={"formats": ["yaml"]},
            context=ParamCompileHookContext(project_root=str(tmp_path)),
        )


def test_load_mapping_missing_relative_file_names_the_file(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match=r"nope\.yaml"):
        load_mapping(
            value="nope.yaml",
            options={"formats": ["yaml"]},
            context=ParamCompileHookContext(project_root=str(tmp_path)),
        )
