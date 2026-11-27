"""The shared test builders: a workflow with changes, and files on disk."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from tests.builders import modified, modified_stage, write_file, write_files


def test_modified_returns_a_copy_and_leaves_the_original_alone() -> None:
    base: dict[str, Any] = {
        "data": {"datasets": [{"name": "a"}]},
        "execution": {"backend": "local"},
    }

    result = modified(base, execution={"strategy": "default"})

    assert result["execution"] == {"backend": "local", "strategy": "default"}
    assert base["execution"] == {"backend": "local"}
    result["data"]["datasets"][0]["name"] = "changed"
    assert base["data"]["datasets"][0]["name"] == "a"


def test_modified_merges_dicts_and_replaces_everything_else() -> None:
    base = {"execution": {"a": 1, "b": {"c": 2, "d": 3}}, "list": [1, 2], "x": 1}

    result = modified(base, execution={"b": {"c": 9}}, list=[3], x="y")

    assert result == {
        "execution": {"a": 1, "b": {"c": 9, "d": 3}},
        "list": [3],
        "x": "y",
    }


def test_modified_adds_new_keys_and_none_removes_a_key() -> None:
    base = {"a": 1, "b": {"c": 2}}

    result = modified(base, d=4, a=None, b={"c": None})

    assert result == {"b": {}, "d": 4}


def test_write_file_creates_parents_and_takes_text_or_bytes(tmp_path: Path) -> None:
    text = write_file(tmp_path / "a" / "b.txt", "hello")
    data = write_file(tmp_path / "c" / "d.bin", b"\x00\x01")

    assert text.read_text(encoding="utf-8") == "hello"
    assert data.read_bytes() == b"\x00\x01"


def test_write_files_writes_a_tree(tmp_path: Path) -> None:
    write_files(tmp_path, {"pkg/__init__.py": "", "pkg/mod/x.py": "X = 1\n"})

    assert (tmp_path / "pkg" / "__init__.py").read_text(encoding="utf-8") == ""
    assert (tmp_path / "pkg" / "mod" / "x.py").read_text(encoding="utf-8") == "X = 1\n"


def test_write_workflow_fixture_writes_the_toy_workflow(
    write_workflow: Any, toy_workflow: dict[str, Any]
) -> None:
    path = write_workflow()

    assert path.name == "workflow.yaml"
    assert yaml.safe_load(path.read_text(encoding="utf-8")) == toy_workflow


def test_write_workflow_fixture_applies_changes_to_the_toy_workflow(
    write_workflow: Any, toy_workflow: dict[str, Any]
) -> None:
    path = write_workflow(
        execution={"persistence": {"mode": "all"}}, filename="w2.yaml"
    )

    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert path.name == "w2.yaml"
    assert loaded["execution"] == {"persistence": {"mode": "all"}}
    assert loaded["analysis"] == toy_workflow["analysis"]


def test_write_workflow_fixture_takes_another_workflow(
    write_workflow: Any,
) -> None:
    path = write_workflow({"version": "1.0", "x": {"a": 1}}, x={"b": 2})

    assert yaml.safe_load(path.read_text(encoding="utf-8")) == {
        "version": "1.0",
        "x": {"a": 1, "b": 2},
    }


def _two_stage_workflow() -> dict[str, Any]:
    return {
        "registry": {"r": 1},
        "analysis": {
            "stages": [
                {"id": "A", "params": {"x": 1, "y": 2}, "write": [{"kind": "w"}]},
                {"id": "B", "params": {"x": 5}},
            ]
        },
    }


def test_modified_stage_changes_the_first_stage_by_default() -> None:
    result = modified_stage(_two_stage_workflow(), params={"x": 9}, applies_to={"a": 1})

    first, second = result["analysis"]["stages"]
    assert first["params"] == {"x": 9, "y": 2}
    assert first["applies_to"] == {"a": 1}
    assert second == {"id": "B", "params": {"x": 5}}
    assert result["registry"] == {"r": 1}


def test_modified_stage_finds_a_stage_by_id() -> None:
    result = modified_stage(_two_stage_workflow(), "B", params={"x": 6})

    first, second = result["analysis"]["stages"]
    assert second["params"] == {"x": 6}
    assert first["params"] == {"x": 1, "y": 2}


def test_modified_stage_replaces_lists_and_removes_with_none() -> None:
    result = modified_stage(_two_stage_workflow(), write=[{"kind": "other"}])
    assert result["analysis"]["stages"][0]["write"] == [{"kind": "other"}]

    result = modified_stage(_two_stage_workflow(), write=None)
    assert "write" not in result["analysis"]["stages"][0]


def test_modified_stage_leaves_the_original_alone() -> None:
    base = _two_stage_workflow()

    modified_stage(base, params={"x": 9}, write=None)

    assert base == _two_stage_workflow()


def test_modified_stage_with_an_unknown_id_says_so() -> None:
    with pytest.raises(KeyError, match="'nope'"):
        modified_stage(_two_stage_workflow(), "nope", params={"x": 1})
