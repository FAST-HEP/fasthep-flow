"""The shared test builders: a workflow with changes, and files on disk."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from tests.builders import modified, write_file, write_files, write_yaml


def test_modified_returns_a_copy_and_leaves_the_original_alone() -> None:
    base = {"data": {"datasets": [{"name": "a"}]}, "execution": {"backend": "local"}}

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


def test_write_yaml_keeps_key_order(tmp_path: Path) -> None:
    path = write_yaml(tmp_path / "w.yaml", {"version": "1.0", "a": 1, "z": 2, "b": 3})

    assert list(yaml.safe_load(path.read_text(encoding="utf-8"))) == [
        "version",
        "a",
        "z",
        "b",
    ]
    assert path.read_text(encoding="utf-8").startswith("version: '1.0'")


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
