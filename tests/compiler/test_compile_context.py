from __future__ import annotations

import dataclasses
from pathlib import Path

import pytest

from hepflow.compiler.context import CompileContext


def test_entry_dir_is_the_parent_of_the_workflow(tmp_path: Path) -> None:
    ctx = CompileContext(tmp_path / "entry" / "wf.yaml", tmp_path)

    assert ctx.entry_dir == tmp_path / "entry"


def test_for_workflow_resolves_the_root_and_defaults_to_cwd(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)

    default = CompileContext.for_workflow(tmp_path / "wf.yaml")
    explicit = CompileContext.for_workflow(tmp_path / "wf.yaml", root=tmp_path / ".")

    assert default.project_root == tmp_path
    assert explicit.project_root == tmp_path
    assert default.workflow_path == tmp_path / "wf.yaml"


def test_for_workflow_rejects_a_missing_root(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="root"):
        CompileContext.for_workflow(tmp_path / "wf.yaml", root=tmp_path / "nope")


def test_from_workflow_path_defaults_the_root_to_the_entry_dir_without_checks() -> None:
    ctx = CompileContext.from_workflow_path("does/not/exist/wf.yaml")

    assert ctx.workflow_path == Path("does/not/exist/wf.yaml")
    assert ctx.project_root == Path("does/not/exist")
    assert (
        CompileContext.from_workflow_path("a/wf.yaml", Path("/r")).project_root
        == Path("/r")
    )


def test_to_normalized_uses_the_existing_keys(tmp_path: Path) -> None:
    ctx = CompileContext(tmp_path / "e" / "wf.yaml", tmp_path)

    assert ctx.to_normalized() == {
        "workflow_path": str(tmp_path / "e" / "wf.yaml"),
        "project_root": str(tmp_path),
    }


def test_round_trip_through_normalized_and_plan_context(tmp_path: Path) -> None:
    ctx = CompileContext(tmp_path / "e" / "wf.yaml", tmp_path)

    assert CompileContext.from_normalized(ctx.to_normalized()) == ctx
    assert CompileContext.from_plan_context(ctx.to_normalized()) == ctx


def test_from_normalized_of_an_old_file_falls_back_to_the_entry_dir() -> None:
    ctx = CompileContext.from_normalized({"workflow_path": "/p/e/wf.yaml"})

    assert ctx is not None
    assert ctx.project_root == Path("/p/e")
    old = CompileContext.from_plan_context({"workflow_path": "/p/e/wf.yaml"})
    assert old is not None
    assert old.project_root == Path("/p/e")


@pytest.mark.parametrize("data", [{}, {"workflow_path": ""}, {"workflow_path": None}])
def test_without_a_workflow_path_there_is_no_context(data: dict[str, object]) -> None:
    assert CompileContext.from_normalized(data) is None
    assert CompileContext.from_plan_context(data) is None


def test_the_context_is_frozen(tmp_path: Path) -> None:
    ctx = CompileContext(tmp_path / "wf.yaml", tmp_path)

    with pytest.raises(dataclasses.FrozenInstanceError):
        ctx.project_root = tmp_path / "other"  # type: ignore[misc]
