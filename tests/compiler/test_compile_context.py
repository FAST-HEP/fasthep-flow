from __future__ import annotations

import dataclasses
from pathlib import Path
from typing import Any

import pytest
import yaml

from hepflow.api import compile_workflow_file
from hepflow.compiler.context import CompileContext
from hepflow.compiler.hooks.model import CompileHookResult, ParamCompileHookContext
from hepflow.compiler.hooks.runner import run_parameter_hook_chains_for_node
from hepflow.compiler.normalize import normalize_workflow
from hepflow.compiler.plan import build_plan_context, build_plan_from_normalized
from hepflow.model.component_spec import RuntimeComponentSpec
from hepflow.model.plan import ExecutionNode, ExecutionPlan


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


# --- the context travels through the plan and the parameter hooks ---

def record_context_hook(
    *, value: Any, options: dict[str, Any], context: ParamCompileHookContext
) -> CompileHookResult:
    del options
    return CompileHookResult(
        value=value,
        provenance={
            "hook": "toy.record_context",
            "workflow_dir": context.workflow_dir,
            "project_root": context.project_root,
            "fields": list(context.input_stream_fields),
        },
    )


def _run_hook(compile_context: CompileContext | None) -> dict[str, Any]:
    registry = {
        "compile_hooks": {
            "toy.record_context": {
                "kind": "parameter",
                "impl": "tests.compiler.test_compile_context:record_context_hook",
            }
        }
    }
    spec = RuntimeComponentSpec(
        name="toy.spec",
        kind="transform",
        params={"p": {"hooks": [{"name": "toy.record_context"}]}},
    )
    node = ExecutionNode(
        id="stage.A",
        graph_node_id="stage.A",
        role="transform",
        impl="toy.x",
        params={"p": "v"},
    )

    provenance = run_parameter_hook_chains_for_node(
        node=node,
        spec=spec,
        registry=registry,
        input_fields_by_context={None: {"stage.A": ["pt"]}},
        compile_context=compile_context,
    )

    records = provenance["p"]
    assert len(records) == 1
    return records[0]


def test_hook_context_receives_workflow_dir_and_project_root(tmp_path: Path) -> None:
    ctx = CompileContext(tmp_path / "entry" / "wf.yaml", tmp_path)

    record = _run_hook(ctx)

    assert record["workflow_dir"] == str(tmp_path / "entry")
    assert record["project_root"] == str(tmp_path)
    assert record["fields"] == ["pt"]


def test_hook_context_without_a_compile_context_has_none_values() -> None:
    record = _run_hook(None)

    assert record["workflow_dir"] is None
    assert record["project_root"] is None


def test_build_plan_context_writes_the_context_keys(tmp_path: Path) -> None:
    ctx = CompileContext(tmp_path / "entry" / "wf.yaml", tmp_path)

    context = build_plan_context(ExecutionPlan(), compile_context=ctx)

    assert context["workflow_path"] == str(tmp_path / "entry" / "wf.yaml")
    assert context["project_root"] == str(tmp_path)
    assert list(context)[-2:] == ["workflow_path", "project_root"]


def test_build_plan_context_without_a_compile_context_has_neither_key() -> None:
    context = build_plan_context(ExecutionPlan(), compile_context=None)

    assert "workflow_path" not in context
    assert "project_root" not in context


def test_plan_context_has_the_same_keys_as_before(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    project = tmp_path / "proj"
    entry = project / "entry" / "workflow.yaml"
    entry.parent.mkdir(parents=True)
    entry.write_text(yaml.safe_dump(toy_workflow, sort_keys=False), encoding="utf-8")

    plan = compile_workflow_file(entry, outdir=tmp_path / "build", root=project)

    assert plan.context["workflow_path"] == str(entry)
    assert plan.context["project_root"] == str(project)


def test_plan_from_an_old_normalized_file_records_the_entry_dir_as_root(
    toy_workflow: dict[str, Any],
) -> None:
    normalized = normalize_workflow(toy_workflow)
    normalized["workflow_path"] = "/p/entry/wf.yaml"

    _graph, plan = build_plan_from_normalized(normalized)

    assert plan.context["workflow_path"] == "/p/entry/wf.yaml"
    assert plan.context["project_root"] == "/p/entry"


def test_plan_without_a_workflow_path_has_no_context_keys(
    toy_workflow: dict[str, Any],
) -> None:
    _graph, plan = build_plan_from_normalized(normalize_workflow(toy_workflow))

    assert "workflow_path" not in plan.context
    assert "project_root" not in plan.context
