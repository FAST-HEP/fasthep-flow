from __future__ import annotations

import json
from pathlib import Path
from re import escape
from typing import Any

import pytest
import yaml

from hepflow.api import compile_author_file, run_plan_file
from hepflow.model.plan import ExecutionNode, ExecutionPlan, PlanInputRef
from hepflow.runtime.engine import execute_plan_partition
from hepflow.runtime.hooks.manager import HookDispatchError, HookManager


def test_modifier_name_resolves_through_hook_manager(toy_registry: dict[str, Any]) -> None:
    plan = _plan_with_modifier(toy_registry, ["toy.A"])
    manager = HookManager.from_plan(plan)

    manager.before_node(
        node=plan.get_node("stage.Scale"),
        inputs={"stream": {"pt": [1]}},
        ctx={"A_field": "pt_plus"},
    )

    assert manager.usage_summary()["enabled"][0]["kind"] == "toy.A"
    assert manager.usage_summary()["enabled"][0]["source"] == "execution_modifier"


def test_missing_modifier_errors_clearly(toy_registry: dict[str, Any]) -> None:
    plan = _plan_with_modifier(toy_registry, ["toy.missing"])
    manager = HookManager.from_plan(plan)

    with pytest.raises(
        HookDispatchError,
        match=escape(
            "Error execution_modifier 'toy.missing' failed during resolve "
            "for node stage.Scale: Execution modifier 'toy.missing' is not registered"
        ),
    ):
        manager.before_node(
            node=plan.get_node("stage.Scale"),
            inputs={"stream": {"pt": [1]}},
            ctx={},
        )


def test_invalid_modifier_shape_errors_clearly(toy_registry: dict[str, Any]) -> None:
    registry = _registry_with_modifiers(
        toy_registry,
        {"toy.invalid": {"impl": "tests.toy_components.modifiers:INVALID_MODIFIER"}},
    )
    plan = _plan_with_modifier(registry, ["toy.invalid"])
    manager = HookManager.from_plan(plan)

    with pytest.raises(
        HookDispatchError,
        match="execution modifier must define at least one node lifecycle method",
    ):
        manager.before_node(
            node=plan.get_node("stage.Scale"),
            inputs={"stream": {"pt": [1]}},
            ctx={},
        )


def test_modifier_lifecycle_order_and_mutation(toy_registry: dict[str, Any]) -> None:
    registry = _registry_with_modifiers(toy_registry)
    plan = _plan_with_modifier(registry, ["toy.A", "toy.B"])
    node = plan.get_node("stage.Scale")
    node.params = {"source": "pt_plus", "output": "scaled", "factor": 2}
    ctx: dict[str, Any] = {"_modifier_events": [], "A_field": "pt_plus"}

    value_store = execute_plan_partition(
        plan,
        ctx=ctx,
        registry_cfg=registry,
        initial_values={("source.Events", "stream"): {"pt": [1, 2]}},
        skip_roles={"source"},
    )

    assert ctx["_modifier_events"] == [
        "A.around.enter",
        "B.around.enter",
        "A.before",
        "B.before",
        "B.after",
        "A.after",
        "B.around.exit",
        "A.around.exit",
    ]
    stream = value_store[("stage.Scale", "stream")]
    assert stream["scaled"] == [4, 6]
    assert stream["A_after"] is True
    assert stream["B_after"] is True


def test_global_hook_wraps_node_modifier_lifecycle(toy_registry: dict[str, Any]) -> None:
    registry = _registry_with_modifiers(
        {
            **toy_registry,
            "hooks": {
                **dict(toy_registry.get("hooks") or {}),
                "toy.order": {
                    "spec": "tests.toy_components.hooks:TOY_ORDER_HOOK_SPEC",
                    "impl": "tests.toy_components.hooks:ToyOrderHook",
                },
            },
        }
    )
    plan = _plan_with_modifier(registry, ["toy.A"])
    plan.execution_hooks = [
        {
            "kind": "toy.order",
            "events": ["around_node", "before_node", "after_node"],
        }
    ]
    node = plan.get_node("stage.Scale")
    node.params = {"source": "pt_plus", "output": "scaled", "factor": 2}
    ctx: dict[str, Any] = {"_modifier_events": [], "A_field": "pt_plus"}

    execute_plan_partition(
        plan,
        ctx=ctx,
        registry_cfg=registry,
        initial_values={("source.Events", "stream"): {"pt": [1]}},
        skip_roles={"source"},
    )

    assert ctx["_modifier_events"] == [
        "global.around.enter",
        "A.around.enter",
        "global.before",
        "A.before",
        "A.after",
        "global.after",
        "A.around.exit",
        "global.around.exit",
    ]


@pytest.mark.parametrize(
    ("modifier_name", "message"),
    [
        (
            "toy.fail_before",
            "Error execution_modifier 'toy.fail_before' failed during before_node "
            "for node stage.Scale: before boom",
        ),
        (
            "toy.fail_around",
            "Error execution_modifier 'toy.fail_around' failed during around_node "
            "for node stage.Scale: around boom",
        ),
        (
            "toy.fail_after",
            "Error execution_modifier 'toy.fail_after' failed during after_node "
            "for node stage.Scale: after boom",
        ),
    ],
)
def test_modifier_hook_failures_use_hook_dispatch_error(
    toy_registry: dict[str, Any],
    modifier_name: str,
    message: str,
) -> None:
    registry = _registry_with_modifiers(toy_registry)
    plan = _plan_with_modifier(registry, [modifier_name])

    with pytest.raises(HookDispatchError, match=escape(message)):
        execute_plan_partition(
            plan,
            ctx={},
            registry_cfg=registry,
            initial_values={("source.Events", "stream"): {"pt": [1, 2]}},
            skip_roles={"source"},
        )


def test_runtime_executes_transform_with_modifier_hooks(
    toy_author: dict[str, Any],
    tmp_path: Path,
) -> None:
    author = {
        **toy_author,
        "registry": _registry_with_modifiers(toy_author["registry"]),
    }
    stage = author["analysis"]["stages"][0]
    stage["params"] = {
        "source": "pt_plus",
        "output": "modified",
        "factor": 2,
    }
    stage["execution"] = {
        "modifiers": [{"name": "toy.A", "params": {"A_field": "pt_plus"}}]
    }

    author_path = tmp_path / "author.yaml"
    author_path.write_text(yaml.safe_dump(author, sort_keys=False), encoding="utf-8")
    build_dir = tmp_path / "build"
    compile_author_file(author_path, outdir=build_dir)

    result = run_plan_file(build_dir / "compile" / "plan.yaml", outdir=build_dir)

    assert result.success is True
    payload = json.loads(
        (build_dir / "artifacts" / "files" / "output.json").read_text(
            encoding="utf-8"
        )
    )
    assert payload["modified"] == [26, 38, 44, 58]
    assert payload["A_after"] is True


def _plan_with_modifier(
    registry: dict[str, Any],
    modifiers: list[str],
) -> ExecutionPlan:
    plan = ExecutionPlan(registry=_registry_with_modifiers(registry))
    plan.add_node(
        ExecutionNode(
            id="source.Events",
            graph_node_id="source.Events",
            role="source",
            impl="toy.source",
            outputs={"stream": "event_stream"},
        )
    )
    plan.add_node(
        ExecutionNode(
            id="stage.Scale",
            graph_node_id="stage.Scale",
            role="transform",
            impl="toy.scale",
            inputs=[
                PlanInputRef(
                    node_id="source.Events",
                    output_name="stream",
                    input_name="stream",
                )
            ],
            params={"factor": 2},
            outputs={"stream": "event_stream"},
            meta={
                "execution": {
                    "modifiers": [
                        {"name": modifier, "params": {}}
                        for modifier in modifiers
                    ]
                }
            },
        )
    )
    return plan


def _registry_with_modifiers(
    registry: dict[str, Any],
    modifiers: dict[str, dict[str, str]] | None = None,
) -> dict[str, Any]:
    return {
        **registry,
        "execution_modifiers": {
            **dict(registry.get("execution_modifiers") or {}),
            "toy.A": {"impl": "tests.toy_components.modifiers:MODIFIER_A"},
            "toy.B": {"impl": "tests.toy_components.modifiers:MODIFIER_B"},
            "toy.fail_before": {
                "impl": "tests.toy_components.modifiers:FAILING_BEFORE"
            },
            "toy.fail_around": {
                "impl": "tests.toy_components.modifiers:FAILING_AROUND"
            },
            "toy.fail_after": {"impl": "tests.toy_components.modifiers:FAILING_AFTER"},
            **dict(modifiers or {}),
        },
    }
