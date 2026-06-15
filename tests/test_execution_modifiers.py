from __future__ import annotations

import json
from pathlib import Path
from re import escape
from typing import Any

import pytest
import yaml

from hepflow.api import compile_author_file, run_plan_file
from hepflow.model.plan import ExecutionNode
from hepflow.runtime.execution_modifiers import (
    resolve_execution_modifiers_for_node,
    run_transform_with_execution_modifiers,
)


def test_modifier_name_resolves_through_registry(toy_registry: dict[str, Any]) -> None:
    registry = _registry_with_modifiers(toy_registry)
    node = _modifier_node(["toy.A"])

    resolved = resolve_execution_modifiers_for_node(node, registry_cfg=registry)

    assert [modifier.name for modifier in resolved] == ["toy.A"]


def test_missing_modifier_errors_clearly(toy_registry: dict[str, Any]) -> None:
    node = _modifier_node(["toy.missing"])

    with pytest.raises(
        RuntimeError,
        match=escape(
            "Execution modifier 'toy.missing' is not registered for node stage.Scale"
        ),
    ):
        resolve_execution_modifiers_for_node(node, registry_cfg=toy_registry)


def test_invalid_modifier_shape_errors_clearly(toy_registry: dict[str, Any]) -> None:
    registry = _registry_with_modifiers(
        toy_registry,
        {"toy.invalid": {"impl": "tests.toy_components.modifiers:INVALID_MODIFIER"}},
    )
    node = _modifier_node(["toy.invalid"])

    with pytest.raises(
        TypeError,
        match="must define at least one callable hook",
    ):
        resolve_execution_modifiers_for_node(node, registry_cfg=registry)


def test_modifier_lifecycle_order_and_mutation(toy_registry: dict[str, Any]) -> None:
    registry = _registry_with_modifiers(toy_registry)
    node = _modifier_node(["toy.A", "toy.B"])
    node.params = {
        "source": "pt_plus",
        "output": "scaled",
        "factor": 2,
        "A_field": "pt_plus",
    }
    ctx: dict[str, Any] = {"_modifier_events": []}

    result = run_transform_with_execution_modifiers(
        node=node,
        inputs={"stream": {"pt": [1, 2]}},
        params=node.params,
        registry_cfg=registry,
        ctx=ctx,
    )

    assert ctx["_modifier_events"] == [
        "A.before",
        "B.before",
        "B.wrap.enter",
        "A.wrap.enter",
        "A.wrap.exit",
        "B.wrap.exit",
        "B.after",
        "A.after",
    ]
    assert result["stream"]["scaled"] == [4, 6]
    assert result["stream"]["A_after"] is True
    assert result["stream"]["B_after"] is True


@pytest.mark.parametrize(
    ("modifier_name", "message"),
    [
        (
            "toy.fail_before",
            "Execution modifier 'toy.fail_before' failed during before phase "
            "for node stage.Scale: before boom",
        ),
        (
            "toy.fail_wrap",
            "Execution modifier 'toy.fail_wrap' failed during wrap phase "
            "for node stage.Scale: wrap boom",
        ),
        (
            "toy.fail_after",
            "Execution modifier 'toy.fail_after' failed during after phase "
            "for node stage.Scale: after boom",
        ),
    ],
)
def test_modifier_hook_failures_include_phase_node_and_name(
    toy_registry: dict[str, Any],
    modifier_name: str,
    message: str,
) -> None:
    registry = _registry_with_modifiers(toy_registry)
    node = _modifier_node([modifier_name])

    with pytest.raises(RuntimeError, match=escape(message)):
        run_transform_with_execution_modifiers(
            node=node,
            inputs={"stream": {"pt": [1, 2]}},
            params=node.params,
            registry_cfg=registry,
            ctx={},
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
        "A_field": "pt_plus",
    }
    stage["execution"] = {"modifiers": ["toy.A"]}

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


def _modifier_node(modifiers: list[str]) -> ExecutionNode:
    return ExecutionNode(
        id="stage.Scale",
        graph_node_id="stage.Scale",
        role="transform",
        impl="toy.scale",
        params={"factor": 2},
        outputs={"stream": "event_stream"},
        meta={
            "execution": {
                "modifiers": [
                    {"name": modifier, "params": {}} for modifier in modifiers
                ]
            }
        },
    )


def _registry_with_modifiers(
    registry: dict[str, Any],
    modifiers: dict[str, dict[str, str]] | None = None,
) -> dict[str, Any]:
    return {
        **registry,
        "execution_modifiers": {
            "toy.A": {"impl": "tests.toy_components.modifiers:MODIFIER_A"},
            "toy.B": {"impl": "tests.toy_components.modifiers:MODIFIER_B"},
            "toy.fail_before": {
                "impl": "tests.toy_components.modifiers:FAILING_BEFORE"
            },
            "toy.fail_wrap": {"impl": "tests.toy_components.modifiers:FAILING_WRAP"},
            "toy.fail_after": {"impl": "tests.toy_components.modifiers:FAILING_AFTER"},
            **dict(modifiers or {}),
        },
    }
