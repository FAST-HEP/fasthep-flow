from __future__ import annotations

from typing import Any

import pytest

from hepflow.compiler.hooks.model import ParamCompileHookContext
from hepflow.compiler.hooks.registry import resolve_compile_hook
from hepflow.compiler.hooks.runner import run_single_parameter_hook


def test_compile_hook_registry_resolves_parameter_hook() -> None:
    hook = resolve_compile_hook(_registry(), "flow.expand_field_glob", kind="parameter")

    assert hook.name == "flow.expand_field_glob"
    assert hook.kind == "parameter"
    assert callable(hook.impl)


def test_compile_hook_registry_rejects_wrong_kind() -> None:
    with pytest.raises(TypeError, match="expected 'phase'"):
        resolve_compile_hook(_registry(), "flow.expand_field_glob", kind="phase")


def test_parameter_hook_runner_accepts_mapping_result() -> None:
    result = run_single_parameter_hook(
        value=["a"],
        hook_options={"name": "toy.mapping_result"},
        context=ParamCompileHookContext(),
        registry={
            "compile_hooks": {
                "toy.mapping_result": {
                    "kind": "parameter",
                    "impl": "tests.compiler.test_compile_hook_registry:mapping_hook",
                }
            }
        },
        param_name="fields",
        spec_name="toy.spec",
    )

    assert result.value == ["a", "mapped"]
    assert result.provenance == {"hook": "toy.mapping_result"}


def mapping_hook(
    *,
    value: Any,
    options: dict[str, Any],
    context: ParamCompileHookContext,
) -> dict[str, Any]:
    del options, context
    return {
        "value": [*list(value), "mapped"],
        "provenance": {"hook": "toy.mapping_result"},
    }


def _registry() -> dict[str, Any]:
    return {
        "compile_hooks": {
            "flow.expand_field_glob": {
                "kind": "parameter",
                "impl": "hepflow.compiler.hooks.expand_field_glob:expand_field_glob",
            }
        }
    }
