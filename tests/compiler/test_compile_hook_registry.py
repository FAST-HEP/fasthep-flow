from __future__ import annotations

from typing import Any

import pytest
import yaml

from hepflow.compiler.hooks.expand_mapping_matrix import expand_mapping_matrix
from hepflow.compiler.hooks.load_mapping import load_mapping
from hepflow.compiler.hooks.model import ParamCompileHookContext
from hepflow.compiler.hooks.registry import resolve_compile_hook
from hepflow.compiler.hooks.runner import run_single_parameter_hook


def test_compile_hook_registry_resolves_parameter_hook() -> None:
    hook = resolve_compile_hook(_registry(), "flow.expand_field_glob", kind="parameter")

    assert hook.name == "flow.expand_field_glob"
    assert hook.kind == "parameter"
    assert callable(hook.impl)


def test_compile_hook_registry_resolves_mapping_matrix_hook() -> None:
    hook = resolve_compile_hook(
        _registry(), "flow.expand_mapping_matrix", kind="parameter"
    )

    assert hook.name == "flow.expand_mapping_matrix"
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


def test_load_mapping_inline_mapping_records_noop_provenance() -> None:
    result = load_mapping(
        value={"fields": {"pt": {}}},
        options={"formats": ["yaml"]},
        context=ParamCompileHookContext(),
    )

    assert result.value == {"fields": {"pt": {}}}
    assert result.provenance == {
        "hook": "flow.load_mapping",
        "input_kind": "mapping",
        "output_kind": "mapping",
    }


def test_load_mapping_rejects_extension_not_declared(tmp_path) -> None:
    path = tmp_path / "config.yml"
    path.write_text(yaml.safe_dump({"fields": {}}), encoding="utf-8")

    with pytest.raises(ValueError, match="unsupported file format"):
        load_mapping(
            value="config.yml",
            options={"formats": ["yaml"]},
            context=ParamCompileHookContext(workflow_dir=str(tmp_path)),
        )


def test_load_mapping_can_feed_later_hook(tmp_path) -> None:
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump({"fields": {}}), encoding="utf-8")
    registry = {
        "compile_hooks": {
            "flow.load_mapping": {
                "kind": "parameter",
                "impl": "hepflow.compiler.hooks.load_mapping:load_mapping",
            },
            "toy.mapping_result": {
                "kind": "parameter",
                "impl": "tests.compiler.test_compile_hook_registry:mapping_hook",
            },
        }
    }

    loaded = run_single_parameter_hook(
        value="config.yaml",
        hook_options={"name": "flow.load_mapping", "formats": ["yaml"]},
        context=ParamCompileHookContext(workflow_dir=str(tmp_path)),
        registry=registry,
        param_name="config",
        spec_name="toy.spec",
    )
    chained = run_single_parameter_hook(
        value=loaded.value,
        hook_options={"name": "toy.mapping_result"},
        context=ParamCompileHookContext(workflow_dir=str(tmp_path)),
        registry=registry,
        param_name="config",
        spec_name="toy.spec",
    )

    assert chained.value == ["fields", "mapped"]


def test_expand_mapping_matrix_expands_in_author_order() -> None:
    result = expand_mapping_matrix(
        value={
            "matrix": {
                "axes": {
                    "source": ["A", "B"],
                    "direction": ["up", "down"],
                },
                "mappings": [
                    {
                        "target": "jec_{source}_{direction}_pt",
                        "source": "jec_Nominal_pt",
                    }
                ],
            }
        },
        options={},
        context=ParamCompileHookContext(),
    )

    assert result.value == {
        "jec_A_up_pt": "jec_Nominal_pt",
        "jec_A_down_pt": "jec_Nominal_pt",
        "jec_B_up_pt": "jec_Nominal_pt",
        "jec_B_down_pt": "jec_Nominal_pt",
    }
    assert result.provenance["axis_names"] == ["source", "direction"]
    assert result.provenance["generated"] == 4


def test_expand_mapping_matrix_preserves_explicit_entries() -> None:
    result = expand_mapping_matrix(
        value={
            "Nominal_pt": "Jet_pt",
            "matrix": {
                "axes": {"direction": ["up"]},
                "mappings": [
                    {
                        "target": "jec_{direction}_pt",
                        "source": "jec_Nominal_pt",
                    }
                ],
            },
        },
        options={},
        context=ParamCompileHookContext(),
    )

    assert result.value == {
        "Nominal_pt": "Jet_pt",
        "jec_up_pt": "jec_Nominal_pt",
    }


def test_expand_mapping_matrix_rejects_duplicate_targets() -> None:
    with pytest.raises(ValueError, match="duplicate target 'jec_up_pt'"):
        expand_mapping_matrix(
            value={
                "matrix": {
                    "axes": {"source": ["jec"], "direction": ["up", "up"]},
                    "mappings": [
                        {
                            "target": "{source}_{direction}_pt",
                            "source": "jec_Nominal_pt",
                        }
                    ],
                }
            },
            options={},
            context=ParamCompileHookContext(),
        )


def test_expand_mapping_matrix_leaves_plain_mapping_unchanged() -> None:
    result = expand_mapping_matrix(
        value={"alias": "source"},
        options={},
        context=ParamCompileHookContext(),
    )

    assert result.value == {"alias": "source"}
    assert result.provenance["generated"] == 0


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
            },
            "flow.expand_mapping_matrix": {
                "kind": "parameter",
                "impl": (
                    "hepflow.compiler.hooks.expand_mapping_matrix:expand_mapping_matrix"
                ),
            },
        }
    }
