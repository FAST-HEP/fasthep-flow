from __future__ import annotations

from typing import Any

import pytest
import yaml

from hepflow.compiler.hooks.expand_field_glob import expand_field_glob
from hepflow.compiler.hooks.model import ParamCompileHookContext
from hepflow.compiler.normalize import normalize_workflow
from hepflow.compiler.plan import build_plan_from_normalized


def test_field_glob_expands_exact_field(toy_registry: dict[str, Any]) -> None:
    plan = _plan(toy_registry, [_glob_stage(["Foo_a"])], fields=["Foo_a", "Foo_b"])

    assert _glob_params(plan) == ["Foo_a"]


def test_field_glob_expands_star_prefix(toy_registry: dict[str, Any]) -> None:
    plan = _plan(
        toy_registry,
        [_glob_stage(["Foo_*"])],
        fields=["Foo_a", "Bar_a", "Foo_b"],
    )

    assert _glob_params(plan) == ["Foo_a", "Foo_b"]


def test_field_glob_star_matches_all_available_fields(
    toy_registry: dict[str, Any],
) -> None:
    plan = _plan(toy_registry, [_glob_stage(["*"])], fields=["a", "b", "c"])

    assert _glob_params(plan) == ["a", "b", "c"]


def test_field_glob_question_mark_matches_single_character(
    toy_registry: dict[str, Any],
) -> None:
    plan = _plan(
        toy_registry,
        [_glob_stage(["Muon_pt?"])],
        fields=["Muon_pt1", "Muon_pt10", "Muon_eta", "Muon_pt2"],
    )

    assert _glob_params(plan) == ["Muon_pt1", "Muon_pt2"]


def test_field_glob_overlapping_patterns_do_not_duplicate_fields(
    toy_registry: dict[str, Any],
) -> None:
    plan = _plan(
        toy_registry,
        [_glob_stage(["Foo_*", "*_pt"])],
        fields=["Foo_pt", "Foo_eta", "Bar_pt"],
    )

    assert _glob_params(plan) == ["Foo_pt", "Foo_eta", "Bar_pt"]


def test_field_glob_output_order_follows_input_stream_order(
    toy_registry: dict[str, Any],
) -> None:
    plan = _plan(
        toy_registry,
        [_glob_stage(["Foo_*"])],
        fields=["Foo_b", "Foo_a", "Foo_c"],
    )

    assert _glob_params(plan) == ["Foo_b", "Foo_a", "Foo_c"]


def test_field_glob_can_match_upstream_produced_fields(
    toy_registry: dict[str, Any],
) -> None:
    plan = _plan(
        toy_registry,
        [
            _scale_stage("Produce", source="pt", output="Foo_upstream"),
            _glob_stage(["Foo_*"]),
        ],
        fields=["pt", "Foo_source"],
    )

    assert _glob_params(plan) == ["Foo_source", "Foo_upstream"]


def test_field_glob_does_not_match_unrelated_branch_fields(
    toy_registry: dict[str, Any],
) -> None:
    with pytest.warns(UserWarning, match="Branch_\\*"):
        plan = _plan(
            toy_registry,
            [
                _scale_stage("ProduceBranch", source="pt", output="Branch_field"),
                {
                    **_glob_stage(["Branch_*"]),
                    "from": "events",
                },
            ],
            fields=["pt"],
        )

    assert _glob_params(plan) == []


def test_field_glob_does_not_match_downstream_produced_fields(
    toy_registry: dict[str, Any],
) -> None:
    with pytest.warns(UserWarning, match="Later_\\*"):
        plan = _plan(
            toy_registry,
            [
                _glob_stage(["Later_*"]),
                _scale_stage("ProduceLater", source="pt", output="Later_field"),
            ],
            fields=["pt"],
        )

    assert _glob_params(plan) == []


def test_field_glob_unmatched_pattern_warns_and_expands_to_nothing(
    toy_registry: dict[str, Any],
) -> None:
    with pytest.warns(UserWarning, match="Missing_\\*"):
        plan = _plan(toy_registry, [_glob_stage(["Missing_*"])], fields=["pt"])

    assert _glob_params(plan) == []
    assert _glob_node(plan).meta["compile_hooks"]["fields"][0]["unmatched"] == [
        "Missing_*",
    ]


def test_field_glob_multiple_applicability_contexts_use_active_fields(
    toy_registry: dict[str, Any],
) -> None:
    plan = _plan(
        toy_registry,
        [
            {
                **_scale_stage("ProduceMC", source="pt", output="MC_only"),
                "applies_to": {"eventtype": "mc"},
            },
            {
                **_glob_stage(["MC_*"]),
                "id": "ConsumeMC",
                "applies_to": {"eventtype": "mc"},
            },
        ],
        fields=["pt"],
        datasets=[
            {"name": "mc", "files": ["mc.root"], "eventtype": "mc"},
            {"name": "data", "files": ["data.root"], "eventtype": "data"},
        ],
    )

    assert plan.get_node("stage.ConsumeMC").params["fields"] == ["MC_only"]


def test_field_glob_rejects_different_expansions_across_contexts(
    toy_registry: dict[str, Any],
) -> None:
    with pytest.raises(ValueError, match="expanded differently"):
        _plan(
            toy_registry,
            [
                {
                    **_scale_stage("ProduceMC", source="pt", output="MC_only"),
                    "applies_to": {"eventtype": "mc"},
                },
                {
                    **_glob_stage(["MC_*"]),
                    "id": "ConsumeBoth",
                },
            ],
            fields=["pt"],
            datasets=[
                {"name": "mc", "files": ["mc.root"], "eventtype": "mc"},
                {"name": "data", "files": ["data.root"], "eventtype": "data"},
            ],
        )


def test_field_glob_runtime_params_have_no_wildcards(
    toy_registry: dict[str, Any],
) -> None:
    plan = _plan(toy_registry, [_glob_stage(["Foo_*"])], fields=["Foo_a", "Foo_b"])

    assert plan.get_node("stage.Glob").params["fields"] == ["Foo_a", "Foo_b"]
    assert not any("*" in field for field in plan.get_node("stage.Glob").params["fields"])


def test_plain_list_string_params_without_expand_are_untouched(
    toy_registry: dict[str, Any],
) -> None:
    plan = _plan(
        toy_registry,
        [_glob_stage(["Foo_*"], plain=["Foo_*"])],
        fields=["Foo_a"],
    )

    assert plan.get_node("stage.Glob").params["fields"] == ["Foo_a"]
    assert plan.get_node("stage.Glob").params["plain"] == ["Foo_*"]


def test_expansion_is_driven_solely_by_spec_metadata(
    toy_registry: dict[str, Any],
) -> None:
    plan = _plan(
        toy_registry,
        [
            {
                "id": "Record",
                "op": "toy.record",
                "params": {"source": ["Foo_*"], "output": "out"},
            }
        ],
        fields=["Foo_a"],
    )

    assert plan.get_node("stage.Record").params["source"] == ["Foo_*"]


def test_compiled_representation_exposes_explicit_matches(
    toy_registry: dict[str, Any],
) -> None:
    plan = _plan(toy_registry, [_glob_stage(["Foo_*"])], fields=["Foo_a", "Foo_b"])

    assert _glob_node(plan).meta["compile_hooks"]["fields"] == [
        {
            "hook": "flow.expand_field_glob",
            "against": "input.stream",
            "input": ["Foo_*"],
            "output": ["Foo_a", "Foo_b"],
            "unmatched": [],
        }
    ]


def test_field_glob_hook_execution_occurs_once(
    toy_registry: dict[str, Any],
) -> None:
    plan = _plan(toy_registry, [_glob_stage(["Foo_*"])], fields=["Foo_a"])

    assert len(_glob_node(plan).meta["compile_hooks"]["fields"]) == 1


def test_field_glob_provenance_is_not_compiler_input(
    toy_registry: dict[str, Any],
) -> None:
    plan = _plan(toy_registry, [_glob_stage(["Foo_*"])], fields=["Foo_a"])

    _glob_node(plan).meta["compile_hooks"]["fields"][0]["input"] = ["Bar_*"]
    assert _glob_node(plan).params["fields"] == ["Foo_a"]


def test_unknown_param_compile_hook_fails_clearly(
    toy_registry: dict[str, Any],
) -> None:
    registry = _registry_with_transform(
        toy_registry,
        "toy.unknown_param_hook",
        "tests.toy_components.transforms:TOY_UNKNOWN_PARAM_HOOK_SPEC",
    )

    with pytest.raises(KeyError, match=r"toy\.missing_param_hook"):
        _plan(registry, [_glob_stage(["Foo_*"], op="toy.unknown_param_hook")], fields=["Foo_a"])


def test_malformed_param_compile_hook_declaration_fails_clearly(
    toy_registry: dict[str, Any],
) -> None:
    registry = _registry_with_transform(
        toy_registry,
        "toy.malformed_param_hook",
        "tests.toy_components.transforms:TOY_MALFORMED_PARAM_HOOK_SPEC",
    )

    with pytest.raises(TypeError, match="hooks must be a list"):
        _plan(registry, [_glob_stage(["Foo_*"], op="toy.malformed_param_hook")], fields=["Foo_a"])


def test_multiple_param_compile_hooks_run_left_to_right(
    toy_registry: dict[str, Any],
) -> None:
    registry = _registry_with_transform(
        toy_registry,
        "toy.double_field_glob",
        "tests.toy_components.transforms:TOY_DOUBLE_FIELD_GLOB_SPEC",
    )
    plan = _plan(registry, [_glob_stage(["Foo_*"], op="toy.double_field_glob")], fields=["Foo_a"])

    assert _glob_node(plan).params["fields"] == ["Foo_a"]
    assert [
        record["input"] for record in _glob_node(plan).meta["compile_hooks"]["fields"]
    ] == [["Foo_*"], ["Foo_a"]]


def test_identical_expansions_across_contexts_compile_normally(
    toy_registry: dict[str, Any],
) -> None:
    plan = _plan(
        toy_registry,
        [_glob_stage(["pt"])],
        fields=["pt"],
        datasets=[
            {"name": "mc", "files": ["mc.root"], "eventtype": "mc"},
            {"name": "data", "files": ["data.root"], "eventtype": "data"},
        ],
    )

    assert _glob_params(plan) == ["pt"]


def test_plan_yaml_contains_no_author_glob_for_expanded_param(
    toy_registry: dict[str, Any],
) -> None:
    plan = _plan(toy_registry, [_glob_stage(["Foo_*"])], fields=["Foo_a"])

    serialized = yaml.safe_dump(plan.to_dict(), sort_keys=False)

    assert "Foo_*" not in serialized.split("params:", 1)[1].split("outputs:", 1)[0]
    assert "Foo_a" in serialized


def test_compile_hook_provenance_survives_plan_serialization(
    toy_registry: dict[str, Any],
) -> None:
    plan = _plan(toy_registry, [_glob_stage(["Foo_*"])], fields=["Foo_a"])

    loaded = yaml.safe_load(yaml.safe_dump(plan.to_dict(), sort_keys=False))

    node = next(item for item in loaded["nodes"] if item["id"] == "stage.Glob")
    assert node["meta"]["compile_hooks"]["fields"][0]["input"] == ["Foo_*"]


def test_expand_field_glob_public_contract_does_not_mutate_context() -> None:
    context = ParamCompileHookContext(input_stream_fields=("Foo_a",))

    result = expand_field_glob(
        value=["Foo_*"],
        options={"against": "input.stream"},
        context=context,
    )

    assert result.value == ["Foo_a"]
    assert context.input_stream_fields == ("Foo_a",)


def _plan(
    registry: dict[str, Any],
    stages: list[dict[str, Any]],
    *,
    fields: list[str],
    datasets: list[dict[str, Any]] | None = None,
) -> Any:
    normalized = normalize_workflow(
        {
            "version": "1.0",
            "registry": registry,
            "data": {
                "datasets": datasets
                or [{"name": "sample", "files": ["sample.root"], "eventtype": "mc"}]
            },
            "sources": {
                "events": {
                    "kind": "toy.source",
                    "stream_type": "event_stream",
                    "branches": fields,
                }
            },
            "analysis": {"stages": stages},
        }
    )
    _, plan = build_plan_from_normalized(normalized)
    return plan


def _glob_stage(
    fields: list[str],
    *,
    plain: list[str] | None = None,
    op: str = "toy.field_glob",
) -> dict[str, Any]:
    return {
        "id": "Glob",
        "op": op,
        "params": {
            "fields": fields,
            "plain": list(plain or []),
        },
    }


def _scale_stage(stage_id: str, *, source: str, output: str) -> dict[str, Any]:
    return {
        "id": stage_id,
        "op": "toy.scale",
        "params": {"source": source, "output": output},
    }


def _glob_node(plan: Any) -> Any:
    return plan.get_node("stage.Glob")


def _glob_params(plan: Any) -> list[str]:
    return list(_glob_node(plan).params["fields"])


def _registry_with_transform(
    registry: dict[str, Any],
    name: str,
    spec: str,
) -> dict[str, Any]:
    updated = dict(registry)
    transforms = dict(updated.get("transforms") or {})
    transforms[name] = {
        "spec": spec,
        "impl": "tests.toy_components.transforms:run_toy_field_glob",
    }
    updated["transforms"] = transforms
    return updated
