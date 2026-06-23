from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest
import yaml

from hepflow.api import compile_author_file, run_plan_file
from hepflow.compiler.data_flow import parse_component_data_dependencies
from hepflow.compiler.lower_graph import lower_author_to_graph
from hepflow.compiler.normalize import normalize_author
from hepflow.compiler.plan import build_execution_plan
from hepflow.model.data_flow import DataDependencyResult
from hepflow.model.lifecycle import normalize_lifecycle_event
from hepflow.registry.loaders import load_object
from hepflow.runtime.hooks.manager import HookManager


def test_toy_transform_dependency_parser_tracks_consumed_and_produced_symbols(
    toy_registry: dict[str, Any],
) -> None:
    spec = load_object(
        toy_registry["transforms"]["toy.scale"]["spec"],
    )
    deps = parse_component_data_dependencies(
        spec=spec,
        params={"source": "pt", "output": "scaled_pt"},
        dep_ctx=type(
            "DepCtx",
            (),
            {
                "known_functions": set(),
                "known_constants": set(),
                "context_symbols": set(),
            },
        )(),
    )

    assert deps == DataDependencyResult(consumes={"pt"}, produces={"scaled_pt"})


def test_data_flow_infers_source_requirements_without_requiring_produced_data(
    toy_author_path: Path,
    tmp_path: Path,
) -> None:
    plan = compile_author_file(toy_author_path, outdir=tmp_path / "build")

    assert plan.data_flow["required_sources"]["events"]["branches"] == ["pt"]
    assert plan.data_flow["origins"]["scaled_pt"] == {
        "kind": "produced",
        "node": "stage.Scale",
    }
    assert "scaled_pt" not in plan.data_flow["required_sources"]["events"]["branches"]


def test_data_flow_includes_sink_field_list_requirements(
    toy_author: dict[str, Any],
    tmp_path: Path,
) -> None:
    author = deepcopy(toy_author)
    author["outputs"] = {
        "small": {
            "tree": "events",
            "keep": ["eta", "scaled_pt"],
        }
    }
    author["analysis"]["stages"][0]["write"] = [
        {
            "kind": "toy.write",
            "path": "small.json",
            "use": "small",
        }
    ]
    author_path = tmp_path / "author.yaml"
    author_path.write_text(yaml.safe_dump(author, sort_keys=False), encoding="utf-8")

    plan = compile_author_file(author_path, outdir=tmp_path / "build")

    sink = plan.get_node("write.Scale.0")
    assert sink.params["keep"] == ["eta", "scaled_pt"]
    assert plan.data_flow["required_sources"]["events"]["branches"] == ["eta", "pt"]
    assert plan.data_flow["consumers"]["eta"] == ["write.Scale.0"]
    assert plan.data_flow["consumers"]["scaled_pt"] == ["write.Scale.0"]


@pytest.mark.parametrize(
    "params",
    [
        pytest.param({}, id="absent"),
        pytest.param({"keep": None}, id="none"),
        pytest.param({"keep": []}, id="empty"),
    ],
)
def test_field_list_requirement_allows_no_values(params: dict[str, Any]) -> None:
    spec = {
        "name": "toy.write",
        "kind": "sink",
        "params": {"keep": {"required": False, "default": None}},
        "requires": {
            "symbols": [
                {
                    "from": "params.keep",
                    "kind": "field_list",
                }
            ]
        },
    }

    deps = parse_component_data_dependencies(
        spec=spec,
        params=params,
        dep_ctx=type(
            "DepCtx",
            (),
            {
                "known_functions": set(),
                "known_constants": set(),
                "context_symbols": set(),
            },
        )(),
    )

    assert deps == DataDependencyResult()


def test_expression_requirements_support_wildcards_and_optional_params() -> None:
    spec = {
        "name": "toy.hist",
        "kind": "transform",
        "params": {
            "axes": {"required": True},
            "weight_expr": {"required": False},
        },
        "requires": {
            "symbols": [
                {
                    "from": "params.axes.*.source",
                    "kind": "expr_or_field",
                },
                {
                    "from": "params.weight_expr",
                    "kind": "expr",
                },
            ]
        },
    }

    deps = parse_component_data_dependencies(
        spec=spec,
        params={
            "axes": [
                {"source": "Muon_Pz"},
                {"source": "abs(Muon_Eta)"},
            ],
            "weight_expr": "EventWeight * luminosity",
        },
        dep_ctx=type(
            "DepCtx",
            (),
            {
                "known_functions": {"abs"},
                "known_constants": set(),
                "context_symbols": {"luminosity"},
            },
        )(),
    )

    assert deps == DataDependencyResult(
        consumes={"Muon_Pz", "Muon_Eta", "EventWeight"}
    )


def test_hook_context_outputs_are_visible_to_data_flow(
    tmp_path: Path,
    toy_author: dict[str, Any],
) -> None:
    author = dict(toy_author)
    author["registry"] = {
        **dict(author["registry"]),
        "hooks": {
            "toy.context": {
                "spec": "tests.toy_components.hooks:TOY_CONTEXT_HOOK_SPEC",
                "impl": "tests.toy_components.hooks:ToyContextHook",
            }
        },
    }
    author["execution_hooks"] = [
        {
            "kind": "toy.context",
            "events": ["partition_start"],
        }
    ]
    author["analysis"]["stages"][0]["params"] = {
        "source": "toy_context",
        "output": "from_context",
    }

    author_path = tmp_path / "author.yaml"
    author_path.write_text(yaml.safe_dump(author, sort_keys=False), encoding="utf-8")
    plan = compile_author_file(author_path, outdir=tmp_path / "build")

    assert "events" not in plan.data_flow["required_sources"]
    assert (
        "toy_context" in plan.data_flow["origins"]
        or "from_context" in plan.data_flow["origins"]
    )


def test_hook_executes_lifecycle_event_and_records_summary(
    tmp_path: Path,
    toy_author: dict[str, Any],
) -> None:
    author = dict(toy_author)
    author["registry"] = {
        **dict(author["registry"]),
        "hooks": {
            "toy.context": {
                "spec": "tests.toy_components.hooks:TOY_CONTEXT_HOOK_SPEC",
                "impl": "tests.toy_components.hooks:ToyContextHook",
            }
        },
    }
    author["execution_hooks"] = [
        {
            "kind": "toy.context",
            "events": [
                "partition_start",
                "around_node",
                "before_node",
                "after_node",
                "run_end",
            ],
            "params": {"value": "from-hook"},
        }
    ]
    author_path = tmp_path / "author.yaml"
    author_path.write_text(yaml.safe_dump(author, sort_keys=False), encoding="utf-8")
    build_dir = tmp_path / "build"
    compile_author_file(author_path, outdir=build_dir)
    result = run_plan_file(build_dir / "compile" / "plan.yaml", outdir=build_dir)

    assert result.summary["hooks"]["enabled"][0]["kind"] == "toy.context"
    assert result.summary["hooks"]["enabled"][0]["calls"] > 0


def test_invalid_hook_event_raises(toy_author: dict[str, Any]) -> None:
    author = dict(toy_author)
    author["registry"] = {
        **dict(author["registry"]),
        "hooks": {
            "toy.context": {
                "spec": "tests.toy_components.hooks:TOY_CONTEXT_HOOK_SPEC",
                "impl": "tests.toy_components.hooks:ToyContextHook",
            }
        },
    }
    author["execution_hooks"] = [{"kind": "toy.context", "events": ["not_an_event"]}]
    plan = compile_author_file_from_dict(author)

    with pytest.raises(ValueError, match="does not support event not_an_event"):
        HookManager.from_plan(plan)


def compile_author_file_from_dict(author: dict[str, Any]):

    normalized = normalize_author(author)
    normalized["execution_hooks"] = author.get("execution_hooks", [])
    graph = lower_author_to_graph(normalized)
    return build_execution_plan(
        graph,
        registry=normalized["registry"],
        execution_hooks=normalized["execution_hooks"],
    )


def test_lifecycle_aliases_keep_final_as_run_end() -> None:
    assert normalize_lifecycle_event("final") == "run_end"
