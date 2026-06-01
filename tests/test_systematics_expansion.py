from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest
import yaml

from hepflow.api import (
    compile_author_file,
    make_plan_file,
    run_author_file,
    run_plan_file,
)
from hepflow.compiler.normalize import normalize_author
from hepflow.compiler.systematics import expand_systematics
from hepflow.runtime.config import default_run_outdir_for_plan
from hepflow.utils import read_yaml


def test_no_systematics_expands_to_one_nominal_workflow(
    toy_author: dict[str, Any],
) -> None:
    normalized = normalize_author(toy_author)

    expanded = expand_systematics(normalized)

    assert len(expanded) == 1
    assert expanded[0].variation.to_dict() == {"name": "nominal", "is_nominal": True}
    assert expanded[0].workflow["variation"] == {
        "name": "nominal",
        "is_nominal": True,
    }


def test_default_run_outdir_for_plan_handles_compile_layout() -> None:
    assert default_run_outdir_for_plan(Path("build/compile/plan.yaml")) == Path("build")
    assert default_run_outdir_for_plan(Path("build/compile/nominal/plan.yaml")) == Path(
        "build/nominal"
    )
    assert default_run_outdir_for_plan(Path("build/compile/foo_up/plan.yaml")) == Path(
        "build/foo_up"
    )
    assert default_run_outdir_for_plan(Path("other/path/plan.yaml")) == Path(
        "other/path"
    )


def test_include_nominal_expands_nominal_plus_variations(
    toy_author: dict[str, Any],
) -> None:
    normalized = _normalized_with_systematics(
        toy_author,
        include_nominal=True,
        variations=[{"name": "trigger_eff_up", "group": "trigger", "direction": "up"}],
    )

    expanded = expand_systematics(normalized)

    assert [item.variation.name for item in expanded] == ["nominal", "trigger_eff_up"]
    assert [item.variation.is_nominal for item in expanded] == [True, False]


def test_include_nominal_false_expands_variations_only(
    toy_author: dict[str, Any],
) -> None:
    normalized = _normalized_with_systematics(
        toy_author,
        include_nominal=False,
        variations=[
            {"name": "trigger_eff_up"},
            {"name": "trigger_eff_down"},
        ],
    )

    expanded = expand_systematics(normalized)

    assert [item.variation.name for item in expanded] == [
        "trigger_eff_up",
        "trigger_eff_down",
    ]


def test_expanded_workflows_do_not_mutate_original(
    toy_author: dict[str, Any],
) -> None:
    normalized = _normalized_with_systematics(
        toy_author,
        include_nominal=True,
        variations=[{"name": "trigger_eff_up"}],
    )
    before = deepcopy(normalized)

    expanded = expand_systematics(normalized)
    expanded[0].workflow["analysis"]["stages"].append({"id": "extra"})

    assert normalized == before
    assert "variation" not in normalized


def test_variation_metadata_attached_to_expanded_workflow(
    toy_author: dict[str, Any],
) -> None:
    normalized = _normalized_with_systematics(
        toy_author,
        include_nominal=False,
        variations=[
            {
                "name": "ttbar_pdf_up",
                "group": "pdf",
                "direction": "up",
                "applies_to": {"datasets": ["ttbar"]},
                "requires": ["stage.PDFWeights"],
                "weight": {"multiply": ["ttbar_pdf_weight_up"]},
                "replace": {},
                "datasets": {},
            }
        ],
    )

    expanded = expand_systematics(normalized)

    assert expanded[0].workflow["variation"] == {
        "name": "ttbar_pdf_up",
        "group": "pdf",
        "direction": "up",
        "is_nominal": False,
        "applies_to": {"eventtypes": [], "datasets": ["ttbar"]},
        "requires": ["stage.PDFWeights"],
        "weight": {"multiply": ["ttbar_pdf_weight_up"]},
        "replace": {},
        "datasets": {},
    }


def test_nominal_workflow_weight_expr_is_unchanged(toy_author: dict[str, Any]) -> None:
    author = _author_with_systematics(_author_with_weight_expr(toy_author))
    normalized = normalize_author(author)

    nominal = expand_systematics(normalized)[0]

    assert nominal.variation.name == "nominal"
    assert nominal.workflow["analysis"]["stages"][0]["params"]["weight_expr"] == (
        "EventWeight"
    )


def test_single_weight_multiplier_rewrites_stage_weight_expr(
    toy_author: dict[str, Any],
) -> None:
    author = _author_with_systematics(_author_with_weight_expr(toy_author))
    normalized = normalize_author(author)

    variation = expand_systematics(normalized)[1]

    assert variation.workflow["analysis"]["stages"][0]["params"]["weight_expr"] == (
        "(EventWeight) * (TriggerEffWeight_up)"
    )
    assert variation.workflow["variation"]["rewrites"]["weight_expr"] == [
        {
            "stage": "Scale",
            "original": "EventWeight",
            "rewritten": "(EventWeight) * (TriggerEffWeight_up)",
            "multipliers": ["TriggerEffWeight_up"],
        }
    ]


def test_multiple_weight_multipliers_rewrite_stage_weight_expr(
    toy_author: dict[str, Any],
) -> None:
    author = _author_with_systematics(
        _author_with_weight_expr(toy_author),
        multipliers=["TriggerEffWeight_up", "ScaleFactor_up"],
    )
    normalized = normalize_author(author)

    variation = expand_systematics(normalized)[1]

    assert variation.workflow["analysis"]["stages"][0]["params"]["weight_expr"] == (
        "(EventWeight) * (TriggerEffWeight_up) * (ScaleFactor_up)"
    )


def test_stages_without_weight_expr_are_not_weighted(
    toy_author: dict[str, Any],
) -> None:
    normalized = normalize_author(_author_with_systematics(toy_author))

    variation = expand_systematics(normalized)[1]

    params = variation.workflow["analysis"]["stages"][0]["params"]
    assert "weight_expr" not in params
    assert "rewrites" not in variation.workflow["variation"]


def test_weight_rewrite_only_updates_analysis_stages(
    toy_author: dict[str, Any],
) -> None:
    author = _author_with_systematics(_author_with_weight_expr(toy_author))
    author["observers"] = [
        {
            "kind": "toy.observer",
            "at": ["stage.Scale"],
            "params": {"weight_expr": "ObserverWeight"},
        }
    ]
    normalized = normalize_author(author)

    variation = expand_systematics(normalized)[1]

    assert variation.workflow["analysis"]["stages"][0]["params"]["weight_expr"] == (
        "(EventWeight) * (TriggerEffWeight_up)"
    )
    assert variation.workflow["observers"][0]["params"]["weight_expr"] == (
        "ObserverWeight"
    )


def test_field_replacement_rewrites_exact_sources(toy_author: dict[str, Any]) -> None:
    author = _author_with_field_replacements(_author_with_field_params(toy_author))
    normalized = normalize_author(author)

    variation = expand_systematics(normalized)[1]
    params = variation.workflow["analysis"]["stages"][0]["params"]

    assert params["source"] == "Jet_Pt_JESUp"
    assert params["axes"][0]["source"] == "Jet_Eta_JESUp"


def test_field_replacement_rewrites_expressions(toy_author: dict[str, Any]) -> None:
    author = _author_with_field_replacements(_author_with_field_params(toy_author))
    normalized = normalize_author(author)

    variation = expand_systematics(normalized)[1]
    params = variation.workflow["analysis"]["stages"][0]["params"]

    assert params["variables"][0]["expr"] == "Jet_Pt_JESUp > 30"
    assert params["weight_expr"] == "Jet_Pt_JESUp * EventWeight"
    assert {
        "stage": "Scale",
        "original": "Jet_Pt > 30",
        "rewritten": "Jet_Pt_JESUp > 30",
        "replacements": {"Jet_Pt": "Jet_Pt_JESUp"},
    } in variation.workflow["variation"]["rewrites"]["fields"]


def test_field_replacement_avoids_longer_variable_names(
    toy_author: dict[str, Any],
) -> None:
    author = _author_with_field_replacements(_author_with_field_params(toy_author))
    normalized = normalize_author(author)

    variation = expand_systematics(normalized)[1]
    params = variation.workflow["analysis"]["stages"][0]["params"]

    assert params["variables"][1]["expr"] == "Jet_PtRaw > 30"


def test_field_replacement_rewrites_selection_list(
    toy_author: dict[str, Any],
) -> None:
    author = _author_with_field_replacements(_author_with_field_params(toy_author))
    normalized = normalize_author(author)

    variation = expand_systematics(normalized)[1]
    params = variation.workflow["analysis"]["stages"][0]["params"]

    assert params["selection"] == [
        "Jet_Pt_JESUp > 30",
        "abs(Jet_Eta_JESUp) < 2.4",
        "Jet_PtRaw > 30",
    ]


def test_field_replacement_leaves_labels_ids_and_paths_unchanged(
    toy_author: dict[str, Any],
) -> None:
    author = _author_with_field_replacements(_author_with_field_params(toy_author))
    normalized = normalize_author(author)

    variation = expand_systematics(normalized)[1]
    stage = variation.workflow["analysis"]["stages"][0]
    params = stage["params"]

    assert stage["id"] == "Scale"
    assert stage["op"] == "toy.scale"
    assert stage["write"][0]["path"] == "output.json"
    assert params["label"] == "Jet_Pt"
    assert params["out"] == "Jet_Pt"


def test_field_replacement_does_not_mutate_original_workflow(
    toy_author: dict[str, Any],
) -> None:
    normalized = normalize_author(
        _author_with_field_replacements(_author_with_field_params(toy_author))
    )
    before = deepcopy(normalized)

    expand_systematics(normalized)

    assert normalized == before


def test_nominal_workflow_field_params_are_unchanged(
    toy_author: dict[str, Any],
) -> None:
    author = _author_with_field_replacements(_author_with_field_params(toy_author))
    normalized = normalize_author(author)

    nominal = expand_systematics(normalized)[0]
    params = nominal.workflow["analysis"]["stages"][0]["params"]

    assert params["source"] == "Jet_Pt"
    assert params["variables"][0]["expr"] == "Jet_Pt > 30"
    assert "rewrites" not in nominal.workflow["variation"]


def test_dataset_replacement_substitutes_existing_dataset(
    toy_author: dict[str, Any],
) -> None:
    normalized = normalize_author(_author_with_dataset_replacement(toy_author))

    variation = expand_systematics(normalized)[1]
    datasets = variation.workflow["data"]["datasets"]

    assert datasets == [
        {
            "name": "ttbar",
            "files": ["ttbar_hdamp_up.root"],
            "nevents": "4",
            "eventtype": "mc",
            "group": "ttbar",
            "meta": {
                "systematic_replacement": {
                    "nominal_dataset": "ttbar",
                    "replacement_dataset": "ttbar_hdamp_up",
                }
            },
        },
        {
            "name": "wjets",
            "files": ["wjets.root"],
            "nevents": "3",
            "eventtype": "mc",
            "group": "wjets",
            "meta": {},
        },
    ]


def test_dataset_replacement_missing_dataset_raises_clear_error(
    toy_author: dict[str, Any],
) -> None:
    author = _author_with_dataset_replacement(toy_author)
    author["systematics"]["variations"][0]["datasets"]["replace"] = {
        "ttbar": "missing_dataset"
    }
    normalized = normalize_author(author)

    with pytest.raises(
        ValueError,
        match=(
            "Systematic variation 'ttbar_hdamp_up' replaces dataset 'ttbar' "
            "with 'missing_dataset'"
        ),
    ):
        expand_systematics(normalized)


def test_nominal_workflow_dataset_replacement_is_unchanged(
    toy_author: dict[str, Any],
) -> None:
    normalized = normalize_author(_author_with_dataset_replacement(toy_author))

    nominal = expand_systematics(normalized)[0]

    assert [dataset["name"] for dataset in nominal.workflow["data"]["datasets"]] == [
        "ttbar",
        "ttbar_hdamp_up",
        "wjets",
    ]
    assert nominal.workflow["data"]["datasets"][0]["files"] == ["ttbar.root"]


def test_dataset_replacement_only_affects_requested_dataset(
    toy_author: dict[str, Any],
) -> None:
    normalized = normalize_author(_author_with_dataset_replacement(toy_author))

    variation = expand_systematics(normalized)[1]
    datasets_by_name = {
        dataset["name"]: dataset for dataset in variation.workflow["data"]["datasets"]
    }

    assert datasets_by_name["ttbar"]["files"] == ["ttbar_hdamp_up.root"]
    assert datasets_by_name["wjets"]["files"] == ["wjets.root"]


def test_dataset_replacement_keeps_logical_dataset_name(
    toy_author: dict[str, Any],
) -> None:
    normalized = normalize_author(_author_with_dataset_replacement(toy_author))

    variation = expand_systematics(normalized)[1]

    assert [dataset["name"] for dataset in variation.workflow["data"]["datasets"]] == [
        "ttbar",
        "wjets",
    ]


def test_dataset_replacement_metadata_is_recorded(
    toy_author: dict[str, Any],
) -> None:
    normalized = normalize_author(_author_with_dataset_replacement(toy_author))

    variation = expand_systematics(normalized)[1]

    assert variation.workflow["variation"]["datasets"] == {
        "replace": {"ttbar": "ttbar_hdamp_up"}
    }
    assert variation.workflow["variation"]["rewrites"]["datasets"] == [
        {"dataset": "ttbar", "replacement": "ttbar_hdamp_up"}
    ]


def test_dataset_replacement_removes_replacement_only_dataset(
    toy_author: dict[str, Any],
) -> None:
    normalized = normalize_author(_author_with_dataset_replacement(toy_author))

    variation = expand_systematics(normalized)[1]

    assert "ttbar_hdamp_up" not in {
        dataset["name"] for dataset in variation.workflow["data"]["datasets"]
    }


def test_expanded_workflows_preserve_normalized_sections(
    toy_author: dict[str, Any],
) -> None:
    normalized = _normalized_with_systematics(
        toy_author,
        include_nominal=False,
        variations=[{"name": "trigger_eff_up"}],
    )
    normalized["provenance"] = {"registry_layers": []}
    normalized["execution_hooks"] = [{"kind": "toy.hook", "events": ["run_end"]}]
    normalized["styles"] = {"plain": {"color": "black"}}
    normalized["observers"] = [{"kind": "toy.observer", "at": ["stage.Scale"]}]
    normalized["products"] = [{"id": "product"}]

    expanded = expand_systematics(normalized)
    workflow = expanded[0].workflow

    for key in [
        "registry",
        "provenance",
        "execution",
        "execution_hooks",
        "data",
        "sources",
        "analysis",
        "styles",
        "observers",
        "products",
    ]:
        assert workflow[key] == normalized[key]


def test_compile_writes_per_variation_plans_and_summary(
    toy_author: dict[str, Any],
    tmp_path: Path,
) -> None:
    author = _author_with_systematics(toy_author)
    author_path = _write_author(tmp_path, author)
    build_dir = tmp_path / "build"

    compile_author_file(author_path, outdir=build_dir)

    assert not (build_dir / "compile" / "plan.yaml").exists()
    assert (build_dir / "compile" / "nominal" / "normalized.yaml").exists()
    assert (build_dir / "compile" / "nominal" / "plan.yaml").exists()
    assert (build_dir / "compile" / "trigger_eff_up" / "normalized.yaml").exists()
    assert (build_dir / "compile" / "trigger_eff_up" / "plan.yaml").exists()

    summary = read_yaml(build_dir / "compile" / "systematics.yaml")
    assert summary == {
        "include_nominal": True,
        "variations": [
            {
                "name": "nominal",
                "is_nominal": True,
                "plan": "compile/nominal/plan.yaml",
            },
            {
                "name": "trigger_eff_up",
                "group": "trigger_eff",
                "direction": "up",
                "is_nominal": False,
                "applies_to": {"eventtypes": ["mc"], "datasets": []},
                "requires": ["stage.TriggerEfficiencyWeights"],
                "weight": {"multiply": ["TriggerEffWeight_up"]},
                "replace": {},
                "datasets": {},
                "plan": "compile/trigger_eff_up/plan.yaml",
            },
        ],
    }


def test_each_variation_plan_contains_variation_metadata(
    toy_author: dict[str, Any],
    tmp_path: Path,
) -> None:
    author_path = _write_author(tmp_path, _author_with_systematics(toy_author))
    build_dir = tmp_path / "build"

    compile_author_file(author_path, outdir=build_dir)

    nominal_plan = read_yaml(build_dir / "compile" / "nominal" / "plan.yaml")
    variation_plan = read_yaml(build_dir / "compile" / "trigger_eff_up" / "plan.yaml")

    assert nominal_plan["context"]["variation"] == {
        "name": "nominal",
        "is_nominal": True,
    }
    assert variation_plan["context"]["variation"] == {
        "name": "trigger_eff_up",
        "group": "trigger_eff",
        "direction": "up",
        "is_nominal": False,
        "applies_to": {"eventtypes": ["mc"], "datasets": []},
        "requires": ["stage.TriggerEfficiencyWeights"],
        "weight": {"multiply": ["TriggerEffWeight_up"]},
        "replace": {},
        "datasets": {},
    }


def test_variation_plan_contains_rewritten_weight_expr(
    toy_author: dict[str, Any],
    tmp_path: Path,
) -> None:
    author_path = _write_author(
        tmp_path, _author_with_systematics(_author_with_weight_expr(toy_author))
    )
    build_dir = tmp_path / "build"

    compile_author_file(author_path, outdir=build_dir)

    variation_plan = read_yaml(build_dir / "compile" / "trigger_eff_up" / "plan.yaml")
    stage_node = _plan_node(variation_plan, "stage.Scale")
    assert stage_node["params"]["weight_expr"] == (
        "(EventWeight) * (TriggerEffWeight_up)"
    )


def test_nominal_plan_contains_original_weight_expr(
    toy_author: dict[str, Any],
    tmp_path: Path,
) -> None:
    author_path = _write_author(
        tmp_path, _author_with_systematics(_author_with_weight_expr(toy_author))
    )
    build_dir = tmp_path / "build"

    compile_author_file(author_path, outdir=build_dir)

    nominal_plan = read_yaml(build_dir / "compile" / "nominal" / "plan.yaml")
    stage_node = _plan_node(nominal_plan, "stage.Scale")
    assert stage_node["params"]["weight_expr"] == "EventWeight"


def test_variation_plan_contains_rewritten_field_params(
    toy_author: dict[str, Any],
    tmp_path: Path,
) -> None:
    author_path = _write_author(
        tmp_path, _author_with_field_replacements(_author_with_field_params(toy_author))
    )
    build_dir = tmp_path / "build"

    compile_author_file(author_path, outdir=build_dir)

    variation_plan = read_yaml(build_dir / "compile" / "jes_up" / "plan.yaml")
    stage_node = _plan_node(variation_plan, "stage.Scale")
    assert stage_node["params"]["source"] == "Jet_Pt_JESUp"
    assert stage_node["params"]["variables"][0]["expr"] == "Jet_Pt_JESUp > 30"
    assert stage_node["params"]["selection"][1] == "abs(Jet_Eta_JESUp) < 2.4"
    assert variation_plan["context"]["variation"]["replace"] == {
        "Jet_Pt": "Jet_Pt_JESUp",
        "Jet_Eta": "Jet_Eta_JESUp",
    }


def test_variation_plan_contains_dataset_replacement_metadata(
    toy_author: dict[str, Any],
    tmp_path: Path,
) -> None:
    author_path = _write_author(tmp_path, _author_with_dataset_replacement(toy_author))
    build_dir = tmp_path / "build"

    compile_author_file(author_path, outdir=build_dir)

    variation_plan = read_yaml(build_dir / "compile" / "ttbar_hdamp_up" / "plan.yaml")
    datasets = variation_plan["context"]["datasets"]
    assert sorted(datasets) == ["ttbar", "wjets"]
    assert datasets["ttbar"]["files"] == ["ttbar_hdamp_up.root"]
    assert datasets["ttbar"]["meta"]["systematic_replacement"] == {
        "nominal_dataset": "ttbar",
        "replacement_dataset": "ttbar_hdamp_up",
    }
    assert variation_plan["context"]["variation"]["datasets"] == {
        "replace": {"ttbar": "ttbar_hdamp_up"}
    }
    assert variation_plan["context"]["variation"]["rewrites"]["datasets"] == [
        {"dataset": "ttbar", "replacement": "ttbar_hdamp_up"}
    ]


def test_make_plan_file_expands_normalized_systematics(
    toy_author: dict[str, Any],
    tmp_path: Path,
) -> None:
    author = _author_with_systematics(toy_author)
    normalized = normalize_author(author)
    normalized_path = tmp_path / "normalized.yaml"
    normalized_path.write_text(
        yaml.safe_dump(normalized, sort_keys=False), encoding="utf-8"
    )
    build_dir = tmp_path / "build"

    make_plan_file(normalized_path, outdir=build_dir)

    assert (build_dir / "compile" / "systematics.yaml").exists()
    assert (build_dir / "compile" / "trigger_eff_up" / "plan.yaml").exists()


def test_run_plan_file_uses_variation_outdir_by_default(
    toy_author: dict[str, Any],
    tmp_path: Path,
) -> None:
    author_path = _write_author(tmp_path, _author_with_systematics(toy_author))
    build_dir = tmp_path / "build"
    compile_author_file(author_path, outdir=build_dir)

    result = run_plan_file(build_dir / "compile" / "trigger_eff_up" / "plan.yaml")

    assert result.success is True
    assert (build_dir / "trigger_eff_up" / "run_summary.yaml").exists()
    assert (build_dir / "trigger_eff_up" / "artifacts").exists()
    assert not (build_dir / "run_summary.yaml").exists()


def test_run_plan_file_explicit_outdir_overrides_variation_default(
    toy_author: dict[str, Any],
    tmp_path: Path,
) -> None:
    author_path = _write_author(tmp_path, _author_with_systematics(toy_author))
    build_dir = tmp_path / "build"
    custom_dir = tmp_path / "custom"
    compile_author_file(author_path, outdir=build_dir)

    result = run_plan_file(
        build_dir / "compile" / "trigger_eff_up" / "plan.yaml",
        outdir=custom_dir,
    )

    assert result.success is True
    assert (custom_dir / "run_summary.yaml").exists()
    assert not (build_dir / "trigger_eff_up" / "run_summary.yaml").exists()


def test_no_systematics_workflows_still_compile_to_existing_plan_path(
    toy_author_path: Path,
    tmp_path: Path,
) -> None:
    build_dir = tmp_path / "build"

    compile_author_file(toy_author_path, outdir=build_dir)

    assert (build_dir / "compile" / "plan.yaml").exists()
    assert not (build_dir / "compile" / "systematics.yaml").exists()


def test_run_author_with_systematics_runs_nominal_if_present(
    toy_author: dict[str, Any],
    tmp_path: Path,
) -> None:
    author_path = _write_author(tmp_path, _author_with_systematics(toy_author))
    build_dir = tmp_path / "build"

    result = run_author_file(author_path, outdir=build_dir)

    assert result.success is True
    assert (build_dir / "nominal" / "run_summary.yaml").exists()
    assert not (build_dir / "trigger_eff_up" / "run_summary.yaml").exists()


def test_run_author_with_systematics_without_nominal_raises_clear_message(
    toy_author: dict[str, Any],
    tmp_path: Path,
) -> None:
    author_path = _write_author(
        tmp_path,
        _author_with_systematics(toy_author, include_nominal=False),
    )

    with pytest.raises(ValueError, match="no nominal variation was generated"):
        run_author_file(author_path, outdir=tmp_path / "build")


def _author_with_systematics(
    toy_author: dict[str, Any],
    *,
    include_nominal: bool = True,
    multipliers: list[str] | None = None,
) -> dict[str, Any]:
    return {
        **toy_author,
        "systematics": {
            "include_nominal": include_nominal,
            "variations": [
                {
                    "name": "trigger_eff_up",
                    "group": "trigger_eff",
                    "direction": "up",
                    "applies_to": "mc",
                    "requires": ["stage.TriggerEfficiencyWeights"],
                    "weight": {"multiply": multipliers or "TriggerEffWeight_up"},
                }
            ],
        },
    }


def _author_with_weight_expr(toy_author: dict[str, Any]) -> dict[str, Any]:
    author = deepcopy(toy_author)
    author["analysis"]["stages"][0]["params"]["weight_expr"] = "EventWeight"
    return author


def _author_with_field_replacements(toy_author: dict[str, Any]) -> dict[str, Any]:
    return {
        **toy_author,
        "systematics": {
            "include_nominal": True,
            "variations": [
                {
                    "name": "jes_up",
                    "group": "jes",
                    "direction": "up",
                    "replace": {
                        "Jet_Pt": "Jet_Pt_JESUp",
                        "Jet_Eta": "Jet_Eta_JESUp",
                    },
                }
            ],
        },
    }


def _author_with_field_params(toy_author: dict[str, Any]) -> dict[str, Any]:
    author = deepcopy(toy_author)
    author["analysis"]["stages"][0]["params"].update(
        {
            "source": "Jet_Pt",
            "variables": [
                {"name": "pt_pass", "expr": "Jet_Pt > 30"},
                {"name": "raw_pass", "expr": "Jet_PtRaw > 30"},
            ],
            "weight_expr": "Jet_Pt * EventWeight",
            "selection": [
                "Jet_Pt > 30",
                "abs(Jet_Eta) < 2.4",
                "Jet_PtRaw > 30",
            ],
            "axes": [
                {"name": "eta", "source": "Jet_Eta"},
                {"name": "raw_pt", "source": "Jet_PtRaw"},
            ],
            "label": "Jet_Pt",
            "out": "Jet_Pt",
        }
    )
    return author


def _author_with_dataset_replacement(toy_author: dict[str, Any]) -> dict[str, Any]:
    author = deepcopy(toy_author)
    author["data"] = {
        "datasets": [
            {"name": "ttbar", "files": ["ttbar.root"], "nevents": 4},
            {
                "name": "ttbar_hdamp_up",
                "files": ["ttbar_hdamp_up.root"],
                "nevents": 4,
            },
            {"name": "wjets", "files": ["wjets.root"], "nevents": 3},
        ]
    }
    author["systematics"] = {
        "include_nominal": True,
        "variations": [
            {
                "name": "ttbar_hdamp_up",
                "group": "hdamp",
                "direction": "up",
                "applies_to": {"datasets": ["ttbar"]},
                "datasets": {"replace": {"ttbar": "ttbar_hdamp_up"}},
            }
        ],
    }
    return author


def _normalized_with_systematics(
    toy_author: dict[str, Any],
    *,
    include_nominal: bool,
    variations: list[dict[str, Any]],
) -> dict[str, Any]:
    author = {
        **toy_author,
        "systematics": {
            "include_nominal": include_nominal,
            "variations": variations,
        },
    }
    return normalize_author(author)


def _write_author(tmp_path: Path, author: dict[str, Any]) -> Path:
    author_path = tmp_path / "author.yaml"
    author_path.write_text(yaml.safe_dump(author, sort_keys=False), encoding="utf-8")
    return author_path


def _plan_node(plan: dict[str, Any], node_id: str) -> dict[str, Any]:
    return next(node for node in plan["nodes"] if node["id"] == node_id)
