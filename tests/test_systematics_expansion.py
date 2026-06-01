from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest
import yaml

from hepflow.api import compile_author_file, make_plan_file, run_author_file
from hepflow.compiler.normalize import normalize_author
from hepflow.compiler.systematics import expand_systematics
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


def test_no_systematics_workflows_still_compile_to_existing_plan_path(
    toy_author_path: Path,
    tmp_path: Path,
) -> None:
    build_dir = tmp_path / "build"

    compile_author_file(toy_author_path, outdir=build_dir)

    assert (build_dir / "compile" / "plan.yaml").exists()
    assert not (build_dir / "compile" / "systematics.yaml").exists()


def test_run_author_with_systematics_raises_clear_message(
    toy_author: dict[str, Any],
    tmp_path: Path,
) -> None:
    author_path = _write_author(tmp_path, _author_with_systematics(toy_author))

    with pytest.raises(ValueError, match="running all variations is not implemented"):
        run_author_file(author_path, outdir=tmp_path / "build")


def _author_with_systematics(toy_author: dict[str, Any]) -> dict[str, Any]:
    return {
        **toy_author,
        "systematics": {
            "include_nominal": True,
            "variations": [
                {
                    "name": "trigger_eff_up",
                    "group": "trigger_eff",
                    "direction": "up",
                    "applies_to": "mc",
                    "requires": ["stage.TriggerEfficiencyWeights"],
                    "weight": {"multiply": "TriggerEffWeight_up"},
                }
            ],
        },
    }


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
