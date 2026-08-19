from __future__ import annotations

from typing import Any

import pytest

from hepflow.compiler.normalize import normalize_workflow


def test_missing_systematics_keeps_existing_normalized_shape(
    toy_workflow: dict[str, Any],
) -> None:
    normalized = normalize_workflow(toy_workflow)

    assert "systematics" not in normalized


def test_empty_systematics_normalizes_cleanly(toy_workflow: dict[str, Any]) -> None:
    workflow = {**toy_workflow, "systematics": {}}

    normalized = normalize_workflow(workflow)

    assert normalized["systematics"] == {
        "include_nominal": False,
        "profiles": [],
        "variations": [],
    }


def test_simple_variation_normalizes(toy_workflow: dict[str, Any]) -> None:
    workflow = {
        **toy_workflow,
        "systematics": {
            "include_nominal": True,
            "profiles": ["CMS_Run3_Defaults"],
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

    normalized = normalize_workflow(workflow)

    assert normalized["systematics"] == {
        "include_nominal": True,
        "profiles": ["CMS_Run3_Defaults"],
        "variations": [
            {
                "name": "trigger_eff_up",
                "group": "trigger_eff",
                "direction": "up",
                "applies_to": {"eventtypes": ["mc"], "datasets": []},
                "requires": ["stage.TriggerEfficiencyWeights"],
                "weight": {"multiply": ["TriggerEffWeight_up"]},
                "replace": {},
                "datasets": {},
            }
        ],
    }


def test_dataset_applicability_normalizes(toy_workflow: dict[str, Any]) -> None:
    workflow = {
        **toy_workflow,
        "systematics": {
            "variations": [
                {
                    "name": "ttbar_pdf_up",
                    "applies_to": {"datasets": ["ttbar"]},
                }
            ],
        },
    }

    normalized = normalize_workflow(workflow)

    variation = normalized["systematics"]["variations"][0]
    assert variation["applies_to"] == {"eventtypes": [], "datasets": ["ttbar"]}


def test_weight_multiply_list_is_preserved(toy_workflow: dict[str, Any]) -> None:
    workflow = {
        **toy_workflow,
        "systematics": {
            "variations": [
                {
                    "name": "combined_weight_up",
                    "weight": {"multiply": ["w1_up", "w2_up"]},
                }
            ],
        },
    }

    normalized = normalize_workflow(workflow)

    variation = normalized["systematics"]["variations"][0]
    assert variation["weight"] == {"multiply": ["w1_up", "w2_up"]}


def test_replace_mapping_is_preserved(toy_workflow: dict[str, Any]) -> None:
    workflow = {
        **toy_workflow,
        "systematics": {
            "variations": [
                {
                    "name": "jes_up",
                    "replace": {"Jet_Pt": "Jet_Pt_JESUp"},
                }
            ],
        },
    }

    normalized = normalize_workflow(workflow)

    variation = normalized["systematics"]["variations"][0]
    assert variation["replace"] == {"Jet_Pt": "Jet_Pt_JESUp"}


def test_duplicate_variation_names_error(toy_workflow: dict[str, Any]) -> None:
    workflow = {
        **toy_workflow,
        "systematics": {
            "variations": [
                {"name": "trigger_eff_up"},
                {"name": "trigger_eff_up"},
            ],
        },
    }

    with pytest.raises(ValueError, match="duplicate systematics variation name"):
        normalize_workflow(workflow)


def test_variation_matrix_expands_in_author_axis_order(
    toy_workflow: dict[str, Any],
) -> None:
    workflow = {
        **toy_workflow,
        "systematics": {
            "variations": [
                {
                    "name": "jec_{source}_{direction}",
                    "mode": "inline",
                    "anchor": "JetJEC",
                    "matrix": {
                        "source": ["Regrouped_Absolute", "Regrouped_BBEC1"],
                        "direction": ["up", "down"],
                    },
                    "group": "jec_{source}",
                    "direction": "{direction}",
                    "metadata": {"source": "{source}", "direction": "{direction}"},
                    "patch": {
                        "params": {
                            "source": "{source}",
                            "direction": "{direction}",
                        }
                    },
                    "export": {
                        "Jet_pt": "Jet_{source}_{direction}_pt",
                    },
                }
            ],
        },
    }

    normalized = normalize_workflow(workflow)

    variations = normalized["systematics"]["variations"]
    assert [variation["name"] for variation in variations] == [
        "jec_Regrouped_Absolute_up",
        "jec_Regrouped_Absolute_down",
        "jec_Regrouped_BBEC1_up",
        "jec_Regrouped_BBEC1_down",
    ]
    assert variations[0]["patch"] == {
        "source": "Regrouped_Absolute",
        "direction": "up",
    }
    assert variations[0]["export"] == {
        "Jet_pt": "Jet_Regrouped_Absolute_up_pt",
    }
    assert variations[0]["metadata"] == {
        "source": "Regrouped_Absolute",
        "direction": "up",
    }
    assert variations[0]["matrix_values"] == {
        "source": "Regrouped_Absolute",
        "direction": "up",
    }
    assert variations[0]["matrix_origin"] == {
        "axes": {
            "source": ["Regrouped_Absolute", "Regrouped_BBEC1"],
            "direction": ["up", "down"],
        }
    }


def test_variation_matrix_preserves_non_string_axis_values_in_exact_templates(
    toy_workflow: dict[str, Any],
) -> None:
    workflow = {
        **toy_workflow,
        "systematics": {
            "variations": [
                {
                    "name": "scale_{factor}",
                    "mode": "inline",
                    "anchor": "Scale",
                    "matrix": {"factor": [1, 2]},
                    "patch": {"factor": "{factor}"},
                }
            ],
        },
    }

    normalized = normalize_workflow(workflow)

    variations = normalized["systematics"]["variations"]
    assert [variation["patch"] for variation in variations] == [
        {"factor": 1},
        {"factor": 2},
    ]


def test_variation_matrix_duplicate_concrete_names_error(
    toy_workflow: dict[str, Any],
) -> None:
    workflow = {
        **toy_workflow,
        "systematics": {
            "variations": [
                {
                    "name": "same",
                    "matrix": {"direction": ["up", "down"]},
                }
            ],
        },
    }

    with pytest.raises(ValueError, match="duplicate systematics variation name"):
        normalize_workflow(workflow)


@pytest.mark.parametrize(
    ("variation", "match"),
    [
        (
            {"name": "bad", "matrix": {}},
            "matrix must define at least one axis",
        ),
        (
            {"name": "bad", "matrix": {"direction": "up"}},
            "matrix.direction must be a list",
        ),
        (
            {"name": "bad", "matrix": {"direction": []}},
            "matrix.direction must be a non-empty list",
        ),
        (
            {"name": "bad_{missing}", "matrix": {"direction": ["up"]}},
            "unknown matrix axis 'missing'",
        ),
        (
            {"name": "bad_{direction!r}", "matrix": {"direction": ["up"]}},
            "must not use format specs",
        ),
    ],
)
def test_malformed_variation_matrix_errors(
    toy_workflow: dict[str, Any], variation: dict[str, Any], match: str
) -> None:
    workflow = {
        **toy_workflow,
        "systematics": {"variations": [variation]},
    }

    with pytest.raises(ValueError, match=match):
        normalize_workflow(workflow)


@pytest.mark.parametrize(
    ("systematics", "match"),
    [
        ([], "systematics must be a mapping"),
        (
            {"include_nominal": "true"},
            "systematics.include_nominal must be a boolean",
        ),
        ({"profiles": "CMS_Run3_Defaults"}, "systematics.profiles must be a list"),
        ({"variations": {}}, "systematics.variations must be a list"),
        ({"variations": [{}]}, "name is required"),
        (
            {"variations": [{"name": "bad", "requires": "stage.Bad"}]},
            "requires must be a list",
        ),
        (
            {"variations": [{"name": "bad", "replace": ["Jet_Pt"]}]},
            "replace must be a mapping",
        ),
        (
            {"variations": [{"name": "bad", "weight": {"multiply": 1}}]},
            "weight.multiply must be a string or list",
        ),
        (
            {"variations": [{"name": "bad", "applies_to": ["mc"]}]},
            "applies_to must be a string or mapping",
        ),
    ],
)
def test_malformed_systematics_errors(
    toy_workflow: dict[str, Any], systematics: Any, match: str
) -> None:
    workflow = {**toy_workflow, "systematics": systematics}

    with pytest.raises(ValueError, match=match):
        normalize_workflow(workflow)
