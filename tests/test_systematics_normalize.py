from __future__ import annotations

from typing import Any

import pytest

from hepflow.compiler.normalize import normalize_author


def test_missing_systematics_keeps_existing_normalized_shape(
    toy_author: dict[str, Any],
) -> None:
    normalized = normalize_author(toy_author)

    assert "systematics" not in normalized


def test_empty_systematics_normalizes_cleanly(toy_author: dict[str, Any]) -> None:
    author = {**toy_author, "systematics": {}}

    normalized = normalize_author(author)

    assert normalized["systematics"] == {
        "include_nominal": False,
        "profiles": [],
        "variations": [],
    }


def test_simple_variation_normalizes(toy_author: dict[str, Any]) -> None:
    author = {
        **toy_author,
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

    normalized = normalize_author(author)

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


def test_dataset_applicability_normalizes(toy_author: dict[str, Any]) -> None:
    author = {
        **toy_author,
        "systematics": {
            "variations": [
                {
                    "name": "ttbar_pdf_up",
                    "applies_to": {"datasets": ["ttbar"]},
                }
            ],
        },
    }

    normalized = normalize_author(author)

    variation = normalized["systematics"]["variations"][0]
    assert variation["applies_to"] == {"eventtypes": [], "datasets": ["ttbar"]}


def test_weight_multiply_list_is_preserved(toy_author: dict[str, Any]) -> None:
    author = {
        **toy_author,
        "systematics": {
            "variations": [
                {
                    "name": "combined_weight_up",
                    "weight": {"multiply": ["w1_up", "w2_up"]},
                }
            ],
        },
    }

    normalized = normalize_author(author)

    variation = normalized["systematics"]["variations"][0]
    assert variation["weight"] == {"multiply": ["w1_up", "w2_up"]}


def test_replace_mapping_is_preserved(toy_author: dict[str, Any]) -> None:
    author = {
        **toy_author,
        "systematics": {
            "variations": [
                {
                    "name": "jes_up",
                    "replace": {"Jet_Pt": "Jet_Pt_JESUp"},
                }
            ],
        },
    }

    normalized = normalize_author(author)

    variation = normalized["systematics"]["variations"][0]
    assert variation["replace"] == {"Jet_Pt": "Jet_Pt_JESUp"}


def test_duplicate_variation_names_error(toy_author: dict[str, Any]) -> None:
    author = {
        **toy_author,
        "systematics": {
            "variations": [
                {"name": "trigger_eff_up"},
                {"name": "trigger_eff_up"},
            ],
        },
    }

    with pytest.raises(ValueError, match="duplicate systematics variation name"):
        normalize_author(author)


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
    toy_author: dict[str, Any], systematics: Any, match: str
) -> None:
    author = {**toy_author, "systematics": systematics}

    with pytest.raises(ValueError, match=match):
        normalize_author(author)
