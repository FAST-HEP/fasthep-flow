"""The typed read view of a normalized workflow."""

from __future__ import annotations

from typing import Any

import pytest

from hepflow.compiler.normalize import normalize_workflow
from hepflow.compiler.normalized_view import NormalizedView
from hepflow.compiler.systematics import (
    expand_systematics,
    has_separate_plan_systematics,
)


def test_the_view_reads_the_sections_of_a_normalized_workflow(
    toy_workflow: dict[str, Any],
) -> None:
    normalized = normalize_workflow(toy_workflow)
    normalized.update(workflow_path="wf.yaml", project_root="/proj")

    view = NormalizedView.from_mapping(normalized)

    assert [stage["id"] for stage in view.stages] == ["Scale"]
    assert view.registry == normalized["registry"]
    assert (view.workflow_path, view.project_root) == ("wf.yaml", "/proj")
    assert view.systematics is None
    assert view.variation is None


def test_missing_sections_are_empty_not_errors() -> None:
    view = NormalizedView.from_mapping({})

    assert (view.datasets, view.stages, view.reports, view.execution_hooks) == ((), (), (), ())
    assert (view.registry, view.provenance, view.analysis_globals) == ({}, {}, {})
    assert view.dataset_names == []


def test_the_view_does_not_change_the_dict() -> None:
    normalized: dict[str, Any] = {"data": {"datasets": [{"name": "a"}]}}

    view = NormalizedView.from_mapping(normalized)
    view.datasets[0]["name"] = "changed"

    assert normalized == {"data": {"datasets": [{"name": "a"}]}}


@pytest.mark.parametrize(
    ("section", "value", "message"),
    [
        ("data", ["x"], "data must be a mapping"),
        ("analysis", "x", "analysis must be a mapping"),
        ("registry", [], "registry must be a mapping"),
        ("provenance", 3, "provenance must be a mapping"),
        ("reports", {}, "reports must be a list"),
        ("execution_hooks", "x", "execution_hooks must be a list"),
    ],
)
def test_a_section_of_the_wrong_type_names_the_section(
    section: str, value: Any, message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        NormalizedView.from_mapping({section: value})


def test_malformed_systematics_still_fail_in_expansion_only() -> None:
    normalized = {"systematics": ["not", "a", "mapping"]}

    view = NormalizedView.from_mapping(normalized)

    assert view.systematics is None
    assert view.systematics_malformed is True
    assert has_separate_plan_systematics(normalized) is False
    with pytest.raises(ValueError, match="systematics must be a mapping"):
        expand_systematics(normalized)
