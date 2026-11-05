from __future__ import annotations

import json
from typing import Any

import pytest

from hepflow.compiler.plan_diff.errors import EmptyScopeError, InvalidPlan
from hepflow.compiler.plan_diff.model import (
    DiffKind,
    DiffSection,
    Index,
    Key,
    PlanDiffEntry,
    PlanDiffReport,
    Selector,
    render_location,
)


def _entry(
    section: DiffSection,
    location: tuple,
    kind: DiffKind = DiffKind.CHANGED,
    **kwargs,
) -> PlanDiffEntry:
    values: dict[str, Any] = {
        "old_present": True,
        "new_present": True,
        "old": 1,
        "new": 2,
    }
    values.update(kwargs)
    return PlanDiffEntry(section=section, kind=kind, location=location, **values)


def _report(*entries: PlanDiffEntry) -> PlanDiffReport:
    return PlanDiffReport(entries, classifier_version=1)


def test_sections_and_kinds_have_public_order_and_values() -> None:
    assert [s.value for s in DiffSection.ordered()] == [
        "graph",
        "data",
        "execution",
        "reports",
        "provenance",
        "unknown",
    ]
    assert [k.value for k in DiffKind] == [
        "added",
        "removed",
        "changed",
        "moved",
        "identity_conflict",
    ]


def test_equal_and_semantic_equal_with_provenance_only_entries() -> None:
    provenance = _entry(DiffSection.PROVENANCE, (Key("provenance"), Key("root")))
    assert not _report(provenance).equal
    assert _report(provenance).semantically_equal
    assert _report().equal
    unknown = _entry(DiffSection.UNKNOWN, (Key("foo"),))
    assert not _report(provenance, unknown).semantically_equal


def test_entries_are_sorted_deterministically_regardless_of_input_order() -> None:
    a = _entry(DiffSection.GRAPH, (Key("nodes"), Selector((("id", "a"),))))
    b = _entry(DiffSection.GRAPH, (Key("nodes"), Selector((("id", "b"),))))
    c = _entry(DiffSection.DATA, (Key("partitions"),))
    d = _entry(DiffSection.PROVENANCE, (Key("provenance"),))
    first = _report(d, b, c, a)
    second = _report(a, c, d, b)
    assert first.entries == second.entries == (a, b, c, d)


def test_counts_are_zero_filled_and_count_entries_not_leaves() -> None:
    added = _entry(
        DiffSection.GRAPH,
        (Key("nodes"), Selector((("id", "x"),))),
        DiffKind.ADDED,
        old_present=False,
        old=None,
        new={"id": "x", "params": {"a": 1, "b": 2}},
    )
    counts = _report(added).counts()
    assert counts["sections"]["graph"] == 1
    assert counts["sections"]["unknown"] == 0
    assert counts["kinds"] == {
        "added": 1,
        "removed": 0,
        "changed": 0,
        "moved": 0,
        "identity_conflict": 0,
    }


def test_to_dict_is_json_serialisable_and_distinguishes_null_from_absent() -> None:
    present_null = _entry(
        DiffSection.GRAPH, (Key("a"),), DiffKind.CHANGED, old=None, new=1
    )
    absent = _entry(
        DiffSection.GRAPH,
        (Key("b"),),
        DiffKind.ADDED,
        old_present=False,
        old=None,
        new=1,
    )
    data = _report(present_null, absent).to_dict()
    json.dumps(data, sort_keys=True)
    by_loc = {e["rendered_location"]: e for e in data["entries"]}
    assert "old" in by_loc["a"] and by_loc["a"]["old"] is None
    assert "old" not in by_loc["b"]
    assert data["report_schema_version"] == 1
    assert data["classifier_version"] == 1
    assert data["scope"] is None
    assert data["warnings"] == []


def test_scoped_applies_include_then_ignore_and_recomputes_equality() -> None:
    graph = _entry(DiffSection.GRAPH, (Key("nodes"),))
    prov = _entry(DiffSection.PROVENANCE, (Key("provenance"),))
    report = _report(graph, prov)
    view = report.scoped(
        include=[DiffSection.GRAPH, DiffSection.PROVENANCE],
        ignore=[DiffSection.GRAPH],
    )
    assert view.entries == (prov,)
    assert view.semantically_equal and not view.equal
    assert view.to_dict()["scope"] == {
        "include": ["graph", "provenance"],
        "ignore": ["graph"],
    }
    assert report.entries == (graph, prov) and report.scope is None


def test_scoped_keeps_warnings() -> None:
    from hepflow.compiler.plan_diff.model import PlanDiffWarning

    warning = PlanDiffWarning(code="w", message="m")
    report = PlanDiffReport([], classifier_version=1, warnings=[warning]).scoped(
        ignore=[DiffSection.UNKNOWN]
    )
    assert report.warnings == (warning,)


def test_scoped_empty_scope_raises_empty_scope_error() -> None:
    with pytest.raises(EmptyScopeError):
        _report().scoped(include=[DiffSection.GRAPH], ignore=[DiffSection.GRAPH])


def test_invalid_plan_carries_code_and_path() -> None:
    error = InvalidPlan("bad value", "nodes[0].id")
    assert error.code == "invalid_plan"
    assert error.path == "nodes[0].id"
    assert "nodes[0].id" in str(error)


def test_render_location() -> None:
    location = (
        Key("nodes"),
        Selector((("id", "n1"), ("part", 0))),
        Key("params"),
        Key("x"),
        Index(2),
    )
    assert render_location(location) == "nodes[id='n1',part=0].params.x[2]"
    assert render_location(()) == "<root>"
