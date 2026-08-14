from __future__ import annotations

from typing import Any

from hepflow.compiler.normalize import normalize_workflow
from hepflow.compiler.plan import build_plan_from_normalized


def test_same_visible_field_name_on_independent_streams_is_scoped(
    toy_registry: dict[str, Any],
) -> None:
    plan = _plan(
        toy_registry,
        [
            {
                "id": "BranchA",
                "op": "toy.scale",
                "from": "events",
                "params": {"source": "raw_a", "output": "Jet_pt"},
            },
            {
                "id": "BranchB",
                "op": "toy.scale",
                "from": "events",
                "params": {"source": "raw_b", "output": "Jet_pt"},
            },
        ],
        fields=[],
    )

    assert plan.data_flow["required_sources"]["events"]["branches"] == [
        "raw_a",
        "raw_b",
    ]
    assert plan.data_flow["origins"]["Jet_pt"]["kind"] == "stream_scoped"
    assert {
        item["origin"]["node"]
        for item in plan.data_flow["origins"]["Jet_pt"]["streams"]
    } == {"stage.BranchA", "stage.BranchB"}


def test_consumer_resolves_field_against_actual_upstream_stream(
    toy_registry: dict[str, Any],
) -> None:
    plan = _plan(
        toy_registry,
        [
            {
                "id": "BranchA",
                "op": "toy.scale",
                "from": "events",
                "params": {"source": "raw_a", "output": "Shared_pt"},
            },
            {
                "id": "UseBranchA",
                "op": "toy.scale",
                "from": "BranchA",
                "params": {"source": "Shared_pt", "output": "A_used"},
            },
        ],
        fields=[],
    )

    assert plan.data_flow["required_sources"]["events"]["branches"] == ["raw_a"]
    assert plan.data_flow["origins"]["A_used"] == {
        "kind": "produced",
        "node": "stage.UseBranchA",
    }


def test_sibling_branch_field_does_not_satisfy_requirement(
    toy_registry: dict[str, Any],
) -> None:
    plan = _plan(
        toy_registry,
        [
            {
                "id": "BranchA",
                "op": "toy.scale",
                "from": "events",
                "params": {"source": "raw_a", "output": "Sibling_only"},
            },
            {
                "id": "BranchB",
                "op": "toy.scale",
                "from": "events",
                "params": {"source": "Sibling_only", "output": "B_used"},
            },
        ],
        fields=[],
    )

    assert plan.data_flow["required_sources"]["events"]["branches"] == [
        "Sibling_only",
        "raw_a",
    ]


def test_input_stream_glob_uses_actual_upstream_stream(
    toy_registry: dict[str, Any],
) -> None:
    plan = _plan(
        toy_registry,
        [
            {
                "id": "BranchA",
                "op": "toy.scale",
                "from": "events",
                "params": {"source": "raw_a", "output": "BranchA_pt"},
            },
            {
                "id": "BranchB",
                "op": "toy.scale",
                "from": "events",
                "params": {"source": "raw_b", "output": "BranchB_pt"},
            },
            {
                "id": "GlobA",
                "op": "toy.field_glob",
                "from": "BranchA",
                "params": {"fields": ["Branch*_pt"]},
            },
        ],
        fields=[],
    )

    assert plan.get_node("stage.GlobA").params["fields"] == ["BranchA_pt"]


def test_dataset_specific_streams_remain_isolated(
    toy_registry: dict[str, Any],
) -> None:
    plan = _plan(
        toy_registry,
        [
            {
                "id": "MCBranch",
                "op": "toy.scale",
                "params": {"source": "mc_raw", "output": "context_pt"},
                "applies_to": {"eventtype": "mc"},
            },
            {
                "id": "DataBranch",
                "op": "toy.scale",
                "params": {"source": "data_raw", "output": "context_pt"},
                "applies_to": {"eventtype": "data"},
            },
        ],
        fields=[],
        datasets=[
            {"name": "data", "files": ["data.root"], "eventtype": "data"},
            {"name": "mc", "files": ["mc.root"], "eventtype": "mc"},
        ],
    )

    assert plan.data_flow["required_sources_by_dataset"] == {
        "data": {"events": {"data": ["data_raw"], "branches": ["data_raw"]}},
        "mc": {"events": {"data": ["mc_raw"], "branches": ["mc_raw"]}},
    }


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
