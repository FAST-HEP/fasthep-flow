from __future__ import annotations

from copy import deepcopy
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


def test_source_establishes_symbolic_lineage(toy_registry: dict[str, Any]) -> None:
    plan = _plan(toy_registry, [], fields=["pt"])

    assert _lineage(plan, "read.events") == "source:read.events:stream"


def test_preserving_transform_chain_retains_source_lineage(
    toy_registry: dict[str, Any],
) -> None:
    plan = _plan(
        toy_registry,
        [
            {
                "id": "Scale",
                "op": "toy.scale",
                "params": {"source": "pt", "output": "scaled_pt"},
            },
            {
                "id": "Record",
                "op": "toy.record",
                "params": {"source": "scaled_pt", "output": "recorded_pt"},
            },
        ],
        fields=["pt"],
    )

    assert _lineage(plan, "stage.Scale") == _lineage(plan, "read.events")
    assert _lineage(plan, "stage.Record") == _lineage(plan, "read.events")


def test_sibling_branches_from_one_stream_share_lineage(
    toy_registry: dict[str, Any],
) -> None:
    plan = _plan(
        toy_registry,
        [
            {
                "id": "BranchA",
                "op": "toy.scale",
                "from": "events",
                "params": {"source": "a", "output": "a_out"},
            },
            {
                "id": "BranchB",
                "op": "toy.scale",
                "from": "events",
                "params": {"source": "b", "output": "b_out"},
            },
        ],
        fields=["a", "b"],
    )

    assert _lineage(plan, "stage.BranchA") == _lineage(plan, "read.events")
    assert _lineage(plan, "stage.BranchB") == _lineage(plan, "read.events")


def test_independent_sources_do_not_share_lineage(toy_registry: dict[str, Any]) -> None:
    plan = _plan(
        toy_registry,
        [
            {
                "id": "UseEvents",
                "op": "toy.scale",
                "from": "events",
                "params": {"source": "pt", "output": "events_pt"},
            },
            {
                "id": "UseOther",
                "op": "toy.scale",
                "from": "other",
                "params": {"source": "pt", "output": "other_pt"},
            },
        ],
        fields=[],
        sources={
            "events": _toy_source(["pt"]),
            "other": _toy_source(["pt"]),
        },
    )

    assert _lineage(plan, "read.events") != _lineage(plan, "read.other")
    assert _lineage(plan, "stage.UseEvents") == _lineage(plan, "read.events")
    assert _lineage(plan, "stage.UseOther") == _lineage(plan, "read.other")


def test_field_names_do_not_determine_lineage(toy_registry: dict[str, Any]) -> None:
    plan = _plan(
        toy_registry,
        [
            {
                "id": "EventsSharedName",
                "op": "toy.scale",
                "from": "events",
                "params": {"source": "pt", "output": "Shared_pt"},
            },
            {
                "id": "OtherSharedName",
                "op": "toy.scale",
                "from": "other",
                "params": {"source": "pt", "output": "Shared_pt"},
            },
        ],
        fields=[],
        sources={
            "events": _toy_source(["pt"]),
            "other": _toy_source(["pt"]),
        },
    )

    assert _lineage(plan, "stage.EventsSharedName") != _lineage(
        plan,
        "stage.OtherSharedName",
    )


def test_spec_can_request_new_event_stream_lineage(toy_registry: dict[str, Any]) -> None:
    registry = deepcopy(toy_registry)
    registry["transforms"]["toy.new_lineage"] = {
        "spec": "tests.toy_components.transforms:TOY_NEW_LINEAGE_SPEC",
        "impl": "tests.toy_components.transforms:run_toy_scale",
    }
    plan = _plan(
        registry,
        [
            {
                "id": "Preserve",
                "op": "toy.scale",
                "params": {"source": "pt", "output": "preserved_pt"},
            },
            {
                "id": "NewLineage",
                "op": "toy.new_lineage",
                "params": {"source": "preserved_pt", "output": "new_pt"},
            },
            {
                "id": "After",
                "op": "toy.record",
                "params": {"source": "new_pt", "output": "after_pt"},
            },
        ],
        fields=["pt"],
    )

    assert _lineage(plan, "stage.Preserve") == _lineage(plan, "read.events")
    assert _lineage(plan, "stage.NewLineage") == "stream:stage.NewLineage:stream"
    assert _lineage(plan, "stage.After") == _lineage(plan, "stage.NewLineage")


def _plan(
    registry: dict[str, Any],
    stages: list[dict[str, Any]],
    *,
    fields: list[str],
    datasets: list[dict[str, Any]] | None = None,
    sources: dict[str, Any] | None = None,
) -> Any:
    normalized = normalize_workflow(
        {
            "version": "1.0",
            "registry": registry,
            "data": {
                "datasets": datasets
                or [{"name": "sample", "files": ["sample.root"], "eventtype": "mc"}]
            },
            "sources": sources or {"events": _toy_source(fields)},
            "analysis": {"stages": stages},
        }
    )
    _, plan = build_plan_from_normalized(normalized)
    return plan


def _toy_source(fields: list[str]) -> dict[str, Any]:
    return {
        "kind": "toy.source",
        "stream_type": "event_stream",
        "branches": fields,
    }


def _lineage(plan: Any, node_id: str) -> str:
    return plan.data_flow["_stream_lineage"][f"{node_id}:stream"]["identity"]
