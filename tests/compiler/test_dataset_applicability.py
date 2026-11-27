from __future__ import annotations

from copy import deepcopy
from typing import Any

import pytest
from tests.builders import modified_stage

from hepflow.compiler.lower_graph import lower_workflow_to_graph
from hepflow.compiler.normalize import normalize_workflow
from hepflow.compiler.plan import build_plan_from_normalized
from hepflow.model.applicability import (
    applicability_is_empty,
    intersect_applicability,
    node_applies_to_dataset,
)
from hepflow.model.plan_applicability import active_plan_nodes_for_dataset
from hepflow.runtime.engine import build_partition_context, execute_plan_partition


@pytest.mark.parametrize("eventtype", ["data", "mc"])
def test_stage_applies_to_eventtype_normalizes(
    toy_workflow: dict[str, Any],
    eventtype: str,
) -> None:
    workflow = modified_stage(toy_workflow, applies_to={"eventtype": eventtype})

    normalized = normalize_workflow(workflow)

    assert normalized["analysis"]["stages"][0]["applies_to"] == {"eventtype": eventtype}


@pytest.mark.parametrize(
    ("applies_to", "match"),
    [
        ({"eventtype": "signal"}, "only supports"),
        ({"eventtypes": ["mc"]}, "only supports eventtype"),
        ("mc", "must be a mapping"),
    ],
)
def test_stage_applies_to_rejects_unsupported_shapes(
    toy_workflow: dict[str, Any],
    applies_to: Any,
    match: str,
) -> None:
    workflow = modified_stage(toy_workflow, applies_to=applies_to)

    with pytest.raises(ValueError, match=match):
        normalize_workflow(workflow)


def test_applicability_intersection_unrestricted_with_mc() -> None:
    assert intersect_applicability(
        None,
        {"eventtypes": ["mc"], "datasets": []},
    ) == {"eventtypes": ["mc"]}


def test_applicability_intersection_mc_with_mc() -> None:
    assert intersect_applicability(
        {"eventtype": "mc"},
        {"eventtypes": ["mc"]},
    ) == {"eventtypes": ["mc"]}


def test_applicability_intersection_data_with_mc_is_empty() -> None:
    result = intersect_applicability(
        {"eventtype": "data"},
        {"eventtypes": ["mc"]},
    )

    assert applicability_is_empty(result)


def test_applicability_intersection_datasets() -> None:
    assert intersect_applicability(
        {"datasets": ["ttbar", "dy"]},
        {"datasets": ["dy", "wjets"]},
    ) == {"datasets": ["dy"]}


def test_applicability_intersection_eventtype_and_dataset_combination() -> None:
    assert intersect_applicability(
        {"eventtype": "mc", "datasets": ["ttbar", "dy"]},
        {"eventtypes": ["mc"], "datasets": ["dy"]},
    ) == {"eventtypes": ["mc"], "datasets": ["dy"]}


def test_active_plan_nodes_omit_mc_only_nodes_for_data(
    toy_workflow: dict[str, Any],
) -> None:
    workflow = _applicability_workflow(toy_workflow)
    _graph, plan = build_plan_from_normalized(normalize_workflow(workflow))

    data_nodes = [
        node.id
        for node in active_plan_nodes_for_dataset(
            plan,
            dataset={"name": "data", "eventtype": "data"},
        )
    ]
    mc_nodes = [
        node.id
        for node in active_plan_nodes_for_dataset(
            plan,
            dataset={"name": "ttbar", "eventtype": "mc"},
        )
    ]

    assert data_nodes == ["read.events", "stage.Common", "stage.Final"]
    assert mc_nodes == [
        "read.events",
        "stage.Common",
        "stage.MCOnly",
        "stage.Final",
    ]


def test_runtime_skips_mc_only_linear_stage_for_data(
    toy_workflow: dict[str, Any],
) -> None:
    workflow = _applicability_workflow(toy_workflow)
    _graph, plan = build_plan_from_normalized(normalize_workflow(workflow))
    data_partition = next(
        partition for partition in plan.partitions if partition.dataset == "data"
    )
    mc_partition = next(
        partition for partition in plan.partitions if partition.dataset == "ttbar"
    )

    data_store = execute_plan_partition(
        plan,
        ctx=build_partition_context(
            plan,
            base_ctx=plan.context,
            partition=data_partition,
        ),
    )
    mc_store = execute_plan_partition(
        plan,
        ctx=build_partition_context(
            plan,
            base_ctx=plan.context,
            partition=mc_partition,
        ),
    )

    assert ("stage.MCOnly", "stream") not in data_store
    assert ("stage.MCOnly", "stream") in mc_store
    assert data_store[("stage.Final", "stream")]["final_pt"] == [24, 36, 42, 56]
    assert mc_store[("stage.Final", "stream")]["final_pt"] == [24, 36, 42, 56]


def test_active_plan_nodes_omit_data_only_nodes_for_mc(
    toy_workflow: dict[str, Any],
) -> None:
    workflow = _data_only_workflow(toy_workflow)
    _graph, plan = build_plan_from_normalized(normalize_workflow(workflow))

    data_nodes = [
        node.id
        for node in active_plan_nodes_for_dataset(
            plan,
            dataset={"name": "data", "eventtype": "data"},
        )
    ]
    mc_nodes = [
        node.id
        for node in active_plan_nodes_for_dataset(
            plan,
            dataset={"name": "ttbar", "eventtype": "mc"},
        )
    ]

    assert data_nodes == [
        "read.events",
        "stage.Common",
        "stage.DataOnly",
        "stage.Final",
    ]
    assert mc_nodes == ["read.events", "stage.Common", "stage.Final"]


def test_data_flow_routes_mc_only_branches_only_to_mc_datasets(
    toy_workflow: dict[str, Any],
) -> None:
    workflow = deepcopy(toy_workflow)
    workflow["data"] = {
        "datasets": [
            {"name": "data", "eventtype": "data", "files": ["data.root"]},
            {"name": "ttbar", "eventtype": "mc", "files": ["ttbar.root"]},
        ]
    }
    workflow["analysis"]["stages"] = [
        {
            "id": "MuonPt",
            "op": "toy.scale",
            "params": {"source": "Muon_Px", "output": "Muon_Pt"},
        },
        {
            "id": "MCLeptonPt",
            "op": "toy.scale",
            "params": {"source": "MCLepton_Px", "output": "MCLepton_Pt"},
            "applies_to": {"eventtype": "mc"},
        },
    ]

    _graph, plan = build_plan_from_normalized(normalize_workflow(workflow))
    source = plan.get_node("read.events")

    assert source.params["branches_by_dataset"] == {
        "data": ["Muon_Px"],
        "ttbar": ["MCLepton_Px", "Muon_Px"],
    }
    assert "branches" not in source.params


def test_unsupported_applicability_bypass_raises_clear_error(
    toy_workflow: dict[str, Any],
) -> None:
    workflow = deepcopy(toy_workflow)
    workflow["data"] = {
        "datasets": [
            {"name": "data", "eventtype": "data", "files": ["data.root"]},
            {"name": "ttbar", "eventtype": "mc", "files": ["ttbar.root"]},
        ]
    }
    workflow["analysis"]["stages"] = [
        {
            "id": "MCOnlyHist",
            "op": "hep.hist",
            "params": {
                "name": "mc_only",
                "axes": [
                    {
                        "name": "x",
                        "source": "pt",
                        "bins": {"n": 10, "low": 0, "high": 100},
                    }
                ],
            },
            "applies_to": {"eventtype": "mc"},
        },
        {
            "id": "Render",
            "op": "hep.render.histogram",
            "from": [{"node": "MCOnlyHist", "port": "hist"}],
            "params": {"style": {"op": "hep.render.histogram"}},
        },
    ]

    with pytest.raises(ValueError, match="output is not an event_stream"):
        build_plan_from_normalized(normalize_workflow(workflow))


def test_run_end_sink_can_consume_data_and_mc_specific_products(
    toy_workflow: dict[str, Any],
) -> None:
    workflow = deepcopy(toy_workflow)
    workflow["registry"]["sinks"]["hep.render.comparison"] = {
        "spec": "tests.toy_components.sinks:TOY_COMPARISON_SINK_SPEC",
        "impl": "tests.toy_components.sinks:run_toy_write",
    }
    workflow["data"] = {
        "datasets": [
            {"name": "data", "eventtype": "data", "files": ["data.root"]},
            {"name": "ttbar", "eventtype": "mc", "files": ["ttbar.root"]},
        ]
    }
    workflow["analysis"]["stages"] = [
        {
            "id": "DataProduct",
            "op": "toy.scale",
            "params": {"source": "pt", "output": "data_pt"},
            "applies_to": {"eventtype": "data"},
        },
        {
            "id": "MCProduct",
            "op": "toy.scale",
            "params": {"source": "pt", "output": "mc_pt"},
            "applies_to": {"eventtype": "mc"},
        },
        {
            "id": "Compare",
            "op": "hep.render.comparison",
            "from": [
                {"node": "DataProduct", "port": "stream", "as": "reference"},
                {"node": "MCProduct", "port": "stream", "as": "target"},
            ],
            "when": "final",
            "out": "compare.json",
            "params": {
                "style": {
                    "op": "hep.render.comparison",
                    "comparison": {"reference": "reference", "target": "target"},
                }
            },
        },
    ]

    _graph, plan = build_plan_from_normalized(normalize_workflow(workflow))

    assert plan.get_node("render.Compare.0").params["when"] == "run_end"


def test_hist_variations_add_explicit_variation_axis(toy_workflow: dict[str, Any]) -> None:
    workflow = deepcopy(toy_workflow)
    workflow["analysis"]["stages"] = [
        {
            "id": "PV_npvs",
            "op": "hep.hist",
            "params": {
                "dataset_axis": True,
                "axes": [
                    {
                        "name": "PV_npvs",
                        "source": "PV_npvs",
                        "type": "regular",
                        "bins": {"low": 0, "high": 100, "nbins": 50},
                    }
                ],
                "variations": {
                    "axis": "variation",
                    "apply_to": {"eventtype": "mc"},
                    "weights": {
                        "nominal": "weight_pu_nominal",
                        "up": "weight_pu_up",
                        "down": "weight_pu_down",
                    },
                },
            },
        }
    ]

    graph = lower_workflow_to_graph(normalize_workflow(workflow))
    node = graph.nodes["stage.PV_npvs"]["payload"]

    assert node.params["storage"] == "weighted"
    assert node.params["axes"] == [
        {
            "name": "dataset",
            "type": "category",
            "source": "dataset_name",
            "bins": None,
        },
        {
            "name": "PV_npvs",
            "source": "PV_npvs",
            "type": "regular",
            "bins": {"low": 0, "high": 100, "nbins": 50},
        },
        {
            "name": "variation",
            "type": "category",
            "source": "__variation__",
            "bins": ["nominal", "up", "down"],
        },
    ]


def test_render_variations_expand_to_explicit_render_nodes(
    toy_workflow: dict[str, Any],
) -> None:
    workflow = deepcopy(toy_workflow)
    workflow["analysis"]["stages"] = [
        {
            "id": "PV_npvs",
            "op": "hep.hist",
            "params": {
                "dataset_axis": True,
                "axes": [
                    {
                        "name": "PV_npvs",
                        "source": "PV_npvs",
                        "type": "regular",
                        "bins": {"low": 0, "high": 100, "nbins": 50},
                    }
                ],
                "variations": {
                    "axis": "variation",
                    "apply_to": {"eventtype": "mc"},
                    "weights": {
                        "nominal": "weight_pu_nominal",
                        "up": "weight_pu_up",
                    },
                },
            },
        },
        {
            "id": "RenderPV_npvs",
            "op": "hep.render.comparison",
            "from": "PV_npvs",
            "out": "debug/PV_npvs_{variation}.png",
            "params": {"style": {"op": "hep.render.comparison"}},
            "variations": {
                "axis": "variation",
                "values": ["nominal", "up"],
                "reference": "nominal",
            },
        },
    ]

    graph = lower_workflow_to_graph(normalize_workflow(workflow))

    assert "render.RenderPV_npvs_nominal.0" in graph.nodes
    assert "render.RenderPV_npvs_up.0" in graph.nodes
    nominal = graph.nodes["render.RenderPV_npvs_nominal.0"]["payload"]
    up = graph.nodes["render.RenderPV_npvs_up.0"]["payload"]
    assert nominal.params["out"] == "debug/PV_npvs_nominal.png"
    assert up.params["out"] == "debug/PV_npvs_up.png"
    assert up.params["spec"]["comparison"]["variation_axis"] == "variation"
    assert up.params["spec"]["comparison"]["variation"] == "up"
    assert up.params["spec"]["comparison"]["variation_reference"] == "nominal"


def test_lowered_graph_records_applicability_metadata(
    toy_workflow: dict[str, Any],
) -> None:
    workflow = _applicability_workflow(toy_workflow)
    graph = lower_workflow_to_graph(normalize_workflow(workflow))

    payload = graph.nodes["stage.MCOnly"]["payload"]

    assert payload.meta["applies_to"] == {"eventtype": "mc"}


def _applicability_workflow(toy_workflow: dict[str, Any]) -> dict[str, Any]:
    workflow = deepcopy(toy_workflow)
    workflow["data"] = {
        "datasets": [
            {"name": "data", "eventtype": "data", "files": ["data.root"]},
            {"name": "ttbar", "eventtype": "mc", "files": ["ttbar.root"]},
        ]
    }
    workflow["analysis"]["stages"] = [
        {
            "id": "Common",
            "op": "toy.scale",
            "params": {"source": "pt", "output": "scaled_pt", "factor": 2},
        },
        {
            "id": "MCOnly",
            "op": "toy.scale",
            "params": {"source": "scaled_pt", "output": "mc_scaled", "factor": 3},
            "applies_to": {"eventtype": "mc"},
        },
        {
            "id": "Final",
            "op": "toy.scale",
            "params": {"source": "scaled_pt", "output": "final_pt", "factor": 1},
        },
    ]
    return workflow


def _data_only_workflow(toy_workflow: dict[str, Any]) -> dict[str, Any]:
    workflow = deepcopy(toy_workflow)
    workflow["data"] = {
        "datasets": [
            {"name": "data", "eventtype": "data", "files": ["data.root"]},
            {"name": "ttbar", "eventtype": "mc", "files": ["ttbar.root"]},
        ]
    }
    workflow["analysis"]["stages"] = [
        {
            "id": "Common",
            "op": "toy.scale",
            "params": {"source": "pt", "output": "scaled_pt", "factor": 2},
        },
        {
            "id": "DataOnly",
            "op": "toy.scale",
            "params": {"source": "scaled_pt", "output": "data_scaled", "factor": 3},
            "applies_to": {"eventtype": "data"},
        },
        {
            "id": "Final",
            "op": "toy.scale",
            "params": {"source": "scaled_pt", "output": "final_pt", "factor": 1},
        },
    ]
    return workflow


# applies_to: dataset_groups (the group of the dataset record), alone or with eventtype


def test_stage_applies_to_dataset_groups_normalizes_sorted_and_unique(
    toy_workflow: dict[str, Any],
) -> None:
    workflow = modified_stage(
        toy_workflow,
        applies_to={
            "eventtype": "MC",
            "dataset_groups": ["QCD_Zjet", "QCD_DY", "QCD_Zjet"],
        },
    )

    normalized = normalize_workflow(workflow)

    assert normalized["analysis"]["stages"][0]["applies_to"] == {
        "eventtype": "mc",
        "dataset_groups": ["QCD_DY", "QCD_Zjet"],
    }


def test_stage_applies_to_dataset_groups_without_eventtype_and_single_string(
    toy_workflow: dict[str, Any],
) -> None:
    workflow = modified_stage(toy_workflow, applies_to={"dataset_groups": "QCD_Wjet"})

    normalized = normalize_workflow(workflow)

    assert normalized["analysis"]["stages"][0]["applies_to"] == {
        "dataset_groups": ["QCD_Wjet"]
    }


@pytest.mark.parametrize(
    ("applies_to", "match"),
    [
        ({"dataset_groups": []}, "dataset_groups must be a non-empty"),
        ({"dataset_groups": [""]}, "dataset_groups entries must be non-empty"),
        ({"dataset_groups": [3]}, "dataset_groups entries must be strings"),
        ({}, "needs at least one of"),
        ({"dataset_group": ["QCD"]}, "only supports eventtype, dataset_groups"),
    ],
)
def test_stage_applies_to_dataset_groups_rejects_bad_values(
    toy_workflow: dict[str, Any],
    applies_to: Any,
    match: str,
) -> None:
    workflow = modified_stage(toy_workflow, applies_to=applies_to)

    with pytest.raises(ValueError, match=match):
        normalize_workflow(workflow)


def test_applies_to_dataset_group_matches_the_dataset_record_group() -> None:
    applicability = {"eventtype": "mc", "dataset_groups": ["QCD_Zjet", "QCD_DY"]}

    def applies(dataset: dict[str, Any]) -> bool:
        return node_applies_to_dataset(applicability, dataset=dataset)

    assert applies({"name": "z", "eventtype": "mc", "group": "QCD_Zjet"})
    assert applies({"name": "dy", "eventtype": "mc", "group": "QCD_DY"})
    assert not applies({"name": "w", "eventtype": "mc", "group": "QCD_Wjet"})
    assert not applies({"name": "z", "eventtype": "data", "group": "QCD_Zjet"})
    assert not applies({"name": "nogroup", "eventtype": "mc"})
    assert not applies({"name": "nogroup", "eventtype": "mc", "group": None})


def test_applicability_intersection_dataset_groups() -> None:
    assert intersect_applicability(
        {"dataset_groups": ["QCD_Zjet", "QCD_DY"]},
        {"dataset_groups": ["QCD_DY", "QCD_Wjet"]},
    ) == {"dataset_groups": ["QCD_DY"]}
    assert intersect_applicability(
        {"eventtype": "mc", "dataset_groups": ["QCD_DY"]},
        {"eventtypes": ["mc"]},
    ) == {"eventtypes": ["mc"], "dataset_groups": ["QCD_DY"]}
    assert applicability_is_empty(
        intersect_applicability(
            {"dataset_groups": ["QCD_Zjet"]},
            {"dataset_groups": ["QCD_Wjet"]},
        )
    )


def _group_workflow(toy_workflow: dict[str, Any]) -> dict[str, Any]:
    workflow = deepcopy(toy_workflow)
    workflow["data"] = {
        "datasets": [
            {"name": "data", "eventtype": "data", "files": ["data.root"]},
            {"name": "zjets", "eventtype": "mc", "group": "QCD_Zjet", "files": ["z.root"]},
            {"name": "wjets", "eventtype": "mc", "group": "QCD_Wjet", "files": ["w.root"]},
            {"name": "ttbar", "eventtype": "mc", "group": "TTbar", "files": ["tt.root"]},
        ]
    }
    workflow["analysis"]["stages"] = [
        {
            "id": "Common",
            "op": "toy.scale",
            "params": {"source": "pt", "output": "scaled_pt", "factor": 2},
        },
        {
            "id": "ZOnly",
            "op": "toy.scale",
            "params": {"source": "scaled_pt", "output": "z_scaled", "factor": 3},
            "applies_to": {"eventtype": "mc", "dataset_groups": ["QCD_Zjet", "QCD_DY"]},
        },
        {
            "id": "WOnly",
            "op": "toy.scale",
            "params": {"source": "scaled_pt", "output": "w_scaled", "factor": 4},
            "applies_to": {"dataset_groups": ["QCD_Wjet"]},
        },
        {
            "id": "Final",
            "op": "toy.scale",
            "params": {"source": "scaled_pt", "output": "final_pt", "factor": 1},
        },
    ]
    return workflow


def test_active_plan_nodes_follow_the_dataset_group(toy_workflow: dict[str, Any]) -> None:
    _graph, plan = build_plan_from_normalized(
        normalize_workflow(_group_workflow(toy_workflow))
    )

    def active(name: str) -> list[str]:
        dataset = dict(plan.context["datasets"][name])
        return [
            node.id
            for node in active_plan_nodes_for_dataset(plan, dataset=dataset)
            if node.id.startswith("stage.") and node.id != "stage.Common"
        ]

    assert active("zjets") == ["stage.ZOnly", "stage.Final"]
    assert active("wjets") == ["stage.WOnly", "stage.Final"]
    assert active("ttbar") == ["stage.Final"]
    assert active("data") == ["stage.Final"]
