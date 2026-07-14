from __future__ import annotations

from copy import deepcopy
from typing import Any

import pytest

from hepflow.compiler.lower_graph import lower_author_to_graph
from hepflow.compiler.normalize import normalize_author
from hepflow.compiler.plan import build_plan_from_normalized
from hepflow.model.plan_applicability import active_plan_nodes_for_dataset
from hepflow.runtime.engine import build_partition_context, execute_plan_partition


@pytest.mark.parametrize("eventtype", ["data", "mc"])
def test_stage_applies_to_eventtype_normalizes(
    toy_author: dict[str, Any],
    eventtype: str,
) -> None:
    author = deepcopy(toy_author)
    author["analysis"]["stages"][0]["applies_to"] = {"eventtype": eventtype}

    normalized = normalize_author(author)

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
    toy_author: dict[str, Any],
    applies_to: Any,
    match: str,
) -> None:
    author = deepcopy(toy_author)
    author["analysis"]["stages"][0]["applies_to"] = applies_to

    with pytest.raises(ValueError, match=match):
        normalize_author(author)


def test_active_plan_nodes_omit_mc_only_nodes_for_data(
    toy_author: dict[str, Any],
) -> None:
    author = _applicability_author(toy_author)
    _graph, plan = build_plan_from_normalized(normalize_author(author))

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
    toy_author: dict[str, Any],
) -> None:
    author = _applicability_author(toy_author)
    _graph, plan = build_plan_from_normalized(normalize_author(author))
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
    toy_author: dict[str, Any],
) -> None:
    author = _data_only_author(toy_author)
    _graph, plan = build_plan_from_normalized(normalize_author(author))

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
    toy_author: dict[str, Any],
) -> None:
    author = deepcopy(toy_author)
    author["data"] = {
        "datasets": [
            {"name": "data", "eventtype": "data", "files": ["data.root"]},
            {"name": "ttbar", "eventtype": "mc", "files": ["ttbar.root"]},
        ]
    }
    author["analysis"]["stages"] = [
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

    _graph, plan = build_plan_from_normalized(normalize_author(author))
    source = plan.get_node("read.events")

    assert source.params["branches_by_dataset"] == {
        "data": ["Muon_Px"],
        "ttbar": ["MCLepton_Px", "Muon_Px"],
    }
    assert "branches" not in source.params


def test_unsupported_applicability_bypass_raises_clear_error(
    toy_author: dict[str, Any],
) -> None:
    author = deepcopy(toy_author)
    author["data"] = {
        "datasets": [
            {"name": "data", "eventtype": "data", "files": ["data.root"]},
            {"name": "ttbar", "eventtype": "mc", "files": ["ttbar.root"]},
        ]
    }
    author["analysis"]["stages"] = [
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
        build_plan_from_normalized(normalize_author(author))


def test_run_end_sink_can_consume_data_and_mc_specific_products(
    toy_author: dict[str, Any],
) -> None:
    author = deepcopy(toy_author)
    author["data"] = {
        "datasets": [
            {"name": "data", "eventtype": "data", "files": ["data.root"]},
            {"name": "ttbar", "eventtype": "mc", "files": ["ttbar.root"]},
        ]
    }
    author["analysis"]["stages"] = [
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

    _graph, plan = build_plan_from_normalized(normalize_author(author))

    assert plan.get_node("render.Compare.0").params["when"] == "run_end"


def test_hist_variations_add_explicit_variation_axis(toy_author: dict[str, Any]) -> None:
    author = deepcopy(toy_author)
    author["analysis"]["stages"] = [
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

    graph = lower_author_to_graph(normalize_author(author))
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
    toy_author: dict[str, Any],
) -> None:
    author = deepcopy(toy_author)
    author["analysis"]["stages"] = [
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

    graph = lower_author_to_graph(normalize_author(author))

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
    toy_author: dict[str, Any],
) -> None:
    author = _applicability_author(toy_author)
    graph = lower_author_to_graph(normalize_author(author))

    payload = graph.nodes["stage.MCOnly"]["payload"]

    assert payload.meta["applies_to"] == {"eventtype": "mc"}


def _applicability_author(toy_author: dict[str, Any]) -> dict[str, Any]:
    author = deepcopy(toy_author)
    author["data"] = {
        "datasets": [
            {"name": "data", "eventtype": "data", "files": ["data.root"]},
            {"name": "ttbar", "eventtype": "mc", "files": ["ttbar.root"]},
        ]
    }
    author["analysis"]["stages"] = [
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
    return author


def _data_only_author(toy_author: dict[str, Any]) -> dict[str, Any]:
    author = deepcopy(toy_author)
    author["data"] = {
        "datasets": [
            {"name": "data", "eventtype": "data", "files": ["data.root"]},
            {"name": "ttbar", "eventtype": "mc", "files": ["ttbar.root"]},
        ]
    }
    author["analysis"]["stages"] = [
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
    return author
