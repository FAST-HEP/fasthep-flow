from __future__ import annotations

import json
import os
import time
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest
import yaml
from tests.toy_components import transforms as toy_transforms

from hepflow.api import compile_workflow_file, run_plan_file
from hepflow.build_layout import ensure_build_layout, plan_path
from hepflow.compiler import plan as plan_module
from hepflow.compiler.artifacts import write_compile_artifacts
from hepflow.compiler.component_defaults import apply_component_param_defaults
from hepflow.compiler.inline_variations import apply_inline_variation_branches_to_graph
from hepflow.compiler.lower_graph import lower_workflow_to_graph
from hepflow.compiler.normalize import normalize_workflow
from hepflow.compiler.plan import build_plan_from_normalized
from hepflow.model.plan import ExecutionPartition, ExecutionPlan
from hepflow.runtime.engine import execute_plan_locally
from hepflow.utils import write_yaml


def test_inline_variation_clones_linear_downstream_branch(
    toy_registry: dict[str, Any],
) -> None:
    plan = _inline_plan(
        toy_registry,
        [
            _scale("A", source="pt", output="a_pt"),
            _scale("B", source="a_pt", output="b_pt", upstream="A"),
            _scale("C", source="b_pt", output="c_pt"),
            _scale("D", source="c_pt", output="d_pt"),
        ],
        fields=["pt"],
        variations=[
            {
                "name": "jec_up",
                "mode": "inline",
                "anchor": "B",
                "patch": {"params": {"factor": 10}},
            }
        ],
    )

    assert {"stage.B@jec_up", "stage.C@jec_up", "stage.D@jec_up"} <= set(
        plan.node_index
    )
    assert _input_node(plan, "stage.B@jec_up") == "stage.A"
    assert _input_node(plan, "stage.C@jec_up") == "stage.B@jec_up"
    assert _input_node(plan, "stage.D@jec_up") == "stage.C@jec_up"
    assert plan.get_node("stage.B@jec_up").impl == plan.get_node("stage.B").impl
    assert plan.get_node("stage.B@jec_up").params["factor"] == 10


def test_inline_variation_clones_branching_downstream_graph(
    toy_registry: dict[str, Any],
) -> None:
    plan = _inline_plan(
        toy_registry,
        [
            _scale("A", source="pt", output="a_pt"),
            _scale("B", source="a_pt", output="b_pt", upstream="A"),
            _scale("Left", source="b_pt", output="left_pt", upstream="B"),
            _scale("Right", source="b_pt", output="right_pt", upstream="B"),
            _scale("AfterLeft", source="left_pt", output="after_left_pt", upstream="Left"),
        ],
        fields=["pt"],
        variations=[
            {
                "name": "jes_down",
                "mode": "inline",
                "anchor": "B",
            }
        ],
    )

    assert {
        "stage.B@jes_down",
        "stage.Left@jes_down",
        "stage.Right@jes_down",
        "stage.AfterLeft@jes_down",
    } <= set(plan.node_index)
    assert _input_node(plan, "stage.Left@jes_down") == "stage.B@jes_down"
    assert _input_node(plan, "stage.Right@jes_down") == "stage.B@jes_down"
    assert _input_node(plan, "stage.AfterLeft@jes_down") == "stage.Left@jes_down"


def test_inline_variation_keeps_shared_nominal_side_input(
    toy_registry: dict[str, Any],
) -> None:
    plan = _inline_plan(
        _registry_with_product(toy_registry),
        [
            _scale("A", source="pt", output="a_pt"),
            {
                "id": "SharedWeights",
                "op": "toy.product",
                "from": [],
                "params": {"value": "weights"},
            },
            _scale("B", source="a_pt", output="b_pt", upstream="A"),
            {
                **_scale("C", source="b_pt", output="c_pt", upstream="B"),
                "from": [
                    {"node": "B", "port": "stream", "as": "stream"},
                    {"node": "SharedWeights", "port": "product", "as": "weights"},
                ],
            },
        ],
        fields=["pt"],
        variations=[{"name": "up", "mode": "inline", "anchor": "B"}],
    )

    cloned_c = plan.get_node("stage.C@up")
    assert {
        (ref.input_name, ref.node_id)
        for ref in cloned_c.inputs
    } == {
        ("stream", "stage.B@up"),
        ("weights", "stage.SharedWeights"),
    }
    nominal_c = plan.get_node("stage.C")
    assert {
        (ref.input_name, ref.node_id)
        for ref in nominal_c.inputs
    } == {
        ("stream", "stage.B"),
        ("weights", "stage.SharedWeights"),
    }
    assert plan.get_node("stage.SharedWeights").output_scope == "global"
    assert plan.get_node("stage.SharedWeights").outputs == {"product": "toy_product"}
    assert "stage.SharedWeights@up" not in plan.node_index


def test_global_side_product_is_produced_once_for_partition_run(
    toy_registry: dict[str, Any],
) -> None:
    plan = _inline_plan(
        _registry_with_product(toy_registry),
        [
            {
                "id": "SharedWeights",
                "op": "toy.product",
                "from": [],
                "params": {"value": "weights"},
            },
            {
                **_scale("UseWeights", source="pt", output="scaled_pt"),
                "from": [
                    {"node": "events", "port": "stream", "as": "stream"},
                    {"node": "SharedWeights", "port": "product", "as": "weights"},
                ],
            },
        ],
        fields=["pt"],
        variations=[],
    )
    plan.partitions = [
        ExecutionPartition(
            id="events__sample__0",
            dataset="sample",
            file="sample.root",
            source="events",
            part="0_0",
        ),
        ExecutionPartition(
            id="events__sample__1",
            dataset="sample",
            file="sample-2.root",
            source="events",
            part="1_0",
        ),
    ]
    toy_transforms.TOY_PRODUCT_CALLS.clear()

    summaries = execute_plan_locally(plan, partitions=plan.partitions)

    assert len(summaries) == 2
    assert toy_transforms.TOY_PRODUCT_CALLS == ["weights"]


def test_inline_variation_preserves_same_field_names_across_streams(
    toy_registry: dict[str, Any],
) -> None:
    plan = _inline_plan(
        toy_registry,
        [
            _scale("A", source="pt", output="a_pt"),
            _scale("B", source="a_pt", output="Jet_pt"),
            _scale("C", source="Jet_pt", output="Selected_pt"),
        ],
        fields=["pt"],
        variations=[{"name": "jer_up", "mode": "inline", "anchor": "B"}],
    )

    assert plan.get_node("stage.B@jer_up").params["output"] == "Jet_pt"
    assert plan.get_node("stage.C@jer_up").params["source"] == "Jet_pt"
    assert plan.data_flow["origins"]["Jet_pt"]["kind"] == "stream_scoped"


def test_inline_variation_duplicate_names_fail_before_graph_expansion(
    toy_registry: dict[str, Any],
) -> None:
    with pytest.raises(ValueError, match="duplicate systematics variation name"):
        _inline_plan(
            toy_registry,
            [_scale("A", source="pt", output="a_pt"), _scale("B", source="a_pt")],
            fields=["pt"],
            variations=[
                {"name": "up", "mode": "inline", "anchor": "B"},
                {"name": "up", "mode": "inline", "anchor": "B"},
            ],
        )


def test_inline_variation_preserves_event_lineage(toy_registry: dict[str, Any]) -> None:
    plan = _inline_plan(
        toy_registry,
        [_scale("A", source="pt", output="a_pt"), _scale("B", source="a_pt")],
        fields=["pt"],
        variations=[{"name": "up", "mode": "inline", "anchor": "B"}],
    )

    assert _lineage(plan, "stage.B@up") == _lineage(plan, "stage.B")
    assert _lineage(plan, "stage.B@up") == _lineage(plan, "read.events")


def test_inline_variation_stop_boundary_is_exclusive(
    toy_registry: dict[str, Any],
) -> None:
    plan = _inline_plan(
        toy_registry,
        [
            _scale("A", source="pt", output="a_pt"),
            _scale("B", source="a_pt", output="b_pt"),
            _scale("C", source="b_pt", output="c_pt"),
            _scale("D", source="c_pt", output="d_pt"),
        ],
        fields=["pt"],
        variations=[
            {
                "name": "up",
                "mode": "inline",
                "anchor": "B",
                "stop_before": ["C"],
            }
        ],
    )

    assert "stage.B@up" in plan.node_index
    assert "stage.C@up" not in plan.node_index
    assert "stage.D@up" not in plan.node_index


def test_inline_variation_does_not_clone_sink_by_default(
    toy_registry: dict[str, Any],
) -> None:
    plan = _inline_plan(
        toy_registry,
        [
            _scale("A", source="pt", output="a_pt"),
            {
                **_scale("B", source="a_pt"),
                "write": [{"kind": "toy.write", "path": "output.json"}],
            },
        ],
        fields=["pt"],
        variations=[{"name": "up", "mode": "inline", "anchor": "B"}],
    )

    assert "write.Output@up" not in plan.node_index


def test_inline_variation_plan_executes_with_existing_local_runtime(
    toy_registry: dict[str, Any],
) -> None:
    plan = _inline_plan(
        toy_registry,
        [
            _scale("A", source="pt", output="a_pt"),
            _scale("B", source="a_pt", output="b_pt", factor=2),
            _scale("C", source="b_pt", output="c_pt", factor=3),
        ],
        fields=["pt"],
        variations=[
            {
                "name": "up",
                "mode": "inline",
                "anchor": "B",
                "patch": {"params": {"factor": 10}},
            }
        ],
    )
    store = execute_plan_locally(plan)

    assert store[("stage.C", "stream")]["c_pt"] == [72, 108, 126, 168]
    assert store[("stage.C@up", "stream")]["c_pt"] == [360, 540, 630, 840]


def test_authored_inline_variation_compiles_to_one_plan_and_runs(
    toy_registry: dict[str, Any],
    tmp_path: Any,
) -> None:
    workflow = {
        "version": "1.0",
        "registry": toy_registry,
        "data": {
            "datasets": [
                {"name": "sample", "files": ["sample.root"], "eventtype": "mc"}
            ]
        },
        "sources": {
            "events": {
                "kind": "toy.source",
                "stream_type": "event_stream",
                "branches": ["pt"],
            }
        },
        "analysis": {
            "stages": [
                _scale("A", source="pt", output="a_pt"),
                _scale("B", source="a_pt", output="b_pt", factor=2),
                _scale("C", source="b_pt", output="c_pt", factor=3),
            ]
        },
        "systematics": {
            "include_nominal": True,
            "variations": [
                {
                    "name": "up",
                    "group": "toy",
                    "direction": "up",
                    "mode": "inline",
                    "anchor": "B",
                    "patch": {"params": {"factor": 10}},
                }
            ],
        },
    }
    workflow_path = tmp_path / "workflow.yaml"
    workflow_path.write_text(yaml.safe_dump(workflow, sort_keys=False), encoding="utf-8")
    build_dir = tmp_path / "build"

    compile_workflow_file(workflow_path, outdir=build_dir)

    plan_path = build_dir / "compile" / "plan.yaml"
    assert plan_path.exists()
    assert not (build_dir / "compile" / "up" / "plan.yaml").exists()
    plan_doc = yaml.safe_load(plan_path.read_text(encoding="utf-8"))
    nodes = {node["id"]: node for node in plan_doc["nodes"]}
    assert {"stage.A", "stage.B", "stage.C"}.issubset(nodes)
    assert {"stage.B@up", "stage.C@up"}.issubset(nodes)
    assert "variation" not in plan_doc["context"]
    assert nodes["stage.B@up"]["meta"]["variation"] == {
        "name": "up",
        "mode": "inline",
        "group": "toy",
        "direction": "up",
    }
    assert nodes["stage.B@up"]["params"]["factor"] == 10

    result = run_plan_file(plan_path, outdir=build_dir)
    assert result.success is True
    stores = result.outputs["value_store"]
    assert isinstance(stores, list)
    assert [getattr(store, "products", None) for store in stores] == [[]]


def test_inline_variation_export_collects_selected_fields_before_single_writer(
    toy_registry: dict[str, Any],
    tmp_path: Any,
) -> None:
    workflow_path = _write_inline_export_workflow(
        tmp_path,
        _registry_with_collection_ops(toy_registry),
        datasets=[
            {"name": "data", "files": ["data.root"], "eventtype": "data"},
        ],
        variations=[
            {
                "name": "up",
                "mode": "inline",
                "anchor": "B",
                "patch": {"params": {"factor": 10}},
                "export": {"b_pt_up": "b_pt"},
            }
        ],
    )
    build_dir = tmp_path / "build"

    compile_workflow_file(workflow_path, outdir=build_dir)
    result = run_plan_file(build_dir / "compile" / "plan.yaml", outdir=build_dir)

    assert result.success is True
    plan_doc = yaml.safe_load(
        (build_dir / "compile" / "plan.yaml").read_text(encoding="utf-8")
    )
    nodes = {node["id"]: node for node in plan_doc["nodes"]}
    assert [node["role"] for node in plan_doc["nodes"]].count("sink") == 1
    assert any(node["impl"] == "hep.project_fields" for node in nodes.values())
    assert any(node["impl"] == "hep.merge_fields" for node in nodes.values())
    assert not (build_dir / "compile" / "up" / "plan.yaml").exists()

    payload = _output_payload(build_dir)
    assert payload["b_pt"] == [24, 36, 42, 56]
    assert payload["c_pt"] == [72, 108, 126, 168]
    assert payload["b_pt_up"] == [120, 180, 210, 280]
    assert "c_pt_up" not in payload
    assert "variation_1" not in payload


def test_inline_variation_applies_to_marks_cloned_branch(
    toy_registry: dict[str, Any],
    tmp_path: Any,
) -> None:
    workflow_path = _write_inline_export_workflow(
        tmp_path,
        _registry_with_collection_ops(toy_registry),
        variations=[
            {
                "name": "up",
                "mode": "inline",
                "applies_to": "mc",
                "anchor": "B",
                "patch": {"params": {"factor": 10}},
                "export": {"b_pt_up": "b_pt"},
            }
        ],
    )

    compile_workflow_file(workflow_path, outdir=tmp_path / "build")
    plan_doc = yaml.safe_load(
        (tmp_path / "build" / "compile" / "plan.yaml").read_text(encoding="utf-8")
    )
    cloned_nodes = [
        node
        for node in plan_doc["nodes"]
        if isinstance(node.get("meta"), dict) and node["meta"].get("variation_of")
    ]

    assert cloned_nodes
    assert {
        tuple(node["meta"].get("applies_to", {}).get("eventtypes") or [])
        for node in cloned_nodes
    } == {("mc",)}


def test_inline_variation_intersects_original_dataset_applicability(
    toy_registry: dict[str, Any],
) -> None:
    plan = _inline_plan(
        toy_registry,
        [_scale("A", source="pt", output="a_pt"), _scale("B", source="a_pt")],
        fields=["pt"],
        variations=[
            {
                "name": "up",
                "mode": "inline",
                "anchor": "B",
                "applies_to": {"eventtypes": ["mc"], "datasets": ["dy", "wjets"]},
            }
        ],
        stage_updates={"B": {"applies_to": {"datasets": ["dy", "ttbar"]}}},
    )

    assert plan.get_node("stage.B@up").meta["applies_to"] == {
        "eventtypes": ["mc"],
        "datasets": ["dy"],
    }


def test_inline_variation_omits_clone_for_empty_applicability_intersection(
    toy_registry: dict[str, Any],
) -> None:
    plan = _inline_plan(
        toy_registry,
        [_scale("A", source="pt", output="a_pt"), _scale("B", source="a_pt")],
        fields=["pt"],
        variations=[
            {
                "name": "up",
                "mode": "inline",
                "anchor": "B",
                "applies_to": {"eventtypes": ["mc"]},
            }
        ],
        stage_updates={"B": {"applies_to": {"eventtype": "data"}}},
    )

    assert "stage.B@up" not in plan.node_index


def test_inline_variation_downstream_clone_keeps_narrower_original_applicability(
    toy_registry: dict[str, Any],
) -> None:
    plan = _inline_plan(
        toy_registry,
        [
            _scale("A", source="pt", output="a_pt"),
            _scale("B", source="a_pt", output="b_pt"),
            _scale("C", source="b_pt", output="c_pt"),
        ],
        fields=["pt"],
        variations=[
            {
                "name": "up",
                "mode": "inline",
                "anchor": "B",
                "applies_to": {"eventtypes": ["mc"]},
            }
        ],
        stage_updates={
            "C": {"applies_to": {"eventtypes": ["mc"], "datasets": ["dy"]}},
        },
    )

    assert {"stage.B@up", "stage.C@up"} <= set(plan.node_index)
    assert plan.get_node("stage.B@up").meta["applies_to"] == {"eventtypes": ["mc"]}
    assert plan.get_node("stage.C@up").meta["applies_to"] == {
        "eventtypes": ["mc"],
        "datasets": ["dy"],
    }


def test_authored_inline_variation_does_not_broaden_anchor_applicability(
    toy_registry: dict[str, Any],
    tmp_path: Any,
) -> None:
    workflow_path = _write_inline_export_workflow(
        tmp_path,
        _registry_with_collection_ops(toy_registry),
        variations=[
            {
                "name": "up",
                "mode": "inline",
                "applies_to": "mc",
                "anchor": "B",
                "patch": {"params": {"factor": 10}},
                "export": {"b_pt_up": "b_pt"},
            }
        ],
        anchor_applies_to={"eventtype": "data"},
        datasets=[{"name": "data", "files": ["data.root"], "eventtype": "data"}],
    )

    compile_workflow_file(workflow_path, outdir=tmp_path / "build")
    plan_doc = yaml.safe_load(
        (tmp_path / "build" / "compile" / "plan.yaml").read_text(encoding="utf-8")
    )

    assert all("@up" not in node["id"] for node in plan_doc["nodes"])


def test_inline_variation_export_can_collect_before_stop_boundary(
    toy_registry: dict[str, Any],
    tmp_path: Any,
) -> None:
    workflow_path = _write_inline_export_workflow(
        tmp_path,
        _registry_with_collection_ops(toy_registry),
        variations=[
            {
                "name": "up",
                "mode": "inline",
                "anchor": "B",
                "patch": {"params": {"factor": 10}},
                "stop_before": ["C"],
                "export": {"b_pt_up": "b_pt"},
            }
        ],
    )
    build_dir = tmp_path / "build"

    compile_workflow_file(workflow_path, outdir=build_dir)
    run_plan_file(build_dir / "compile" / "plan.yaml", outdir=build_dir)

    plan_doc = yaml.safe_load(
        (build_dir / "compile" / "plan.yaml").read_text(encoding="utf-8")
    )
    nodes = {node["id"]: node for node in plan_doc["nodes"]}
    assert "stage.C@up" not in nodes
    assert nodes["stage.C"]["inputs"][0]["node_id"] == "collect.stage.C"
    assert nodes["collect.stage.C"]["meta"]["collection_for"] == "stage.C"
    assert [node["role"] for node in plan_doc["nodes"]].count("sink") == 1

    payload = _output_payload(build_dir)
    assert payload["b_pt_up"] == [120, 180, 210, 280]
    assert payload["c_pt"] == [72, 108, 126, 168]


def test_inline_variation_collection_selects_the_varied_stream_input(
    toy_registry: dict[str, Any],
    tmp_path: Any,
) -> None:
    workflow_path = _write_inline_export_workflow(
        tmp_path,
        _registry_with_collection_ops(toy_registry),
        stages=[
            _scale("A", source="pt", output="a_pt"),
            _scale("B", source="a_pt", output="b_pt", factor=2),
            _scale("Side", source="pt", output="side_pt", upstream="events"),
            {
                "id": "Join",
                "op": "hep.merge_fields",
                "from": [
                    {"node": "Side", "port": "stream", "as": "side"},
                    {"node": "B", "port": "stream", "as": "stream"},
                ],
                "params": {"on_conflict": "keep_first"},
                "write": [{"kind": "toy.write", "path": "output.json"}],
            },
        ],
        variations=[
            {
                "name": "up",
                "mode": "inline",
                "anchor": "B",
                "patch": {"params": {"factor": 10}},
                "stop_before": ["Join"],
                "export": {"b_pt_up": "b_pt"},
            }
        ],
    )
    build_dir = tmp_path / "build"

    compile_workflow_file(workflow_path, outdir=build_dir)
    plan_doc = yaml.safe_load(
        (build_dir / "compile" / "plan.yaml").read_text(encoding="utf-8")
    )
    nodes = {node["id"]: node for node in plan_doc["nodes"]}
    join_inputs = {
        item["input_name"]: item["node_id"] for item in nodes["stage.Join"]["inputs"]
    }

    assert join_inputs == {
        "side": "stage.Side",
        "stream": "collect.stage.Join",
    }
    assert {
        item["input_name"]: item["node_id"]
        for item in nodes["collect.stage.Join"]["inputs"]
    } == {
        "nominal": "stage.B",
        "variation_1": "export.stage.B@up",
    }


def test_inline_variation_collection_rejects_ambiguous_varied_stream_inputs(
    toy_registry: dict[str, Any],
    tmp_path: Any,
) -> None:
    workflow_path = _write_inline_export_workflow(
        tmp_path,
        _registry_with_collection_ops(toy_registry),
        stages=[
            _scale("A", source="pt", output="a_pt"),
            _scale("B", source="a_pt", output="b_pt", factor=2),
            _scale("Side", source="pt", output="side_pt", upstream="events"),
            {
                "id": "Join",
                "op": "hep.merge_fields",
                "from": [
                    {"node": "Side", "port": "stream", "as": "side"},
                    {"node": "B", "port": "stream", "as": "stream"},
                ],
                "params": {"on_conflict": "keep_first"},
                "write": [{"kind": "toy.write", "path": "output.json"}],
            },
        ],
        variations=[
            {
                "name": "b_up",
                "mode": "inline",
                "anchor": "B",
                "patch": {"params": {"factor": 10}},
                "stop_before": ["Join"],
                "export": {"b_pt_up": "b_pt"},
            },
            {
                "name": "side_up",
                "mode": "inline",
                "anchor": "Side",
                "patch": {"params": {"factor": 10}},
                "stop_before": ["Join"],
                "export": {"side_pt_up": "side_pt"},
            },
        ],
    )

    with pytest.raises(ValueError, match="multiple event-stream inputs vary"):
        compile_workflow_file(workflow_path, outdir=tmp_path / "build")


def test_inline_variation_collector_omits_inactive_mc_exports_for_data(
    toy_registry: dict[str, Any],
    tmp_path: Any,
) -> None:
    workflow_path = _write_inline_export_workflow(
        tmp_path,
        _registry_with_collection_ops(toy_registry),
        datasets=[
            {"name": "data", "files": ["data.root"], "eventtype": "data"},
            {"name": "mc", "files": ["mc.root"], "eventtype": "mc"},
        ],
        variations=[
            {
                "name": "up",
                "mode": "inline",
                "applies_to": "mc",
                "anchor": "B",
                "patch": {"params": {"factor": 10}},
                "export": {"b_pt_up": "b_pt"},
            }
        ],
    )
    build_dir = tmp_path / "build"

    compile_workflow_file(workflow_path, outdir=build_dir)
    result = run_plan_file(build_dir / "compile" / "plan.yaml", outdir=build_dir)
    assert result.success is True
    data_collect = json.loads(
        (build_dir / "artifacts" / "files" / "output" / "data" / "0_0.json").read_text(
            encoding="utf-8"
        )
    )
    mc_collect = json.loads(
        (build_dir / "artifacts" / "files" / "output" / "mc" / "0_0.json").read_text(
            encoding="utf-8"
        )
    )
    assert "b_pt_up" not in data_collect
    assert "b_pt_up" in mc_collect


def test_unrelated_multi_input_transform_rejects_inactive_required_input(
    toy_registry: dict[str, Any],
    tmp_path: Any,
) -> None:
    workflow_path = _write_inline_export_workflow(
        tmp_path,
        _registry_with_strict_merge(toy_registry),
        datasets=[
            {"name": "data", "files": ["data.root"], "eventtype": "data"},
            {"name": "mc", "files": ["mc.root"], "eventtype": "mc"},
        ],
        stages=[
            _scale("A", source="pt", output="a_pt"),
            _scale("MCOnly", source="pt", output="mc_pt", upstream="events"),
            {
                "id": "Join",
                "op": "toy.strict_merge_fields",
                "from": [
                    {"node": "A", "port": "stream", "as": "left"},
                    {"node": "MCOnly", "port": "stream", "as": "right"},
                ],
                "params": {"on_conflict": "keep_first"},
                "write": [{"kind": "toy.write", "path": "output.json"}],
            },
        ],
        stage_updates={"MCOnly": {"applies_to": {"eventtype": "mc"}}},
        variations=[],
    )

    with pytest.raises(ValueError, match="inactive required input"):
        compile_workflow_file(workflow_path, outdir=tmp_path / "build")


def test_two_inline_variation_exports_contribute_different_fields(
    toy_registry: dict[str, Any],
    tmp_path: Any,
) -> None:
    workflow_path = _write_inline_export_workflow(
        tmp_path,
        _registry_with_collection_ops(toy_registry),
        variations=[
            {
                "name": "up",
                "mode": "inline",
                "anchor": "B",
                "patch": {"params": {"factor": 10}},
                "export": {"b_pt_up": "b_pt"},
            },
            {
                "name": "down",
                "mode": "inline",
                "anchor": "B",
                "patch": {"params": {"factor": 1}},
                "export": {"c_pt_down": "c_pt"},
            },
        ],
    )
    build_dir = tmp_path / "build"

    compile_workflow_file(workflow_path, outdir=build_dir)
    run_plan_file(build_dir / "compile" / "plan.yaml", outdir=build_dir)

    payload = _output_payload(build_dir)
    assert payload["b_pt_up"] == [120, 180, 210, 280]
    assert payload["c_pt_down"] == [36, 54, 63, 84]
    assert "b_pt_down" not in payload


def test_inline_variation_compile_scales_to_thousand_node_graph(
    toy_registry: dict[str, Any],
    tmp_path: Any,
) -> None:
    workflow = _large_inline_variation_workflow(toy_registry)
    workflow_path = tmp_path / "workflow.yaml"
    workflow_path.write_text(yaml.safe_dump(workflow, sort_keys=False), encoding="utf-8")

    compile_workflow_file(workflow_path, outdir=tmp_path / "build")

    compile_dir = tmp_path / "build" / "compile"
    plan_doc = yaml.safe_load((compile_dir / "plan.yaml").read_text(encoding="utf-8"))
    deps = yaml.safe_load((compile_dir / "deps.yaml").read_text(encoding="utf-8"))
    nodes = {node["id"]: node for node in plan_doc["nodes"]}
    cloned_nodes = [
        node_id
        for node_id, node in nodes.items()
        if isinstance(node.get("meta"), dict) and node["meta"].get("variation_of")
    ]

    assert len(plan_doc["nodes"]) >= 1000
    assert len(cloned_nodes) == 46 * 22
    assert [node["role"] for node in plan_doc["nodes"]].count("sink") == 1
    assert any(node["id"].startswith("collect.") for node in plan_doc["nodes"])
    assert "f45_v00" in deps["origins"]
    assert "f45_v21" in deps["origins"]
    assert _origin_count(deps["origins"]["pt"]) == 1
    assert _origin_count(deps["origins"]["f0"]) == 23
    assert deps["origins"]["f0"]["kind"] == "stream_scoped"
    assert len(deps["origins"]) < 200
    assert (compile_dir / "deps.yaml").stat().st_size < 4_000_000
    assert (compile_dir / "plan.yaml").stat().st_size < 8_000_000


@pytest.mark.performance
@pytest.mark.skipif(
    os.environ.get("FASTHEP_RUN_PERFORMANCE") != "1",
    reason="set FASTHEP_RUN_PERFORMANCE=1 to run compiler performance benchmarks",
)
def test_inline_variation_thousand_node_compile_benchmark(
    toy_registry: dict[str, Any],
    tmp_path: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workflow = _large_inline_variation_workflow(toy_registry)
    timings: dict[str, float] = {}
    compile_start = time.perf_counter()

    normalized = _time_phase(timings, "normalize_workflow", normalize_workflow, workflow)

    def lower_phase() -> Any:
        prepared = apply_component_param_defaults(normalized)
        return lower_workflow_to_graph(prepared)

    graph = _time_phase(timings, "lower_workflow_to_graph", lower_phase)
    _time_phase(
        timings,
        "inline_variation_expansion",
        apply_inline_variation_branches_to_graph,
        graph,
        normalized,
    )

    infer_call = 0
    original_infer_data_flow = plan_module.infer_data_flow
    original_run_param_compile_hooks = plan_module.run_param_compile_hooks

    def timed_infer_data_flow(*args: Any, **kwargs: Any) -> Any:
        nonlocal infer_call
        phase = (
            "preliminary_infer_data_flow"
            if infer_call == 0
            else "final_infer_data_flow"
        )
        infer_call += 1
        return _time_phase(timings, phase, original_infer_data_flow, *args, **kwargs)

    def timed_run_param_compile_hooks(*args: Any, **kwargs: Any) -> Any:
        return _time_phase(
            timings,
            "parameter_hooks",
            original_run_param_compile_hooks,
            *args,
            **kwargs,
        )

    monkeypatch.setattr(plan_module, "infer_data_flow", timed_infer_data_flow)
    monkeypatch.setattr(
        plan_module,
        "run_param_compile_hooks",
        timed_run_param_compile_hooks,
    )
    plan_start = time.perf_counter()
    plan = plan_module.build_execution_plan(
        graph,
        registry=dict(normalized.get("registry") or {}),
        provenance=dict(normalized.get("provenance") or {}),
        execution=plan_module.normalize_global_execution(normalized.get("execution")),
        execution_hooks=list(normalized.get("execution_hooks") or []),
    )
    build_execution_plan_total = time.perf_counter() - plan_start
    timings["build_execution_plan"] = max(
        0.0,
        build_execution_plan_total
        - timings.get("preliminary_infer_data_flow", 0.0)
        - timings.get("parameter_hooks", 0.0)
        - timings.get("final_infer_data_flow", 0.0),
    )

    outdir = tmp_path / "build"
    ensure_build_layout(outdir)

    def serialize_artifacts() -> None:
        write_compile_artifacts(
            plan=plan,
            graph=graph,
            outdir=outdir,
            normalized=normalized,
        )
        write_yaml(plan.to_dict(), str(plan_path(outdir)))

    _time_phase(timings, "artifact_serialization", serialize_artifacts)
    timings["compile_total"] = time.perf_counter() - compile_start

    compile_dir = outdir / "compile"
    metrics = _compiler_benchmark_metrics(
        plan=plan,
        edge_count=graph.number_of_edges(),
        timings=timings,
        deps_path=compile_dir / "deps.yaml",
        plan_yaml_path=compile_dir / "plan.yaml",
    )
    output_path = Path(
        os.environ.get(
            "FASTHEP_BENCHMARK_OUT",
            str(tmp_path / "inline_variation_compile_benchmark.json"),
        )
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(metrics, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(metrics, indent=2, sort_keys=True))

    assert metrics["node_count"] >= 1000
    assert metrics["variation_clone_count"] == 46 * 22
    assert metrics["origin_field_count"] < 200
    assert metrics["deps_yaml_size_bytes"] < 4_000_000
    assert metrics["plan_yaml_size_bytes"] < 8_000_000

    max_seconds = os.environ.get("FASTHEP_COMPILER_BENCHMARK_MAX_SECONDS")
    if max_seconds:
        assert metrics["compile_time_seconds"] <= float(max_seconds)


def test_inline_variation_export_rejects_incompatible_lineage(
    toy_registry: dict[str, Any],
    tmp_path: Any,
) -> None:
    registry = _registry_with_collection_ops(toy_registry)
    registry["transforms"]["toy.new_lineage"] = {
        "spec": "tests.toy_components.transforms:TOY_NEW_LINEAGE_SPEC",
        "impl": "tests.toy_components.transforms:run_toy_scale",
    }
    workflow_path = _write_inline_export_workflow(
        tmp_path,
        registry,
        anchor_op="toy.new_lineage",
        variations=[
            {
                "name": "up",
                "mode": "inline",
                "anchor": "B",
                "patch": {"params": {"factor": 10}},
                "export": {"b_pt_up": "b_pt"},
            }
        ],
    )

    with pytest.raises(ValueError, match=r"incompatible .*lineage"):
        compile_workflow_file(workflow_path, outdir=tmp_path / "build")


def _inline_plan(
    registry: dict[str, Any],
    stages: list[dict[str, Any]],
    *,
    fields: list[str],
    variations: list[dict[str, Any]],
    stage_updates: dict[str, dict[str, Any]] | None = None,
) -> Any:
    normalized = normalize_workflow(
        {
            "version": "1.0",
            "registry": registry,
            "data": {
                "datasets": [
                    {"name": "sample", "files": ["sample.root"], "eventtype": "mc"}
                ]
            },
            "sources": {
                "events": {
                    "kind": "toy.source",
                    "stream_type": "event_stream",
                    "branches": fields,
                }
            },
            "analysis": {"stages": stages},
            "systematics": {
                "include_nominal": True,
                "variations": variations,
            },
        }
    )
    stage_updates = stage_updates or {}
    for stage in normalized["analysis"]["stages"]:
        update = stage_updates.get(str(stage.get("id") or ""))
        if update:
            stage.update(update)
    _, plan = build_plan_from_normalized(normalized)
    return plan


def _write_inline_export_workflow(
    tmp_path: Any,
    registry: dict[str, Any],
    *,
    variations: list[dict[str, Any]],
    anchor_op: str = "toy.scale",
    anchor_applies_to: dict[str, Any] | None = None,
    datasets: list[dict[str, Any]] | None = None,
    stages: list[dict[str, Any]] | None = None,
    stage_updates: dict[str, dict[str, Any]] | None = None,
) -> Any:
    workflow = {
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
                "branches": ["pt"],
            }
        },
        "analysis": {
            "stages": stages
            or [
                _scale("A", source="pt", output="a_pt"),
                {
                    **_scale("B", source="a_pt", output="b_pt", factor=2),
                    "op": anchor_op,
                    **(
                        {"applies_to": anchor_applies_to}
                        if anchor_applies_to is not None
                        else {}
                    ),
                },
                {
                    **_scale("C", source="b_pt", output="c_pt", factor=3),
                    "write": [{"kind": "toy.write", "path": "output.json"}],
                },
            ]
        },
        "systematics": {
            "include_nominal": True,
            "variations": variations,
        },
    }
    if stage_updates:
        normalized = normalize_workflow(workflow)
        for stage in normalized["analysis"]["stages"]:
            update = stage_updates.get(str(stage.get("id") or ""))
            if update:
                stage.update(update)
        workflow = normalized
    workflow_path = tmp_path / "workflow.yaml"
    workflow_path.write_text(yaml.safe_dump(workflow, sort_keys=False), encoding="utf-8")
    return workflow_path


def _large_inline_variation_workflow(toy_registry: dict[str, Any]) -> dict[str, Any]:
    registry = _registry_with_collection_ops(toy_registry)
    stages: list[dict[str, Any]] = []
    source = "pt"
    for index in range(46):
        output = f"f{index}"
        stage = _scale(f"S{index:02d}", source=source, output=output)
        if index == 45:
            stage["write"] = [{"kind": "toy.write", "path": "output.json"}]
        stages.append(stage)
        source = output

    variations = [
        {
            "name": f"v{index:02d}",
            "mode": "inline",
            "anchor": "S00",
            "patch": {"params": {"factor": index + 2}},
            "export": {f"f45_v{index:02d}": "f45"},
        }
        for index in range(22)
    ]
    return {
        "version": "1.0",
        "registry": registry,
        "data": {
            "datasets": [
                {"name": "sample", "files": ["sample.root"], "eventtype": "mc"}
            ]
        },
        "sources": {
            "events": {
                "kind": "toy.source",
                "stream_type": "event_stream",
                "branches": ["pt"],
            }
        },
        "analysis": {"stages": stages},
        "systematics": {"include_nominal": True, "variations": variations},
    }


def _registry_with_collection_ops(registry: dict[str, Any]) -> dict[str, Any]:
    registry = deepcopy(registry)
    registry["transforms"]["hep.project_fields"] = {
        "spec": "tests.toy_components.transforms:TOY_PROJECT_FIELDS_SPEC",
        "impl": "tests.toy_components.transforms:run_toy_project_fields",
    }
    registry["transforms"]["hep.merge_fields"] = {
        "spec": "tests.toy_components.transforms:TOY_MERGE_FIELDS_SPEC",
        "impl": "tests.toy_components.transforms:run_toy_merge_fields",
    }
    return registry


def _registry_with_product(registry: dict[str, Any]) -> dict[str, Any]:
    registry = deepcopy(registry)
    registry["transforms"]["toy.product"] = {
        "spec": "tests.toy_components.transforms:TOY_PRODUCT_SPEC",
        "impl": "tests.toy_components.transforms:run_toy_product",
    }
    return registry


def _registry_with_strict_merge(registry: dict[str, Any]) -> dict[str, Any]:
    registry = deepcopy(registry)
    registry["transforms"]["toy.strict_merge_fields"] = {
        "spec": "tests.toy_components.transforms:TOY_STRICT_MERGE_FIELDS_SPEC",
        "impl": "tests.toy_components.transforms:run_toy_merge_fields",
    }
    return registry


def _output_payload(build_dir: Any) -> dict[str, Any]:
    direct = build_dir / "artifacts" / "files" / "output.json"
    if direct.exists():
        path = direct
    else:
        [path] = sorted((build_dir / "artifacts" / "files" / "output").glob("*/*.json"))
    return json.loads(path.read_text(encoding="utf-8"))


def _origin_count(origin: dict[str, Any]) -> int:
    if origin.get("kind") == "stream_scoped":
        return len(origin["origins"])
    return 1


def _time_phase(
    timings: dict[str, float],
    name: str,
    func: Any,
    *args: Any,
    **kwargs: Any,
) -> Any:
    start = time.perf_counter()
    try:
        return func(*args, **kwargs)
    finally:
        timings[name] = time.perf_counter() - start


def _compiler_benchmark_metrics(
    *,
    plan: ExecutionPlan,
    edge_count: int,
    timings: dict[str, float],
    deps_path: Path,
    plan_yaml_path: Path,
) -> dict[str, Any]:
    origins = dict(plan.data_flow.get("origins") or {})
    return {
        "node_count": len(plan.nodes),
        "edge_count": edge_count,
        "variation_clone_count": sum(
            1
            for node in plan.nodes
            if isinstance(node.meta, dict) and node.meta.get("variation_of")
        ),
        "compile_time_seconds": round(timings.get("compile_total", 0.0), 6),
        "phase_seconds": {
            name: round(timings.get(name, 0.0), 6)
            for name in [
                "normalize_workflow",
                "lower_workflow_to_graph",
                "inline_variation_expansion",
                "build_execution_plan",
                "preliminary_infer_data_flow",
                "parameter_hooks",
                "final_infer_data_flow",
                "artifact_serialization",
            ]
        },
        "deps_yaml_size_bytes": deps_path.stat().st_size,
        "plan_yaml_size_bytes": plan_yaml_path.stat().st_size,
        "origin_field_count": len(origins),
        "origin_entry_count": sum(
            _origin_count(dict(origin)) for origin in origins.values()
        ),
    }


def _scale(
    node_id: str,
    *,
    source: str,
    output: str = "scaled_pt",
    upstream: str | None = None,
    factor: int | float | None = None,
) -> dict[str, Any]:
    params: dict[str, Any] = {"source": source, "output": output}
    if factor is not None:
        params["factor"] = factor
    stage = {
        "id": node_id,
        "op": "toy.scale",
        "params": params,
    }
    if upstream is not None:
        stage["from"] = upstream
    return stage


def _input_node(plan: ExecutionPlan, node_id: str, input_name: str = "stream") -> str:
    node = plan.get_node(node_id)
    for ref in node.inputs:
        if ref.input_name == input_name:
            return ref.node_id
    raise AssertionError(f"{node_id!r} has no {input_name!r} input")


def _lineage(plan: ExecutionPlan, node_id: str) -> str:
    return plan.data_flow["_stream_lineage"][f"{node_id}:stream"]["identity"]
