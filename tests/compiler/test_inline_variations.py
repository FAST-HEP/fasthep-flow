from __future__ import annotations

from typing import Any

import pytest
import yaml

from hepflow.api import compile_workflow_file, run_plan_file
from hepflow.compiler.inline_variations import (
    InlineVariationBranch,
    add_inline_variation_branch,
)
from hepflow.compiler.normalize import normalize_workflow
from hepflow.compiler.plan import build_plan_from_normalized
from hepflow.model.plan import ExecutionNode, ExecutionPlan, PlanInputRef
from hepflow.runtime.engine import execute_plan_locally


def test_inline_variation_clones_linear_downstream_branch(
    toy_registry: dict[str, Any],
) -> None:
    plan = _plan(
        toy_registry,
        [
            _scale("A", source="pt", output="a_pt"),
            _scale("B", source="a_pt", output="b_pt"),
            _scale("C", source="b_pt", output="c_pt"),
            _scale("D", source="c_pt", output="d_pt"),
        ],
        fields=["pt"],
    )

    result = add_inline_variation_branch(
        plan,
        InlineVariationBranch(
            anchor_node_id="stage.B",
            variation={"name": "jec_up"},
            parameter_patch={"factor": 10},
        ),
    )

    assert result.cloned_nodes == {
        "stage.B": "stage.B@jec_up",
        "stage.C": "stage.C@jec_up",
        "stage.D": "stage.D@jec_up",
    }
    assert _input_node(plan, "stage.B@jec_up") == "stage.A"
    assert _input_node(plan, "stage.C@jec_up") == "stage.B@jec_up"
    assert _input_node(plan, "stage.D@jec_up") == "stage.C@jec_up"
    assert plan.get_node("stage.B@jec_up").impl == plan.get_node("stage.B").impl
    assert plan.get_node("stage.B@jec_up").params["factor"] == 10


def test_inline_variation_clones_branching_downstream_graph(
    toy_registry: dict[str, Any],
) -> None:
    plan = _plan(
        toy_registry,
        [
            _scale("A", source="pt", output="a_pt"),
            _scale("B", source="a_pt", output="b_pt"),
            _scale("Left", source="b_pt", output="left_pt", upstream="B"),
            _scale("Right", source="b_pt", output="right_pt", upstream="B"),
            _scale("AfterLeft", source="left_pt", output="after_left_pt", upstream="Left"),
        ],
        fields=["pt"],
    )

    result = add_inline_variation_branch(
        plan,
        InlineVariationBranch(anchor_node_id="stage.B", variation={"name": "jes_down"}),
    )

    assert result.cloned_nodes == {
        "stage.B": "stage.B@jes_down",
        "stage.Left": "stage.Left@jes_down",
        "stage.Right": "stage.Right@jes_down",
        "stage.AfterLeft": "stage.AfterLeft@jes_down",
    }
    assert _input_node(plan, "stage.Left@jes_down") == "stage.B@jes_down"
    assert _input_node(plan, "stage.Right@jes_down") == "stage.B@jes_down"
    assert _input_node(plan, "stage.AfterLeft@jes_down") == "stage.Left@jes_down"


def test_inline_variation_keeps_shared_nominal_side_input(
    toy_registry: dict[str, Any],
) -> None:
    plan = _manual_side_input_plan(toy_registry)

    add_inline_variation_branch(
        plan,
        InlineVariationBranch(anchor_node_id="stage.B", variation={"name": "up"}),
    )

    cloned_c = plan.get_node("stage.C@up")
    assert {
        (ref.input_name, ref.node_id)
        for ref in cloned_c.inputs
    } == {
        ("stream", "stage.B@up"),
        ("weights", "stage.SharedWeights"),
    }
    assert "stage.SharedWeights@up" not in plan.node_index


def test_inline_variation_preserves_same_field_names_across_streams(
    toy_registry: dict[str, Any],
) -> None:
    plan = _plan(
        toy_registry,
        [
            _scale("A", source="pt", output="a_pt"),
            _scale("B", source="a_pt", output="Jet_pt"),
            _scale("C", source="Jet_pt", output="Selected_pt"),
        ],
        fields=["pt"],
    )

    add_inline_variation_branch(
        plan,
        InlineVariationBranch(anchor_node_id="stage.B", variation={"name": "jer_up"}),
    )

    assert plan.get_node("stage.B@jer_up").params["output"] == "Jet_pt"
    assert plan.get_node("stage.C@jer_up").params["source"] == "Jet_pt"
    assert plan.data_flow["origins"]["Jet_pt"]["kind"] == "stream_scoped"


def test_inline_variation_clone_ids_are_unique(toy_registry: dict[str, Any]) -> None:
    plan = _plan(
        toy_registry,
        [_scale("A", source="pt", output="a_pt"), _scale("B", source="a_pt")],
        fields=["pt"],
    )

    add_inline_variation_branch(
        plan,
        InlineVariationBranch(anchor_node_id="stage.B", variation={"name": "up"}),
    )

    with pytest.raises(ValueError, match="clone id already exists"):
        add_inline_variation_branch(
            plan,
            InlineVariationBranch(anchor_node_id="stage.B", variation={"name": "up"}),
        )


def test_inline_variation_preserves_event_lineage(toy_registry: dict[str, Any]) -> None:
    plan = _plan(
        toy_registry,
        [_scale("A", source="pt", output="a_pt"), _scale("B", source="a_pt")],
        fields=["pt"],
    )

    add_inline_variation_branch(
        plan,
        InlineVariationBranch(anchor_node_id="stage.B", variation={"name": "up"}),
    )

    assert _lineage(plan, "stage.B@up") == _lineage(plan, "stage.B")
    assert _lineage(plan, "stage.B@up") == _lineage(plan, "read.events")


def test_inline_variation_stop_boundary_is_exclusive(
    toy_registry: dict[str, Any],
) -> None:
    plan = _plan(
        toy_registry,
        [
            _scale("A", source="pt", output="a_pt"),
            _scale("B", source="a_pt", output="b_pt"),
            _scale("C", source="b_pt", output="c_pt"),
            _scale("D", source="c_pt", output="d_pt"),
        ],
        fields=["pt"],
    )

    result = add_inline_variation_branch(
        plan,
        InlineVariationBranch(
            anchor_node_id="stage.B",
            variation={"name": "up"},
            stop_before=frozenset({"stage.C"}),
        ),
    )

    assert result.cloned_nodes == {"stage.B": "stage.B@up"}
    assert "stage.C@up" not in plan.node_index
    assert "stage.D@up" not in plan.node_index


def test_inline_variation_does_not_clone_sink_by_default(
    toy_registry: dict[str, Any],
) -> None:
    plan = _plan(
        toy_registry,
        [_scale("A", source="pt", output="a_pt"), _scale("B", source="a_pt")],
        fields=["pt"],
    )
    plan.add_node(
        ExecutionNode(
            id="write.Output",
            graph_node_id="write.Output",
            role="sink",
            impl="toy.write",
            inputs=[
                PlanInputRef(
                    node_id="stage.B",
                    output_name="stream",
                    input_name="target",
                )
            ],
            params={"path": "output.json"},
            outputs={"result": "file"},
        )
    )

    add_inline_variation_branch(
        plan,
        InlineVariationBranch(anchor_node_id="stage.B", variation={"name": "up"}),
    )

    assert "write.Output@up" not in plan.node_index


def test_inline_variation_rejects_incompatible_variation_fan_in(
    toy_registry: dict[str, Any],
) -> None:
    plan = _manual_side_input_plan(toy_registry)
    plan.get_node("stage.SharedWeights").meta["variation"] = {"name": "other"}

    with pytest.raises(ValueError, match="incompatible variation context"):
        add_inline_variation_branch(
            plan,
            InlineVariationBranch(anchor_node_id="stage.B", variation={"name": "up"}),
        )


def test_inline_variation_plan_executes_with_existing_local_runtime(
    toy_registry: dict[str, Any],
) -> None:
    plan = _plan(
        toy_registry,
        [
            _scale("A", source="pt", output="a_pt"),
            _scale("B", source="a_pt", output="b_pt", factor=2),
            _scale("C", source="b_pt", output="c_pt", factor=3),
        ],
        fields=["pt"],
    )

    add_inline_variation_branch(
        plan,
        InlineVariationBranch(
            anchor_node_id="stage.B",
            variation={"name": "up"},
            parameter_patch={"factor": 10},
        ),
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

    value_store = run_plan_file(plan_path, outdir=build_dir).outputs["value_store"]
    store = value_store[0] if isinstance(value_store, list) else value_store
    assert store[("stage.C", "stream")]["c_pt"] == [72, 108, 126, 168]
    assert store[("stage.C@up", "stream")]["c_pt"] == [360, 540, 630, 840]


def _plan(
    registry: dict[str, Any],
    stages: list[dict[str, Any]],
    *,
    fields: list[str],
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
        }
    )
    _, plan = build_plan_from_normalized(normalized)
    return plan


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


def _manual_side_input_plan(registry: dict[str, Any]) -> ExecutionPlan:
    plan = ExecutionPlan()
    plan.registry = registry
    plan.context = {
        "datasets": {"sample": {"name": "sample", "files": ["sample.root"]}},
        "dataset_names": ["sample"],
        "globals": {},
    }
    plan.add_node(
        ExecutionNode(
            id="read.events",
            graph_node_id="read.events",
            role="source",
            impl="toy.source",
            params={"branches": ["pt"]},
            outputs={"stream": "event_stream"},
            meta={"source_name": "events"},
        )
    )
    plan.add_node(
        ExecutionNode(
            id="stage.A",
            graph_node_id="stage.A",
            role="transform",
            impl="toy.scale",
            inputs=[
                PlanInputRef(
                    node_id="read.events",
                    output_name="stream",
                    input_name="stream",
                )
            ],
            params={"source": "pt", "output": "a_pt"},
            outputs={"stream": "event_stream"},
        )
    )
    plan.add_node(
        ExecutionNode(
            id="stage.SharedWeights",
            graph_node_id="stage.SharedWeights",
            role="transform",
            impl="toy.product",
            params={"value": "weights"},
            outputs={"product": "toy_product"},
        )
    )
    plan.add_node(
        ExecutionNode(
            id="stage.B",
            graph_node_id="stage.B",
            role="transform",
            impl="toy.scale",
            inputs=[
                PlanInputRef(
                    node_id="stage.A",
                    output_name="stream",
                    input_name="stream",
                )
            ],
            params={"source": "a_pt", "output": "b_pt"},
            outputs={"stream": "event_stream"},
        )
    )
    plan.add_node(
        ExecutionNode(
            id="stage.C",
            graph_node_id="stage.C",
            role="transform",
            impl="toy.scale",
            inputs=[
                PlanInputRef(
                    node_id="stage.B",
                    output_name="stream",
                    input_name="stream",
                ),
                PlanInputRef(
                    node_id="stage.SharedWeights",
                    output_name="product",
                    input_name="weights",
                ),
            ],
            params={"source": "b_pt", "output": "c_pt"},
            outputs={"stream": "event_stream"},
        )
    )
    return plan
