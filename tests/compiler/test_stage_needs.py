from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
import yaml

from hepflow.api import run_workflow_file
from hepflow.compiler.d2_graph import lowered_graph_to_d2
from hepflow.compiler.graph_artifacts import (
    _lowered_graph_to_dot,
    _lowered_graph_to_mermaid,
)
from hepflow.compiler.lower_graph import lower_workflow_to_graph
from hepflow.compiler.normalize import normalize_workflow
from hepflow.compiler.plan import build_plan_from_normalized
from hepflow.model.plan_applicability import resolve_active_input_ref


def test_no_needs_keeps_implicit_previous_stage_dependency(
    toy_registry: dict[str, Any],
) -> None:
    graph = _lower_graph(
        toy_registry,
        [
            _stage("A"),
            _stage("B"),
            _stage("C"),
        ],
    )

    assert graph.has_edge("read.events", "stage.A")
    assert graph.has_edge("stage.A", "stage.B")
    assert graph.has_edge("stage.B", "stage.C")


def test_single_need_replaces_implicit_previous_stage_dependency(
    toy_registry: dict[str, Any],
) -> None:
    graph = _lower_graph(
        toy_registry,
        [
            _stage("A"),
            _stage("B"),
            _stage("C", needs=["A"]),
        ],
    )

    assert graph.has_edge("stage.A", "stage.C")
    assert not graph.has_edge("stage.B", "stage.C")
    assert graph.edges["stage.A", "stage.C"]["input_name"] == "stream"


def test_multiple_needs_add_all_dependencies(toy_registry: dict[str, Any]) -> None:
    graph = _lower_graph(
        toy_registry,
        [
            _stage("A"),
            _stage("B", needs=[]),
            _stage("C", needs=["A", "B"]),
        ],
    )

    assert graph.has_edge("stage.A", "stage.C")
    assert graph.has_edge("stage.B", "stage.C")
    assert graph.in_degree("stage.C") == 2


def test_empty_needs_suppresses_implicit_stage_dependency(
    toy_registry: dict[str, Any],
) -> None:
    graph = _lower_graph(
        toy_registry,
        [
            _stage("A"),
            _stage("B"),
            _stage("C", needs=[]),
        ],
    )

    assert not graph.has_edge("stage.B", "stage.C")
    assert list(graph.in_edges("stage.C")) == []


def test_needs_plus_from_preserves_ordering_and_product_binding(
    toy_registry: dict[str, Any],
) -> None:
    graph, plan = _build_plan(
        toy_registry,
        [
            _stage("Prepare"),
            _stage("HistA", op="hep.hist"),
            {
                "id": "Render",
                "op": "hep.hist",
                "needs": ["Prepare"],
                "from": [{"node": "HistA", "port": "hist", "as": "hist"}],
            },
        ],
    )
    render = plan.get_node("stage.Render")

    assert graph.has_edge("stage.Prepare", "stage.Render")
    assert graph.has_edge("stage.HistA", "stage.Render")
    assert {ref.node_id for ref in render.inputs} == {"stage.Prepare", "stage.HistA"}
    assert {
        (ref.node_id, ref.output_name, ref.input_name) for ref in render.inputs
    } == {
        ("stage.Prepare", "stream", "dependency"),
        ("stage.HistA", "hist", "hist"),
    }


def test_needs_plus_spec_derived_data_flow_are_both_preserved(
    toy_registry: dict[str, Any],
) -> None:
    _, plan = _build_plan(
        toy_registry,
        [
            _stage("Produce", params={"source": "pt", "output": "scaled_pt"}),
            _stage(
                "Consume",
                needs=["Produce"],
                params={"source": "scaled_pt", "output": "copied_pt"},
            ),
        ],
    )
    consume = plan.get_node("stage.Consume")

    assert [(ref.node_id, ref.input_name) for ref in consume.inputs] == [
        ("stage.Produce", "stream")
    ]
    assert plan.data_flow["consumers"]["scaled_pt"] == ["stage.Consume"]


def test_duplicate_needs_and_from_edge_keeps_specific_input_metadata(
    toy_registry: dict[str, Any],
) -> None:
    graph, plan = _build_plan(
        toy_registry,
        [
            _stage("A"),
            {
                **_stage("B", needs=["A"]),
                "from": [{"node": "A", "port": "stream", "as": "stream"}],
            },
        ],
    )
    node = plan.get_node("stage.B")

    assert graph.in_degree("stage.B") == 1
    assert [(ref.node_id, ref.output_name, ref.input_name) for ref in node.inputs] == [
        ("stage.A", "stream", "stream")
    ]


def test_dependency_only_edges_are_not_bound_as_runtime_inputs(
    toy_registry: dict[str, Any],
    tmp_path: Path,
) -> None:
    workflow = _workflow(
        toy_registry,
        [
            _stage("A", params={"source": "pt", "output": "a"}),
            {
                **_stage("B", needs=["A"], params={"source": "a", "output": "b"}),
                "from": "A",
            },
            {
                **_stage("C", needs=["A", "B"], params={"source": "b", "output": "c"}),
                "from": "B",
            },
        ],
    )
    workflow_path = tmp_path / "workflow.yaml"
    workflow_path.write_text(yaml.safe_dump(workflow, sort_keys=False), encoding="utf-8")

    result = run_workflow_file(workflow_path, outdir=tmp_path / "build")

    assert result.success


def test_empty_event_stream_still_publishes_stream_value(
    toy_registry: dict[str, Any],
    tmp_path: Path,
) -> None:
    workflow = _workflow(
        toy_registry,
        [
            _stage("Scale", params={"source": "pt", "output": "scaled_pt"}),
            _stage("Hist", op="hep.hist", needs=["Scale"]),
            {
                "id": "WriteEmpty",
                "role": "sink",
                "op": "toy.write",
                "needs": ["Scale"],
                "params": {"path": "empty.json"},
            },
        ],
        datasets={"empty": {"files": ["empty.root"], "nevents": "0"}},
    )
    workflow_path = tmp_path / "workflow.yaml"
    workflow_path.write_text(yaml.safe_dump(workflow, sort_keys=False), encoding="utf-8")

    result = run_workflow_file(workflow_path, outdir=tmp_path / "build")

    assert result.success
    summary = yaml.safe_load((tmp_path / "build" / "run_summary.yaml").read_text())
    [partition] = summary["partitions"]
    assert partition["partition"] == {
        "id": "events__empty__0",
        "dataset": "empty",
        "file": "empty.root",
        "source": "events",
        "part": "0_0",
        "start": 0,
        "stop": 0,
    }
    assert {
        (output["node"], output["port"], output["kind"])
        for output in partition["outputs"]
    } >= {
        ("stage.Scale", "stream", "event_stream"),
        ("stage.WriteEmpty", "artifact", "artifact"),
    }

    provenance_manifest = json.loads(
        (tmp_path / "build" / "artifacts" / "provenance" / "manifest.json").read_text(
            encoding="utf-8"
        )
    )
    [record] = provenance_manifest["records"]
    assert record["node_id"] == "stage.WriteEmpty"
    artifact_path = tmp_path / "build" / record["artifact"]
    assert artifact_path.exists()
    assert json.loads(artifact_path.read_text(encoding="utf-8")) == {
        "dataset": "empty",
        "params": {"stream_type": "event_stream"},
        "pt": [],
        "scaled_pt": [],
    }


def test_unknown_needed_stage_id_fails_clearly(toy_registry: dict[str, Any]) -> None:
    with pytest.raises(
        ValueError,
        match=r"analysis\.stages\[1\]\.needs references unknown stage id 'Missing'",
    ):
        normalize_workflow(
            _workflow(toy_registry, [_stage("A"), _stage("B", needs=["Missing"])])
        )


def test_self_dependency_fails_clearly(toy_registry: dict[str, Any]) -> None:
    with pytest.raises(
        ValueError,
        match=r"analysis\.stages\[0\]\.needs references its own stage id 'A'",
    ):
        normalize_workflow(_workflow(toy_registry, [_stage("A", needs=["A"])]))


def test_needs_cycle_fails_dag_validation(toy_registry: dict[str, Any]) -> None:
    workflow = normalize_workflow(
        _workflow(
            toy_registry,
            [
                _stage("A", needs=["B"]),
                _stage("B", needs=["A"]),
            ],
        )
    )

    with pytest.raises(ValueError, match="Lowered graph contains a cycle"):
        lower_workflow_to_graph(workflow)


def test_needs_keeps_existing_inactive_node_bypass_semantics(
    toy_registry: dict[str, Any],
) -> None:
    _, plan = _build_plan(
        toy_registry,
        [
            _stage("A", applies_to={"eventtype": "mc"}),
            _stage("B", needs=["A"]),
        ],
        datasets={
            "data_sample": {
                "files": ["data.root"],
                "eventtype": "data",
            }
        },
    )
    ref = plan.get_node("stage.B").inputs[0]

    resolved = resolve_active_input_ref(
        plan,
        ref,
        dataset=plan.context["datasets"]["data_sample"],
    )

    assert ref.node_id == "stage.A"
    assert resolved.node_id == "read.events"
    assert resolved.input_name == "stream"


def test_normalized_workflow_preserves_explicit_needs(
    toy_registry: dict[str, Any],
) -> None:
    normalized = normalize_workflow(
        _workflow(
            toy_registry,
            [
                _stage("A"),
                _stage("B", needs="A"),
                _stage("C", needs=[]),
            ],
        )
    )

    stages = normalized["analysis"]["stages"]
    assert stages[1]["needs"] == ["A"]
    assert stages[2]["needs"] == []


def test_graph_renderers_show_branched_needs_topology(
    toy_registry: dict[str, Any],
) -> None:
    graph = _lower_graph(
        toy_registry,
        [
            _stage("A"),
            _stage("B", needs=[]),
            _stage("C", needs=["A", "B"]),
        ],
    )

    mermaid = _lowered_graph_to_mermaid(graph)
    dot = _lowered_graph_to_dot(graph)
    d2 = lowered_graph_to_d2(graph)

    assert "stage_A -->|stream -> dependency| stage_C" in mermaid
    assert "stage_B -->|stream -> dependency| stage_C" in mermaid
    assert '"stage.A" -> "stage.C" [label="stream"]' in dot
    assert '"stage.B" -> "stage.C" [label="stream"]' in dot
    assert '"stage.A" -> "stage.C"' in d2
    assert '"stage.B" -> "stage.C"' in d2


def _lower_graph(
    toy_registry: dict[str, Any],
    stages: list[dict[str, Any]],
):
    return lower_workflow_to_graph(normalize_workflow(_workflow(toy_registry, stages)))


def _build_plan(
    toy_registry: dict[str, Any],
    stages: list[dict[str, Any]],
    *,
    datasets: dict[str, Any] | None = None,
):
    return build_plan_from_normalized(
        normalize_workflow(_workflow(toy_registry, stages, datasets=datasets))
    )


def _workflow(
    toy_registry: dict[str, Any],
    stages: list[dict[str, Any]],
    *,
    datasets: dict[str, Any] | None = None,
) -> dict[str, Any]:
    workflow: dict[str, Any] = {
        "version": "1.0",
        "registry": toy_registry,
        "sources": {
            "events": {
                "kind": "toy.source",
                "stream_type": "event_stream",
            }
        },
        "analysis": {"stages": stages},
    }
    if datasets is not None:
        workflow["data"] = {"datasets": datasets}
    return workflow


def _stage(
    stage_id: str,
    *,
    op: str = "toy.scale",
    needs: list[str] | str | None = None,
    params: dict[str, Any] | None = None,
    applies_to: dict[str, Any] | None = None,
) -> dict[str, Any]:
    stage = {
        "id": stage_id,
        "op": op,
        "params": params or {"factor": 2},
    }
    if needs is not None:
        stage["needs"] = needs
    if applies_to is not None:
        stage["applies_to"] = applies_to
    return stage
