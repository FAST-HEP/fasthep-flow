from __future__ import annotations

import json
from pathlib import Path

import networkx as nx

from hepflow.build_layout import ensure_build_layout
from hepflow.compiler.artifacts import write_compile_artifacts
from hepflow.model.plan import ExecutionNode, ExecutionPlan, PlanInputRef
from hepflow.runtime.materialize import (
    materialize_final_cutflows,
    materialize_final_histograms,
)
from hepflow.utils import read_pickle


def test_final_histogram_product_and_manifest_are_written(tmp_path: Path) -> None:
    plan = ExecutionPlan()
    plan.add_node(
        ExecutionNode(
            id="stage.NumberMuons",
            graph_node_id="stage.NumberMuons",
            role="transform",
            impl="hep.hist",
            outputs={"hist": "histogram"},
            meta={"stage_id": "NumberMuons"},
        )
    )
    value_store = {("stage.NumberMuons", "hist"): {"bins": [1, 2, 3]}}

    items = materialize_final_histograms(
        plan,
        value_store=value_store,
        outdir=tmp_path,
    )

    product = tmp_path / "artifacts" / "histograms" / "NumberMuons.pkl"
    manifest = tmp_path / "artifacts" / "histograms" / "manifest.json"
    assert read_pickle(product) == {"bins": [1, 2, 3]}
    assert json.loads(manifest.read_text(encoding="utf-8")) == {
        "histograms": [
            {
                "id": "NumberMuons",
                "path": "artifacts/histograms/NumberMuons.pkl",
                "producer": "stage.NumberMuons",
            }
        ]
    }
    assert items == [
        {
            "id": "NumberMuons",
            "path": "artifacts/histograms/NumberMuons.pkl",
            "producer": "stage.NumberMuons",
        }
    ]
    assert not (tmp_path / "debug" / "partitions" / "histograms").exists()


def test_render_spec_includes_histogram_product_path(tmp_path: Path) -> None:
    plan = ExecutionPlan()
    plan.add_node(
        ExecutionNode(
            id="stage.NumberMuons",
            graph_node_id="stage.NumberMuons",
            role="transform",
            impl="hep.hist",
            outputs={"hist": "histogram"},
            meta={"stage_id": "NumberMuons"},
        )
    )
    plan.add_node(
        ExecutionNode(
            id="render.NumberMuons.0",
            graph_node_id="render.NumberMuons.0",
            role="sink",
            impl="hep.render.heatmap2d",
            inputs=[
                PlanInputRef(
                    node_id="stage.NumberMuons",
                    output_name="hist",
                    input_name="target",
                )
            ],
            params={
                "out": "NumberMuons_heatmap",
                "spec": {
                    "op": "hep.render.heatmap2d",
                    "axes": {
                        "x": {"name": "x"},
                        "y": {"name": "y"},
                    },
                },
            },
        )
    )

    ensure_build_layout(tmp_path)
    write_compile_artifacts(plan=plan, graph=nx.DiGraph(), outdir=tmp_path)

    spec = (
        tmp_path
        / "render"
        / "specs"
        / "render_NumberMuons_0.yaml"
    ).read_text(encoding="utf-8")
    assert "path: artifacts/histograms/NumberMuons.pkl" in spec
    assert "kind: histogram" in spec


def test_final_cutflow_product_and_manifest_are_written(tmp_path: Path) -> None:
    plan = ExecutionPlan()
    plan.add_node(
        ExecutionNode(
            id="stage.EventSelection",
            graph_node_id="stage.EventSelection",
            role="transform",
            impl="hep.selection.cutflow",
            outputs={"stream": "event_stream", "cutflow": "cutflow"},
            meta={"stage_id": "EventSelection"},
        )
    )
    value_store = {
        ("stage.EventSelection", "cutflow"): {
            "cutflows": [
                {
                    "dataset": "data",
                    "cuts": [{"name": "All[0]", "n": 12}],
                }
            ]
        }
    }

    items = materialize_final_cutflows(
        plan,
        value_store=value_store,
        outdir=tmp_path,
    )

    product = tmp_path / "artifacts" / "cutflows" / "EventSelection.json"
    manifest = tmp_path / "artifacts" / "cutflows" / "manifest.json"
    assert json.loads(product.read_text(encoding="utf-8")) == {
        "cutflows": [
            {
                "dataset": "data",
                "cuts": [{"name": "All[0]", "n": 12}],
            }
        ]
    }
    assert json.loads(manifest.read_text(encoding="utf-8")) == {
        "cutflows": [
            {
                "id": "EventSelection",
                "path": "artifacts/cutflows/EventSelection.json",
                "producer": "stage.EventSelection",
            }
        ]
    }
    assert items == [
        {
            "id": "EventSelection",
            "path": "artifacts/cutflows/EventSelection.json",
            "producer": "stage.EventSelection",
        }
    ]
    assert not (tmp_path / "debug" / "partitions" / "cutflows").exists()


def test_render_spec_includes_cutflow_product_path(tmp_path: Path) -> None:
    plan = ExecutionPlan()
    plan.add_node(
        ExecutionNode(
            id="stage.EventSelection",
            graph_node_id="stage.EventSelection",
            role="transform",
            impl="hep.selection.cutflow",
            outputs={"stream": "event_stream", "cutflow": "cutflow"},
            meta={"stage_id": "EventSelection"},
        )
    )
    plan.add_node(
        ExecutionNode(
            id="render.EventSelection.0",
            graph_node_id="render.EventSelection.0",
            role="sink",
            impl="hep.render.cutflow_csv",
            inputs=[
                PlanInputRef(
                    node_id="stage.EventSelection",
                    output_name="cutflow",
                    input_name="target",
                )
            ],
            params={
                "out": "EventSelection.csv",
                "spec": {"op": "hep.render.cutflow_csv"},
            },
        )
    )

    ensure_build_layout(tmp_path)
    write_compile_artifacts(plan=plan, graph=nx.DiGraph(), outdir=tmp_path)

    spec = (
        tmp_path
        / "render"
        / "specs"
        / "render_EventSelection_0.yaml"
    ).read_text(encoding="utf-8")
    assert "path: artifacts/cutflows/EventSelection.json" in spec
    assert "kind: cutflow" in spec
