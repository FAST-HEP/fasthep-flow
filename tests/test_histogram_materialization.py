from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import networkx as nx

from hepflow.build_layout import ensure_build_layout
from hepflow.compiler.artifacts import write_compile_artifacts
from hepflow.model.plan import ExecutionNode, ExecutionPlan, PlanInputRef
from hepflow.model.products import OperationResult, ProductHandlerEntry, ProductRef
from hepflow.registry.runtime import RuntimeRegistry
from hepflow.runtime.engine import _store_node_outputs, merge_partition_value_stores
from hepflow.runtime.materialize import materialize_final_products


def test_product_ref_model_records_operation_product_contract() -> None:
    ref = ProductRef(
        name="hist",
        kind="histogram",
        scope="final",
        format="pkl",
        path="artifacts/histograms/NumberMuons.pkl",
        metadata={"producer": "stage.NumberMuons"},
    )

    assert ref.name == "hist"
    assert ref.kind == "histogram"
    assert ref.scope == "final"
    assert ref.format == "pkl"
    assert ref.path == "artifacts/histograms/NumberMuons.pkl"
    assert ref.metadata == {"producer": "stage.NumberMuons"}


def test_operation_result_products_are_stored_by_output_name() -> None:
    value_store: dict[tuple[str, str], Any] = {}

    _store_node_outputs(
        "stage.NumberMuons",
        {"hist": "histogram"},
        OperationResult(
            products={"hist": {"bins": [1, 2, 3]}},
            product_refs=[
                ProductRef(
                    name="hist",
                    kind="histogram",
                    scope="final",
                    format="pkl",
                )
            ],
        ),
        value_store,
    )

    assert value_store[("stage.NumberMuons", "hist")] == {"bins": [1, 2, 3]}


def test_final_product_materialization_goes_through_handler(tmp_path: Path) -> None:
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
    calls: list[dict[str, Any]] = []

    def materialize(value: Any, *, node: Any, output_name: str, outdir: str | Path):
        calls.append(
            {
                "value": value,
                "node": node.id,
                "output_name": output_name,
                "outdir": str(outdir),
            }
        )
        product = Path(outdir) / "artifacts" / "histograms" / "NumberMuons.pkl"
        product.parent.mkdir(parents=True, exist_ok=True)
        product.write_text("handled", encoding="utf-8")
        item = {
            "id": "NumberMuons",
            "path": "artifacts/histograms/NumberMuons.pkl",
            "producer": node.id,
        }
        (product.parent / "manifest.json").write_text(
            json.dumps({"histograms": [item]}, indent=2) + "\n",
            encoding="utf-8",
        )
        return {"value": {"handled": value}, "items": [item]}

    items = materialize_final_products(
        plan,
        value_store=value_store,
        outdir=tmp_path,
        runtime_registry=RuntimeRegistry(
            product_handlers={
                "histogram": ProductHandlerEntry(materialize=materialize)
            }
        ),
    )

    assert calls == [
        {
            "value": {"bins": [1, 2, 3]},
            "node": "stage.NumberMuons",
            "output_name": "hist",
            "outdir": str(tmp_path),
        }
    ]
    assert value_store[("stage.NumberMuons", "hist")] == {
        "handled": {"bins": [1, 2, 3]}
    }
    assert items == [
        {
            "id": "NumberMuons",
            "path": "artifacts/histograms/NumberMuons.pkl",
            "producer": "stage.NumberMuons",
        }
    ]
    assert (tmp_path / "artifacts" / "histograms" / "NumberMuons.pkl").read_text(
        encoding="utf-8"
    ) == "handled"


def test_partition_product_merge_goes_through_handler() -> None:
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
    calls: list[list[Any]] = []

    def merge(values: list[Any], **_: Any) -> Any:
        calls.append(values)
        return {"merged": values}

    merged = merge_partition_value_stores(
        plan,
        [
            {("stage.NumberMuons", "hist"): {"part": 1}},
            {("stage.NumberMuons", "hist"): {"part": 2}},
        ],
        runtime_registry=RuntimeRegistry(
            product_handlers={"histogram": ProductHandlerEntry(merge=merge)}
        ),
    )

    assert calls == [[{"part": 1}, {"part": 2}]]
    assert merged[("stage.NumberMuons", "hist")] == {
        "merged": [{"part": 1}, {"part": 2}]
    }

def test_render_spec_does_not_define_histogram_product_contract(tmp_path: Path) -> None:
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
        tmp_path / "render" / "specs" / "render_NumberMuons_0.yaml"
    ).read_text(encoding="utf-8")
    assert "product:" not in spec
    assert "products:" not in spec


def test_render_spec_does_not_define_cutflow_product_contract(tmp_path: Path) -> None:
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
        tmp_path / "render" / "specs" / "render_EventSelection_0.yaml"
    ).read_text(encoding="utf-8")
    assert "product:" not in spec
    assert "products:" not in spec
