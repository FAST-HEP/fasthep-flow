from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import networkx as nx

from hepflow.build_layout import ensure_build_layout
from hepflow.compiler.artifacts import write_compile_artifacts
from hepflow.model.io import OutputResult
from hepflow.model.plan import (
    ExecutionNode,
    ExecutionPartition,
    ExecutionPlan,
    PlanInputRef,
)
from hepflow.model.products import OperationResult, ProductHandlerEntry, ProductRef
from hepflow.registry.runtime import RuntimeRegistry
from hepflow.runtime.engine import (
    _store_node_outputs,
    merge_partition_value_stores,
    merge_partition_value_stores_for_dataset,
)
from hepflow.runtime.materialize import materialize_final_products
from hepflow.runtime.provenance import (
    format_provenance_artifact,
    format_provenance_graph,
    format_provenance_summary,
    write_artifact_provenance_records,
)
from hepflow.runtime.writer_manifests import write_writer_manifests


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


def test_writer_manifests_emit_generic_provenance_records(tmp_path: Path) -> None:
    ensure_build_layout(tmp_path)
    plan = ExecutionPlan(provenance={"run_id": "run-123"})
    plan.add_node(
        ExecutionNode(
            id="write.SelectedEvents.0",
            graph_node_id="write.SelectedEvents.0",
            role="sink",
            impl="root_tree",
            inputs=[
                PlanInputRef(
                    node_id="stage.SelectedEvents",
                    output_name="stream",
                    input_name="target",
                )
            ],
            outputs={"artifact": "artifact"},
        )
    )
    output = OutputResult(
        kind="artifact",
        path=str(
            tmp_path
            / "artifacts"
            / "files"
            / "selected"
            / "data"
            / "0_0.root"
        ),
        format="root",
        metadata={
            "writer_manifest": {
                "kind": "root_tree",
                "name": "selected",
                "node_id": "write.SelectedEvents.0",
                "input_node": "stage.SelectedEvents",
                "tree": "events",
                "path": "artifacts/files/selected/data/0_0.root",
                "path_type": "relative_to_outdir",
                "dataset": "data",
                "partition": 0,
                "attempt": 0,
                "entries": 12,
                "size_bytes": 345,
            }
        },
    )
    store = {("write.SelectedEvents.0", "artifact"): output}
    partition = ExecutionPartition(
        id="events__data__0",
        dataset="data",
        source="events",
        file="data/CMS/Zmumu/data.root",
        part="0_0",
    )

    write_writer_manifests(
        plan, stores=[store], partitions=[partition], outdir=tmp_path
    )

    writer_manifest = json.loads(
        (
            tmp_path / "artifacts" / "files" / "selected" / "manifest.json"
        ).read_text(encoding="utf-8")
    )
    assert writer_manifest["total_entries"] == 12
    writer_file = writer_manifest["datasets"]["data"]["files"][0]
    assert writer_file["path_type"] == "relative_to_outdir"

    provenance_manifest = json.loads(
        (tmp_path / "artifacts" / "provenance" / "manifest.json").read_text(
            encoding="utf-8"
        )
    )
    assert provenance_manifest["version"] == "1.0"
    assert provenance_manifest["run_id"] == "run-123"
    assert provenance_manifest["execution"] == "artifacts/provenance/execution.json"
    assert provenance_manifest["records"] == [
        {
            "artifact": "artifacts/files/selected/data/0_0.root",
            "kind": "root_tree",
            "node_id": "write.SelectedEvents.0",
            "record": provenance_manifest["records"][0]["record"],
            "record_hash": provenance_manifest["records"][0]["record_hash"],
        }
    ]
    assert provenance_manifest["records"][0]["record_hash"].startswith("sha256:")
    assert writer_file["provenance"] == {
        "record": provenance_manifest["records"][0]["record"],
        "record_hash": provenance_manifest["records"][0]["record_hash"],
    }
    assert output.provenance == writer_file["provenance"]
    assert output.metadata["provenance"] == writer_file["provenance"]
    assert output.metadata["writer_manifest"]["provenance"] == writer_file["provenance"]

    execution = json.loads((tmp_path / provenance_manifest["execution"]).read_text())
    assert execution["version"] == "1.0"
    assert execution["run_id"] == "run-123"
    assert execution["workflow"] == {
        "normalized": "compile/normalized.yaml",
        "graph": "graph/graph.json",
        "plan": "compile/plan.yaml",
    }
    assert "python_version" in execution["execution"]
    assert execution["partitions"] == [
        {
            "id": "events__data__0",
            "dataset": "data",
            "file": "data/CMS/Zmumu/data.root",
            "source": "events",
            "part": "0_0",
            "start": None,
            "stop": None,
        }
    ]
    assert execution["node_executions"] == [
        {
            "id": "write.SelectedEvents.0::events__data__0",
            "node_id": "write.SelectedEvents.0",
            "partition_id": "events__data__0",
        }
    ]

    record_path = tmp_path / provenance_manifest["records"][0]["record"]
    record = json.loads(record_path.read_text(encoding="utf-8"))
    assert record["artifact"] == {
        "path": "artifacts/files/selected/data/0_0.root",
        "path_type": "relative_to_outdir",
        "kind": "root_tree",
    }
    assert "workflow" not in record
    assert "software" not in record
    assert "execution" not in record
    assert "data" not in record
    assert record["producer"] == {
        "node_id": "write.SelectedEvents.0",
        "partition_id": "events__data__0",
        "execution_id": "write.SelectedEvents.0::events__data__0",
    }
    assert record["inputs"] == [{"partition_id": "events__data__0"}]

    summary_text = format_provenance_summary(tmp_path)
    assert "Run ID: run-123" in summary_text
    assert "Records: 1" in summary_text
    assert "Artifact kinds: root_tree=1" in summary_text
    assert "Datasets: data" in summary_text

    artifact_text = format_provenance_artifact(
        tmp_path / "artifacts" / "files" / "selected" / "data" / "0_0.root"
    )
    assert "Artifact: artifacts/files/selected/data/0_0.root" in artifact_text
    assert "node: write.SelectedEvents.0" in artifact_text
    assert "events__data__0 dataset=data source=events" in artifact_text
    assert "file=data/CMS/Zmumu/data.root" in artifact_text
    assert "graph: graph/graph.json" in artifact_text

    graph_dir = tmp_path / "graph"
    graph_dir.mkdir(parents=True, exist_ok=True)
    (graph_dir / "graph.json").write_text(
        json.dumps(
            {
                "nodes": [
                    {
                        "id": "read.events",
                        "payload": (
                            "GraphNode(id='read.events', role='source', "
                            "impl='root_tree', meta={'source_name': 'events'})"
                        ),
                    },
                    {
                        "id": "stage.SelectedEvents",
                        "payload": (
                            "GraphNode(id='stage.SelectedEvents', "
                            "role='transform', impl='hep.selection')"
                        ),
                    },
                    {
                        "id": "write.SelectedEvents.0",
                        "payload": (
                            "GraphNode(id='write.SelectedEvents.0', "
                            "role='sink', impl='root_tree')"
                        ),
                    },
                    {
                        "id": "observe.schema.0",
                        "payload": (
                            "GraphNode(id='observe.schema.0', "
                            "role='observer', impl='hep.schema_snapshot')"
                        ),
                    },
                ],
                "edges": [
                    {"source": "read.events", "target": "stage.SelectedEvents"},
                    {
                        "source": "stage.SelectedEvents",
                        "target": "write.SelectedEvents.0",
                    },
                    {"source": "stage.SelectedEvents", "target": "observe.schema.0"},
                ],
            }
        ),
        encoding="utf-8",
    )
    mermaid = format_provenance_graph(
        tmp_path / "artifacts" / "files" / "selected" / "data" / "0_0.root"
    )
    assert "flowchart TD" in mermaid
    assert "read_events" in mermaid
    assert "stage_SelectedEvents" in mermaid
    assert "write_SelectedEvents_0" in mermaid
    assert "producer" in mermaid
    assert "produces: root_tree" in mermaid
    assert "inputs: data" in mermaid
    assert "observe_schema_0" not in mermaid

    graph_json = json.loads(
        format_provenance_graph(
            tmp_path / "artifacts" / "files" / "selected" / "data" / "0_0.root",
            output_format="json",
        )
    )
    assert [node["id"] for node in graph_json["nodes"]] == [
        "read.events",
        "stage.SelectedEvents",
        "write.SelectedEvents.0",
    ]
    assert graph_json["producer"]["node_id"] == "write.SelectedEvents.0"
    assert graph_json["inputs"] == [{"partition_id": "events__data__0"}]
    assert graph_json["related_records"][0]["artifact"] == (
        "artifacts/files/selected/data/0_0.root"
    )


def test_generic_output_result_gets_provenance_record(tmp_path: Path) -> None:
    ensure_build_layout(tmp_path)
    plan = ExecutionPlan(provenance={"run_id": "run-456"})
    plan.add_node(
        ExecutionNode(
            id="render.SelectedEvents.0",
            graph_node_id="render.SelectedEvents.0",
            role="sink",
            impl="hep.render.hist1d",
            inputs=[
                PlanInputRef(
                    node_id="stage.SelectedEvents",
                    output_name="hist",
                    input_name="target",
                )
            ],
            outputs={"artifact": "artifact"},
        )
    )
    output = OutputResult(
        kind="artifact",
        path="artifacts/plots/selected.png",
        format="png",
    )
    store = {("render.SelectedEvents.0", "artifact"): output}

    write_writer_manifests(plan, stores=[store], outdir=tmp_path)

    provenance_manifest = json.loads(
        (tmp_path / "artifacts" / "provenance" / "manifest.json").read_text(
            encoding="utf-8"
        )
    )
    assert provenance_manifest["records"] == [
        {
            "artifact": "artifacts/plots/selected.png",
            "kind": "png",
            "node_id": "render.SelectedEvents.0",
            "record": provenance_manifest["records"][0]["record"],
            "record_hash": provenance_manifest["records"][0]["record_hash"],
        }
    ]
    assert output.provenance == {
        "record": provenance_manifest["records"][0]["record"],
        "record_hash": provenance_manifest["records"][0]["record_hash"],
    }
    execution = json.loads((tmp_path / provenance_manifest["execution"]).read_text())
    assert execution["node_executions"] == [
        {
            "id": "render.SelectedEvents.0",
            "node_id": "render.SelectedEvents.0",
        }
    ]
    record = json.loads(
        (tmp_path / provenance_manifest["records"][0]["record"]).read_text(
            encoding="utf-8"
        )
    )
    assert record["artifact"] == {
        "path": "artifacts/plots/selected.png",
        "path_type": "relative_to_outdir",
        "kind": "png",
    }
    assert "workflow" not in record
    assert "software" not in record
    assert "execution" not in record
    assert "data" not in record
    assert record["producer"] == {
        "node_id": "render.SelectedEvents.0",
        "execution_id": "render.SelectedEvents.0",
    }
    assert record["inputs"] == []


def test_provenance_record_supports_multiple_input_partitions(tmp_path: Path) -> None:
    ensure_build_layout(tmp_path)
    plan = ExecutionPlan(provenance={"run_id": "run-merged"})
    inputs = [
        {
            "id": "events__data__0",
            "dataset": "data",
            "source": "events",
            "file": "data/a.root",
            "part": "0_0",
            "start": 0,
            "stop": 10,
        },
        {
            "id": "events__data__1",
            "dataset": "data",
            "source": "events",
            "file": "data/b.root",
            "part": "1_0",
            "start": 10,
            "stop": 20,
        },
    ]

    write_artifact_provenance_records(
        plan=plan,
        writer_records=[
            {
                "kind": "root_tree",
                "node_id": "merge.SelectedEvents.0",
                "input_node": "write.SelectedEvents.0",
                "path": "artifacts/files/merged/data.root",
                "path_type": "relative_to_outdir",
                "dataset": "data",
                "partition": 0,
                "attempt": 0,
                "inputs": inputs,
            }
        ],
        outdir=tmp_path,
    )

    manifest = json.loads(
        (tmp_path / "artifacts" / "provenance" / "manifest.json").read_text(
            encoding="utf-8"
        )
    )
    record = json.loads(
        (tmp_path / manifest["records"][0]["record"]).read_text(encoding="utf-8")
    )
    assert record["producer"] == {
        "node_id": "merge.SelectedEvents.0",
        "partition_id": "events__data__0",
        "execution_id": "merge.SelectedEvents.0::events__data__0",
    }
    assert record["inputs"] == [
        {"partition_id": "events__data__0"},
        {"partition_id": "events__data__1"},
    ]


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


def test_dataset_event_stream_merge_goes_through_handler() -> None:
    plan = ExecutionPlan()
    plan.add_node(
        ExecutionNode(
            id="stage.SelectedEvents",
            graph_node_id="stage.SelectedEvents",
            role="transform",
            impl="test.transform",
            outputs={"stream": "event_stream"},
        )
    )
    calls: list[tuple[list[Any], str | None]] = []

    def merge(
        values: list[Any],
        *,
        dataset_name: str | None = None,
        **_: Any,
    ) -> Any:
        calls.append((values, dataset_name))
        return {"merged": values}

    merged = merge_partition_value_stores_for_dataset(
        plan,
        [
            {("stage.SelectedEvents", "stream"): {"part": 1}},
            {("stage.SelectedEvents", "stream"): {"part": 2}},
        ],
        dataset_name="data",
        runtime_registry=RuntimeRegistry(
            product_handlers={"event_stream": ProductHandlerEntry(merge=merge)}
        ),
    )

    assert calls == [
        ([{"part": 1}, {"part": 2}], "data"),
    ]
    assert merged[("stage.SelectedEvents", "stream")] == {
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


def test_dataset_entries_artifact_omits_unknown_nevents(tmp_path: Path) -> None:
    plan = ExecutionPlan(
        context={
            "datasets": {
                "data": {
                    "name": "data",
                    "files": ["data.root"],
                    "nevents": None,
                    "eventtype": "data",
                    "group": None,
                    "meta": {},
                }
            }
        }
    )

    ensure_build_layout(tmp_path)
    write_compile_artifacts(plan=plan, graph=nx.DiGraph(), outdir=tmp_path)

    entries = json.loads(
        (tmp_path / "compile" / "dataset_entries.json").read_text(encoding="utf-8")
    )
    assert entries == {
        "data": {
            "eventtype": "data",
            "files": ["data.root"],
            "meta": {},
            "name": "data",
        }
    }
