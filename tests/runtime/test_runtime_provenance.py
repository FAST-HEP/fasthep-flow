from __future__ import annotations

import json
from pathlib import Path

import pytest

from hepflow.model import ResolvedResource
from hepflow.model.plan import ExecutionPartition, ExecutionPlan
from hepflow.runtime.provenance import (
    ExecutionRecord,
    OperationRecord,
    ProvenanceDocument,
    ProvenanceRecorder,
    ProvenanceStore,
    load_provenance_document,
    resolve_operation_resources,
    write_artifact_provenance_records,
    write_provenance_document,
)


def test_operation_provenance_records_symbols_only() -> None:
    recorder = ProvenanceRecorder()

    with recorder.operation_context(
        node_id="stage.Build",
        impl="toy.record",
        role="transform",
        dataset="data",
        partition={"id": "events__data__0"},
    ):
        recorder.record_operation(
            inputs={"symbols": ["pt"]},
            outputs={"symbols": ["scaled_pt"]},
        )

    executions = recorder.serialise_executions()
    execution = executions["stage.Build::events__data__0"]

    assert execution["node_id"] == "stage.Build"
    assert execution["dataset"] == "data"
    assert execution["partition"] == "events__data__0"
    assert execution["operations"] == [
        {
            "inputs": {"symbols": ["pt"]},
            "outputs": {"symbols": ["scaled_pt"]},
        }
    ]


def test_resolved_resource_serialisation_excludes_runtime_value() -> None:
    recorder = ProvenanceRecorder()
    runtime_value = object()

    recorder.record_resource(
        ResolvedResource(
            id="cms.pileup.2024",
            kind="correctionlib",
            value=runtime_value,
            requested_era="RunIII2024Summer24",
            selected_era="2023_Summer23",
            path="/cvmfs/example/puWeights.json.gz",
            correction="Collisions2023",
            fallback=True,
            reason="no 2024 payload",
        )
    )

    resources = recorder.serialise_resources()

    assert resources == {
        "cms.pileup.2024": {
            "kind": "correctionlib",
            "requested_era": "RunIII2024Summer24",
            "selected": {
                "era": "2023_Summer23",
                "path": "/cvmfs/example/puWeights.json.gz",
                "correction": "Collisions2023",
                "fallback": True,
                "reason": "no 2024 payload",
            },
        }
    }
    assert "value" not in json.dumps(resources)


def test_store_validates_resource_references() -> None:
    recorder = ProvenanceRecorder()
    with recorder.operation_context(
        node_id="stage.PileupWeights",
        impl="chip.pileup_weights",
        role="transform",
        dataset="dy",
        partition={"id": "events__dy__0"},
    ):
        recorder.record_operation(
            inputs={"resources": ["cms.pileup.2024"]},
            outputs={"symbols": ["weight_pu_nominal"]},
        )

    with pytest.raises(ValueError, match=r"unresolved resources: cms\.pileup\.2024"):
        recorder.validate()


def test_repeated_operations_preserve_order_and_resources_deduplicate() -> None:
    recorder = ProvenanceRecorder()
    with recorder.operation_context(
        node_id="stage.Build",
        impl="toy.record",
        role="transform",
        dataset="data",
        partition={"id": "events__data__0"},
    ):
        recorder.record_operation(inputs={"symbols": ["a"]}, outputs={"symbols": ["b"]})
        recorder.record_operation(inputs={"symbols": ["b"]}, outputs={"symbols": ["c"]})

    recorder.record_resource(
        ResolvedResource(id="resource", kind="test", value=object(), path="first")
    )
    recorder.record_resource(
        ResolvedResource(id="resource", kind="test", value=object(), path="first")
    )

    execution = recorder.serialise_executions()["stage.Build::events__data__0"]
    assert execution["operations"] == [
        {"inputs": {"symbols": ["a"]}, "outputs": {"symbols": ["b"]}},
        {"inputs": {"symbols": ["b"]}, "outputs": {"symbols": ["c"]}},
    ]
    assert recorder.serialise_resources()["resource"]["selected"]["path"] == "first"


def test_resource_id_conflict_fails_clearly() -> None:
    recorder = ProvenanceRecorder()
    recorder.record_resource(
        ResolvedResource(
            id="resource",
            kind="test",
            value=object(),
            path="logical",
            metadata={
                "selected": {
                    "resolved_path": "payload-a",
                    "sha256": "a" * 64,
                }
            },
        )
    )

    with pytest.raises(ValueError, match="Resource 'resource' changed during execution"):
        recorder.record_resource(
            ResolvedResource(
                id="resource",
                kind="test",
                value=object(),
                path="logical",
                metadata={
                    "selected": {
                        "resolved_path": "payload-b",
                        "sha256": "b" * 64,
                    }
                },
            )
        )


def test_provenance_document_persistence_round_trip(tmp_path: Path) -> None:
    document = ProvenanceDocument(
        version="1.0",
        run_id="run",
        workflow={"graph": "graph/graph.json"},
        software={},
        execution={},
        partitions=[],
        node_executions=[],
    )
    path = tmp_path / "execution.json"

    write_provenance_document(path, document)
    loaded = load_provenance_document(path)

    assert loaded == document


def test_resource_selected_fingerprint_survives_record_round_trip() -> None:
    resource = {
        "kind": "correctionlib",
        "requested_era": "2024_Winter24",
        "selected": {
            "path": "logical.json.gz",
            "configured_path": "logical.json.gz",
            "resolved_path": "payload.json.gz",
            "sha256": "a" * 64,
            "size_bytes": 123,
            "correction": "AK4PUPPI_Tight",
            "fallback": False,
            "correction_version": 2,
        },
    }

    round_tripped = ProvenanceDocument.from_obj(
        {
            "version": "1.0",
            "run_id": "run",
            "workflow": {},
            "software": {},
            "execution": {},
            "partitions": [],
            "node_executions": [],
            "resources": {"resource": resource},
        }
    ).to_record()["resources"]["resource"]

    assert round_tripped == resource


def test_inspection_resolves_operation_resources() -> None:
    execution = {
        "resources": {
            "cms.pileup.2024": {
                "kind": "correctionlib",
                "selected": {"path": "/cvmfs/example/puWeights.json.gz"},
            }
        }
    }
    operation = {
        "inputs": {
            "symbols": ["Pileup_nTrueInt"],
            "resources": ["cms.pileup.2024"],
        }
    }

    assert resolve_operation_resources(operation, execution) == {
        "cms.pileup.2024": {
            "kind": "correctionlib",
            "selected": {"path": "/cvmfs/example/puWeights.json.gz"},
        }
    }


def test_runtime_operation_provenance_persists_to_execution_index(
    tmp_path: Path,
) -> None:
    plan = ExecutionPlan()
    plan.partitions = [
        ExecutionPartition(
            id="events__dy__0",
            dataset="dy",
            source="events",
            file="dy.root",
            part="0_0",
        )
    ]
    recorder = ProvenanceRecorder()
    recorder.record_resource(
        ResolvedResource(
            id="cms.pileup.2024",
            kind="correctionlib",
            value=object(),
            requested_era="RunIII2024Summer24",
            selected_era="2023_Summer23",
            path="/cvmfs/example/puWeights.json.gz",
            correction="Collisions2023",
            fallback=True,
            reason="no 2024 payload",
        )
    )
    with recorder.operation_context(
        node_id="stage.PileupWeights",
        impl="chip.pileup_weights",
        role="transform",
        dataset="dy",
        partition={"id": "events__dy__0"},
    ):
        recorder.record_operation(
            inputs={
                "symbols": ["Pileup_nTrueInt"],
                "resources": ["cms.pileup.2024"],
            },
            outputs={"symbols": ["weight_pu_nominal"]},
        )

    write_artifact_provenance_records(
        plan=plan,
        writer_records=[
            {
                "kind": "root_tree",
                "node_id": "write.FilterChannel.0",
                "path": "artifacts/files/out.root",
                "dataset": "dy",
                "partition": 0,
                "attempt": 0,
                "inputs": [plan.partitions[0].to_context()],
            }
        ],
        outdir=tmp_path,
        runtime_provenance=recorder,
    )

    execution = json.loads(
        (tmp_path / "artifacts" / "provenance" / "execution.json").read_text(
            encoding="utf-8"
        )
    )

    assert execution["resources"]["cms.pileup.2024"]["selected"]["fallback"] is True
    assert execution["executions"]["stage.PileupWeights::events__dy__0"][
        "operations"
    ] == [
        {
            "inputs": {
                "symbols": ["Pileup_nTrueInt"],
                "resources": ["cms.pileup.2024"],
            },
            "outputs": {"symbols": ["weight_pu_nominal"]},
        }
    ]


def test_artifact_provenance_records_product_inputs(tmp_path: Path) -> None:
    write_artifact_provenance_records(
        plan=ExecutionPlan(),
        writer_records=[
            {
                "kind": "toy_report",
                "node_id": "render.ProductReport.0",
                "path": "reports/product-report.md",
                "path_type": "relative_to_outdir",
                "inputs": [
                    {
                        "name": "left",
                        "node_id": "stage.LeftProduct",
                        "port": "product",
                        "kind": "toy_product",
                        "path": "artifacts/toy_products/LeftProduct.json",
                    },
                    {
                        "name": "right",
                        "node_id": "stage.RightProduct",
                        "port": "product",
                        "kind": "toy_product",
                        "path": "artifacts/toy_products/RightProduct.json",
                    },
                ],
            }
        ],
        outdir=tmp_path,
    )

    record_path = next((tmp_path / "artifacts" / "provenance" / "records").glob("*.json"))
    record = json.loads(record_path.read_text(encoding="utf-8"))

    assert record["producer"]["node_id"] == "render.ProductReport.0"
    assert record["inputs"] == [
        {
            "name": "left",
            "node_id": "stage.LeftProduct",
            "port": "product",
            "path": "artifacts/toy_products/LeftProduct.json",
            "kind": "toy_product",
        },
        {
            "name": "right",
            "node_id": "stage.RightProduct",
            "port": "product",
            "path": "artifacts/toy_products/RightProduct.json",
            "kind": "toy_product",
        },
    ]


def test_store_can_merge_worker_records() -> None:
    left = ProvenanceStore()
    right = ProvenanceStore()
    right.register_resource(
        ResolvedResource(id="resource", kind="test", value=object(), path="worker")
    )
    right.register_operation_record(
        execution=_execution("stage.Build::events__data__0"),
        operation=OperationRecord.from_obj({"inputs": {"resources": ["resource"]}}),
    )

    left.merge(right)

    left.validate()
    assert left.serialise_resources()["resource"]["selected"]["path"] == "worker"


def _execution(execution_id: str) -> ExecutionRecord:
    return ExecutionRecord(
        id=execution_id,
        node_id="stage.Build",
        impl="toy.record",
        role="transform",
        dataset="data",
        partition="events__data__0",
    )
