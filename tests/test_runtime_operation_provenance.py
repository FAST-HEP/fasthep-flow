from __future__ import annotations

import json
from pathlib import Path

from hepflow.model.plan import ExecutionPartition, ExecutionPlan
from hepflow.runtime.operation_provenance import (
    ResolvedRuntimeResource,
    RuntimeProvenanceRecorder,
)
from hepflow.runtime.provenance import write_artifact_provenance_records


def test_operation_provenance_records_symbols_only() -> None:
    recorder = RuntimeProvenanceRecorder()

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
    recorder = RuntimeProvenanceRecorder()
    runtime_value = object()

    recorder.record_resource(
        ResolvedRuntimeResource(
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
    recorder = RuntimeProvenanceRecorder()
    recorder.record_resource(
        ResolvedRuntimeResource(
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
