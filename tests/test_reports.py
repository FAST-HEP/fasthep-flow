from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from hepflow.build_layout import BuildPaths
from hepflow.compiler.normalize import normalize_author
from hepflow.model.plan import ExecutionPlan
from hepflow.runtime.reports import provenance_report_context, run_workflow_reports


def test_reports_normalize_to_author_model() -> None:
    normalized = normalize_author(
        {
            "data": {
                "datasets": [
                    {"name": "data", "files": ["data.root"], "eventtype": "data"}
                ]
            },
            "reports": [
                {
                    "id": "provenance",
                    "op": "hep.report.template",
                    "source": "provenance",
                    "template": "hep.provenance.default",
                    "outputs": [
                        {"path": "reports/provenance.md", "format": "markdown"},
                        {"path": "reports/provenance.html", "format": "html"},
                    ],
                }
            ],
        }
    )

    assert normalized["reports"] == [
        {
            "id": "provenance",
            "op": "hep.report.template",
            "source": "provenance",
            "template": "hep.provenance.default",
            "outputs": [
                {"path": "reports/provenance.md", "format": "markdown"},
                {"path": "reports/provenance.html", "format": "html"},
            ],
        }
    ]


def test_provenance_report_context_is_structured(tmp_path: Path) -> None:
    paths = BuildPaths(root=tmp_path)
    _write_provenance_fixture(paths)
    plan = ExecutionPlan(context={"datasets": {"dy": {"name": "dy"}}})

    context = provenance_report_context(
        plan=plan,
        paths=paths,
        summary={
            "summary_path": "run_summary.yaml",
            "partitions": [
                {
                    "partition": {"id": "events__dy__0", "dataset": "dy"},
                    "outputs": [
                        {
                            "node": "stage.PileupWeights",
                            "port": "stream",
                            "type": "Array",
                        }
                    ],
                }
            ],
        },
    )

    assert context["run"]["id"] == "run-1"
    assert context["resources"][0]["id"] == "cms.pileup.2024"
    assert context["executed_stages"] == [
        {
            "node_id": "stage.PileupWeights",
            "datasets": ["dy"],
            "partitions": ["events__dy__0"],
            "outputs": [{"port": "stream", "type": "Array"}],
        }
    ]
    assert context["operations"][0]["inputs"]["symbols"] == ["Pileup_nTrueInt"]
    assert context["artifacts"][0]["path"] == "artifacts/files/out.root"
    assert context["warnings"][0]["code"] == "RESOURCE_FALLBACK"


def test_run_workflow_reports_dispatches_registered_operation(tmp_path: Path) -> None:
    paths = BuildPaths(root=tmp_path)
    _write_provenance_fixture(paths)
    plan = ExecutionPlan(
        context={
            "datasets": {"dy": {"name": "dy"}},
            "author_path": str(tmp_path / "author.yaml"),
        },
        registry={
            "sinks": {
                "toy.report": {
                    "spec": "tests.toy_components.sinks:TOY_REPORT_SPEC",
                    "impl": "tests.toy_components.sinks:run_toy_report",
                }
            }
        },
        reports=[
            {
                "id": "provenance",
                "op": "toy.report",
                "source": "provenance",
                "template": "unused.md.j2",
                "outputs": [{"path": "reports/provenance.json", "format": "json"}],
            }
        ],
    )

    outputs = run_workflow_reports(
        plan,
        outdir=tmp_path,
        summary={"summary_path": "run_summary.yaml"},
    )

    report_path = tmp_path / "reports" / "provenance.json"
    assert outputs == [
        {
            "report_id": "provenance",
            "path": str(report_path),
            "format": "json",
        }
    ]
    assert json.loads(report_path.read_text(encoding="utf-8")) == {
        "artifacts": ["artifacts/files/out.root"],
        "run_id": "run-1",
        "source": "provenance",
    }


def test_unknown_report_source_fails_clearly(tmp_path: Path) -> None:
    plan = ExecutionPlan(
        registry={"sinks": {}},
        reports=[
            {
                "id": "performance",
                "op": "toy.report",
                "source": "performance",
                "outputs": [{"path": "reports/perf.md", "format": "markdown"}],
            }
        ],
    )

    with pytest.raises(ValueError, match="Unknown report source"):
        run_workflow_reports(plan, outdir=tmp_path, summary={})


def _write_provenance_fixture(paths: BuildPaths) -> None:
    paths.provenance_dir().mkdir(parents=True, exist_ok=True)
    _write_json(
        paths.provenance_manifest(),
        {
            "version": "1.0",
            "run_id": "run-1",
            "execution": "artifacts/provenance/execution.json",
            "records": [
                {
                    "artifact": "artifacts/files/out.root",
                    "kind": "root_tree",
                    "node_id": "write.FilterChannel.0",
                    "record": "artifacts/provenance/records/artifact.json",
                }
            ],
        },
    )
    _write_json(
        paths.provenance_execution(),
        {
            "version": "1.0",
            "run_id": "run-1",
            "workflow": {
                "graph": "graph/graph.json",
                "plan": "compile/plan.yaml",
            },
            "software": {"fasthep-flow": "0.1"},
            "execution": {"host": "worker"},
            "partitions": [
                {
                    "id": "events__dy__0",
                    "dataset": "dy",
                    "file": "dy.root",
                    "source": "events",
                    "part": "0_0",
                    "start": None,
                    "stop": None,
                }
            ],
            "resources": {
                "cms.pileup.2024": {
                    "kind": "correctionlib",
                    "requested_era": "RunIII2024Summer24",
                    "selected": {
                        "era": "2023_Summer23",
                        "path": "/cvmfs/pu.json.gz",
                        "correction": "Collisions2023",
                        "fallback": True,
                        "reason": "No 2024 payload is available.",
                    },
                }
            },
            "executions": {
                "stage.PileupWeights::events__dy__0": {
                    "node_id": "stage.PileupWeights",
                    "impl": "chip.pileup_weights",
                    "role": "transform",
                    "dataset": "dy",
                    "partition": "events__dy__0",
                    "operations": [
                        {
                            "inputs": {
                                "symbols": ["Pileup_nTrueInt"],
                                "resources": ["cms.pileup.2024"],
                            },
                            "outputs": {"symbols": ["weight_pu_nominal"]},
                        }
                    ],
                }
            },
        },
    )


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
