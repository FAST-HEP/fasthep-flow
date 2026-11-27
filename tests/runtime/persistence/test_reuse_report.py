"""A skipped reuse names its reason (flow#160)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from tests.runtime.persistence.test_review_fixes import (
    _plan,
    _publish_two,
    _spec,
)
from tests.toy_components.persistence import (
    persistence_workflow,
    registry_with,
    toy_capability,
)

from hepflow.api import compile_workflow_file
from hepflow.model.plan import ExecutionNode
from hepflow.model.products import BoundaryOutputSpec
from hepflow.runtime import engine
from hepflow.runtime.persistence.evidence import EvidenceLedger
from hepflow.runtime.persistence.reuse import ReuseReport, reuse_partition_products
from hepflow.runtime.persistence.save import persistence_root, product_folder
from hepflow.runtime.provenance.recorder import ProvenanceRecorder


def _reuse(
    plan: Any,
    partition: Any,
    selection: Any,
    settings: Any,
    tmp: Path,
    *,
    boundary: list[Any] | None = None,
) -> tuple[Any, ReuseReport]:
    report = ReuseReport()
    result = reuse_partition_products(
        plan,
        partition,
        boundary=[_spec("A"), _spec("B")] if boundary is None else boundary,
        selection=selection,
        settings=settings,
        registry=registry_with(toy_capability()),
        outdir=tmp,
        ledger=EvidenceLedger(),
        store=ProvenanceRecorder().store,
        report=report,
    )
    return result, report


def _skipped(report: ReuseReport) -> list[tuple[str, str]]:
    return [(item["partition"], item["reason"]) for item in report.to_dict()["skipped"]]


def test_a_reused_partition_is_counted_as_reused(tmp_path: Path) -> None:
    plan = _plan("A", "B")
    partition, selection, settings = _publish_two(tmp_path, plan)

    result, report = _reuse(plan, partition, selection, settings, tmp_path)

    assert result is not None
    assert report.to_dict() == {"attempted": 1, "reused": 1, "skipped": []}


def test_reuse_off(tmp_path: Path) -> None:
    plan = _plan("A", "B")
    partition, selection, settings = _publish_two(tmp_path, plan)
    settings = type(settings)(mode="all", reuse=False)

    _, report = _reuse(plan, partition, selection, settings, tmp_path)

    assert _skipped(report) == [(partition.id, "reuse_off")]


def test_no_boundary_products(tmp_path: Path) -> None:
    plan = _plan("A", "B")
    partition, selection, settings = _publish_two(tmp_path, plan)

    _, report = _reuse(plan, partition, selection, settings, tmp_path, boundary=[])

    assert _skipped(report) == [(partition.id, "no_boundary_products")]


def test_execution_hooks_are_named(tmp_path: Path) -> None:
    plan = _plan("A", "B")
    partition, selection, settings = _publish_two(tmp_path, plan)
    plan.execution_hooks = [
        {"kind": "hep.dataset_context", "events": ["partition_start"]},
        {"kind": "hep.error_report", "events": ["on_node_error"]},
    ]

    _, report = _reuse(plan, partition, selection, settings, tmp_path)

    [item] = report.to_dict()["skipped"]
    assert item["reason"] == "execution_hooks"
    assert item["detail"] == "hep.dataset_context, hep.error_report"


def test_sinks_and_observers_are_named(tmp_path: Path) -> None:
    plan = _plan("A", "B")
    partition, selection, settings = _publish_two(tmp_path, plan)
    plan.add_node(
        ExecutionNode(
            id="stage.Write",
            graph_node_id="stage.Write",
            role="sink",
            impl="hep.write",
        )
    )

    _, report = _reuse(plan, partition, selection, settings, tmp_path)

    [item] = report.to_dict()["skipped"]
    assert item["reason"] == "sink_or_observer"
    assert "stage.Write" in item["detail"]


def test_products_not_all_persisted(tmp_path: Path) -> None:
    plan = _plan("A", "B")
    partition, selection, settings = _publish_two(tmp_path, plan)

    _, report = _reuse(
        plan, partition, selection, settings, tmp_path, boundary=[_spec("A")]
    )

    assert _skipped(report) == [(partition.id, "products_not_all_persisted")]


def test_not_published(tmp_path: Path) -> None:
    plan = _plan("A", "B")
    partition, selection, settings = _publish_two(tmp_path, plan)
    root = persistence_root(settings, tmp_path)
    (
        product_folder(root, "stage.B", "hist", "toy", partition.id) / "manifest.json"
    ).unlink()

    _, report = _reuse(plan, partition, selection, settings, tmp_path)

    assert _skipped(report) == [(partition.id, "not_published")]


def test_plan_hash_differs(tmp_path: Path) -> None:
    plan = _plan("A", "B")
    partition, selection, settings = _publish_two(tmp_path, plan)
    plan.nodes[0].params["changed"] = True

    _, report = _reuse(plan, partition, selection, settings, tmp_path)

    assert _skipped(report) == [(partition.id, "plan_hash_differs")]


def test_identity_differs(tmp_path: Path) -> None:
    plan = _plan("A", "B")
    partition, selection, settings = _publish_two(tmp_path, plan)
    other_kind = BoundaryOutputSpec(
        node_id="stage.B", output_name="hist", kind="cutflow"
    )

    _, report = _reuse(
        plan,
        partition,
        selection,
        settings,
        tmp_path,
        boundary=[_spec("A"), other_kind],
    )

    [item] = report.to_dict()["skipped"]
    assert item["reason"] == "identity_differs"
    assert item["detail"] == "stage.B.hist"


def test_validation_failed_carries_the_error(tmp_path: Path) -> None:
    plan = _plan("A", "B")
    partition, selection, settings = _publish_two(tmp_path, plan)
    root = persistence_root(settings, tmp_path)
    (
        product_folder(root, "stage.A", "hist", "toy", partition.id) / "manifest.json"
    ).write_text("{not json", encoding="utf-8")

    _, report = _reuse(plan, partition, selection, settings, tmp_path)

    [item] = report.to_dict()["skipped"]
    assert item["reason"] == "validation_failed"
    assert item["detail"]


def test_a_caller_without_a_report_still_reuses(tmp_path: Path) -> None:
    plan = _plan("A", "B")
    partition, selection, settings = _publish_two(tmp_path, plan)

    result = reuse_partition_products(
        plan,
        partition,
        boundary=[_spec("A"), _spec("B")],
        selection=selection,
        settings=settings,
        registry=registry_with(toy_capability()),
        outdir=tmp_path,
        ledger=EvidenceLedger(),
        store=ProvenanceRecorder().store,
    )

    assert result is not None


def _compile_run(
    workflow: Path, build: Path, *, change_plan: bool = False
) -> dict[str, Any]:
    plan = compile_workflow_file(workflow, outdir=build, chunk_size=2)
    if change_plan:
        plan.nodes[0].params["changed"] = True
    ctx: dict[str, Any] = {"outdir": str(build)}
    engine.execute_plan_locally(
        plan, registry_cfg=plan.registry, ctx=ctx, partitions=plan.partitions
    )
    return ctx


def _toy_workflow_file(toy_workflow: dict[str, Any], tmp_path: Path) -> Path:
    path = tmp_path / "workflow.yaml"
    document = persistence_workflow(toy_workflow, {"mode": "all", "reuse": True})
    path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
    return path


def test_the_run_summary_reports_reuse(
    toy_workflow: dict[str, Any], tmp_path: Path
) -> None:
    workflow = _toy_workflow_file(toy_workflow, tmp_path)
    build = tmp_path / "build"

    first = _compile_run(workflow, build)
    second = _compile_run(workflow, build)

    assert first["_persistence"]["reuse"]["reused"] == 0
    assert {s["reason"] for s in first["_persistence"]["reuse"]["skipped"]} == {
        "not_published"
    }
    reuse = second["_persistence"]["reuse"]
    assert (reuse["attempted"], reuse["reused"], reuse["skipped"]) == (2, 2, [])


def test_a_changed_plan_names_its_reason_in_the_summary(
    toy_workflow: dict[str, Any], tmp_path: Path
) -> None:
    workflow = _toy_workflow_file(toy_workflow, tmp_path)
    build = tmp_path / "build"
    _compile_run(workflow, build)

    changed = _compile_run(workflow, build, change_plan=True)

    assert {s["reason"] for s in changed["_persistence"]["reuse"]["skipped"]} == {
        "plan_hash_differs"
    }


def test_no_reuse_section_when_reuse_is_off(
    toy_workflow: dict[str, Any], tmp_path: Path
) -> None:
    path = tmp_path / "workflow.yaml"
    document = persistence_workflow(toy_workflow, {"mode": "all"})
    path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")

    ctx = _compile_run(path, tmp_path / "build")

    assert "reuse" not in ctx["_persistence"]
