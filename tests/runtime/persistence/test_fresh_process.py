from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
import yaml
from tests.toy_components.persistence import persistence_workflow

from hepflow.api import compile_workflow_file
from hepflow.runtime import engine
from hepflow.runtime.persistence.manifest import MANIFEST_NAME
from hepflow.runtime.provenance import PROVENANCE_VERSION, load_provenance_document

REPO = Path(__file__).resolve().parents[3]

SAVE = """
import json, sys
from pathlib import Path
from hepflow.api import compile_workflow_file
from hepflow.runtime import engine
from hepflow.runtime.persistence import save as save_module

workflow, build, out = map(Path, sys.argv[1:4])
seen = {}
real = save_module.persist_boundary_products
def spy(products, *a, **k):
    for p in products:
        seen[p.partition_id] = p.value
    return real(products, *a, **k)
engine.persist_boundary_products = spy
plan = compile_workflow_file(workflow, outdir=build, chunk_size=2)
engine.execute_plan_locally(
    plan, registry_cfg=plan.registry, ctx={"outdir": str(build)},
    partitions=plan.partitions,
)
out.write_text(json.dumps(seen))
"""

RESTORE = """
import json, sys
from pathlib import Path
from hepflow.api import compile_workflow_file, restore_products
from hepflow.registry.loaders import runtime_registry_from_config

workflow, build, out = map(Path, sys.argv[1:4])
plan = compile_workflow_file(workflow, outdir=build / "again", chunk_size=2)
registry = runtime_registry_from_config(plan.registry)
values = {}
for manifest in sorted((build / "artifacts" / "products").rglob("manifest.json")):
    restored = restore_products(manifest, registry=registry)
    for key, value in restored.products.items():
        values[manifest.parent.name] = value
out.write_text(json.dumps(values))
"""


def _env() -> dict[str, str]:
    path = os.pathsep.join([str(REPO / "src"), str(REPO)])
    return {**os.environ, "PYTHONPATH": path, "GIT_WORK_TREE": ""}


def _write_workflow(
    toy_workflow: dict[str, Any], tmp_path: Path, **persistence: Any
) -> Path:
    path = tmp_path / "workflow.yaml"
    config = {"mode": "all", **persistence}
    path.write_text(
        yaml.safe_dump(persistence_workflow(toy_workflow, config), sort_keys=False),
        encoding="utf-8",
    )
    return path


def test_save_in_one_process_restore_in_another(
    toy_workflow: dict[str, Any], tmp_path: Path
) -> None:
    workflow = _write_workflow(toy_workflow, tmp_path)
    build, saved, restored = (
        tmp_path / "build",
        tmp_path / "s.json",
        tmp_path / "r.json",
    )
    env = _env()
    for script, out in ((SAVE, saved), (RESTORE, restored)):
        subprocess.run(
            [sys.executable, "-c", script, str(workflow), str(build), str(out)],
            check=True,
            env=env,
            cwd=REPO,
        )

    native = json.loads(saved.read_text())
    again = json.loads(restored.read_text())
    assert len(native) == 2
    assert sorted(again.values(), key=str) == sorted(native.values(), key=str)


def _run(workflow: Path, build: Path) -> Any:
    plan = compile_workflow_file(workflow, outdir=build, chunk_size=2)
    return engine.execute_plan_locally(
        plan,
        registry_cfg=plan.registry,
        ctx={"outdir": str(build)},
        partitions=plan.partitions,
    )


def _count_partition_runs(
    monkeypatch: pytest.MonkeyPatch, fail_on: int | None = None
) -> list[str]:
    calls: list[str] = []
    real = engine.execute_plan_partition

    def counting(plan: Any, *, ctx: dict[str, Any], **kwargs: Any) -> Any:
        calls.append(ctx["partition"]["id"])
        if fail_on is not None and len(calls) == fail_on:
            raise RuntimeError("killed")
        return real(plan, ctx=ctx, **kwargs)

    monkeypatch.setattr(engine, "execute_plan_partition", counting)
    return calls


def test_restart_does_not_rerun_finished_producers(
    toy_workflow: dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workflow = _write_workflow(toy_workflow, tmp_path, reuse=True)
    build = tmp_path / "build"
    first = _count_partition_runs(monkeypatch, fail_on=2)
    with pytest.raises(RuntimeError, match="killed"):
        _run(workflow, build)
    assert len(first) == 2

    monkeypatch.undo()
    second = _count_partition_runs(monkeypatch)
    _run(workflow, build)

    assert len(second) == 1
    assert second[0] == first[1]
    assert len(list((build / "artifacts" / "products").rglob(MANIFEST_NAME))) == 2


def test_reuse_is_off_unless_asked(
    toy_workflow: dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workflow = _write_workflow(toy_workflow, tmp_path)
    build = tmp_path / "build"
    _run(workflow, build)
    calls = _count_partition_runs(monkeypatch)
    _run(workflow, build)

    assert len(calls) == 2


def test_a_changed_plan_is_not_reused(
    toy_workflow: dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workflow = _write_workflow(toy_workflow, tmp_path, reuse=True)
    build = tmp_path / "build"
    _run(workflow, build)
    document = yaml.safe_load(workflow.read_text())
    document["analysis"]["stages"][0]["params"]["name"] = "changed"
    workflow.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
    calls = _count_partition_runs(monkeypatch)

    _run(workflow, build)

    assert len(calls) == 2


def test_a_corrupted_set_is_not_reused(
    toy_workflow: dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workflow = _write_workflow(toy_workflow, tmp_path, reuse=True)
    build = tmp_path / "build"
    _run(workflow, build)
    payload = next((build / "artifacts" / "products").rglob("counts.json"))
    payload.write_text('{"entries": 999}', encoding="utf-8")
    calls = _count_partition_runs(monkeypatch)

    _run(workflow, build)

    assert len(calls) == 1


def test_provenance_documents_are_version_3(tmp_path: Path) -> None:
    assert PROVENANCE_VERSION == "3.0"
    old = tmp_path / "manifest.json"
    old.write_text(json.dumps({"version": "2.0", "records": []}), encoding="utf-8")
    with pytest.raises(ValueError, match="version"):
        load_provenance_document(old)


def test_an_operation_that_changes_the_plan_fails_the_run(
    toy_workflow: dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A plan is not changed after compile (carpenter#34: hep.hist filled its axis)."""
    workflow = _write_workflow(toy_workflow, tmp_path, reuse=True)
    real = engine.execute_plan_partition

    def mutating(plan: Any, *, ctx: dict[str, Any], **kwargs: Any) -> Any:
        result = real(plan, ctx=ctx, **kwargs)
        plan.nodes[0].params["completed_at_runtime"] = ctx["partition"]["id"]
        return result

    monkeypatch.setattr(engine, "execute_plan_partition", mutating)

    with pytest.raises(engine.PlanChangedDuringRun, match="changed while it ran"):
        _run(workflow, tmp_path / "build")
