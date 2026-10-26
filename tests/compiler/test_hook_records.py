from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import networkx as nx

from hepflow.build_layout import BuildPaths, ensure_build_layout
from hepflow.compiler.artifacts import write_compile_artifacts, write_plan_artifacts
from hepflow.compiler.compile_hooks import run_compile_hooks
from hepflow.model.plan import ExecutionPlan
from hepflow.registry.defaults import (
    default_runtime_registry_config,
    merge_registry_config,
)

_TOY = "tests.toy_components.compile_hooks"


def _plan() -> ExecutionPlan:
    return ExecutionPlan(
        context={"datasets": {"data": {"name": "data", "files": ["data.root"]}}},
        registry={
            "compile_hooks": {
                "toy.dataset_metadata": {
                    "spec": f"{_TOY}:DATASET_METADATA_HOOK_SPEC",
                    "impl": f"{_TOY}:dataset_metadata_hook",
                },
                "toy.silent": {
                    "spec": f"{_TOY}:SILENT_HOOK_SPEC",
                    "impl": f"{_TOY}:silent_hook",
                    "params": {"x": 1},
                },
                "toy.graph_render": {
                    "spec": f"{_TOY}:GRAPH_RENDER_HOOK_SPEC",
                    "impl": f"{_TOY}:graph_render_hook",
                },
            }
        },
    )


def test_run_compile_hooks_appends_records(tmp_path: Path) -> None:
    records: list[dict[str, Any]] = []
    run_compile_hooks(
        plan=_plan(),
        normalized={},
        build_paths=BuildPaths(root=tmp_path),
        when="after_datasets",
        records=records,
    )
    assert records == [
        {
            "name": "toy.dataset_metadata",
            "phase": "after_datasets",
            "params": {},
            "artifacts": ["dataset_metadata"],
        },
        {
            "name": "toy.silent",
            "phase": "after_datasets",
            "params": {"x": 1},
            "artifacts": [],
        },
    ]


def test_write_compile_artifacts_returns_hashed_records(tmp_path: Path) -> None:
    ensure_build_layout(tmp_path)
    records = write_compile_artifacts(
        plan=_plan(), graph=nx.DiGraph(), outdir=tmp_path, normalized={"sources": {}}
    )
    assert [r["name"] for r in records] == ["toy.dataset_metadata", "toy.silent"]
    meta, silent = records
    assert meta["phase"] == "after_datasets"
    entry = meta["artifacts"]["dataset_metadata"]
    assert entry["path"] == "dataset_metadata.json"
    assert len(entry["sha256"]) == 64
    data = (tmp_path / "compile" / "dataset_metadata.json").read_bytes()
    assert entry["sha256"] == hashlib.sha256(data).hexdigest()
    assert silent["artifacts"] == {}


def test_plan_artifacts_records_keep_run_order_across_phases(tmp_path: Path) -> None:
    plan = _plan()
    plan.registry = merge_registry_config(
        default_runtime_registry_config(), plan.registry
    )
    records = write_plan_artifacts(
        plan=plan, graph=nx.DiGraph(), outdir=tmp_path, normalized={"sources": {}}
    )
    toy = [r for r in records if r["name"].startswith("toy.")]
    assert [(r["name"], r["phase"]) for r in toy] == [
        ("toy.dataset_metadata", "after_datasets"),
        ("toy.silent", "after_datasets"),
        ("toy.graph_render", "after_compile"),
    ]
    phases = [r["phase"] for r in records]
    assert phases == sorted(phases, key=["after_datasets", "after_compile"].index)
    assert len(toy[2]["artifacts"]["graph_render"]["sha256"]) == 64
