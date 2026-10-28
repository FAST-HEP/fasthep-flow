from __future__ import annotations

import json
from pathlib import Path
from re import escape
from typing import Any

import networkx as nx
import pytest
import yaml

from hepflow.api import compile_workflow_file
from hepflow.build_layout import BuildPaths, ensure_build_layout
from hepflow.compiler.artifacts import write_compile_artifacts
from hepflow.compiler.compile_hooks import run_compile_hooks
from hepflow.model.plan import ExecutionPlan
from hepflow.registry.merge import RegistryLayer, merge_registry_layers


def test_compile_hook_registry_entry_is_resolved_and_filtered_by_phase(
    tmp_path: Path,
) -> None:
    plan = _plan_with_compile_hooks()

    artifacts = run_compile_hooks(
        plan=plan,
        normalized={"sources": {}},
        build_paths=BuildPaths(root=tmp_path),
        artifacts={"dataset_entries": {"data": {"files": ["data.root"]}}},
        when="after_datasets",
    )

    assert artifacts == {
        "dataset_metadata": {
            "hook": "after_datasets",
            "datasets": ["data"],
            "has_dataset_entries": True,
        }
    }


def test_compile_hook_with_enabled_false_is_skipped(tmp_path: Path) -> None:
    plan = _plan_with_compile_hooks()
    plan.registry["compile_hooks"]["toy.dataset_metadata"]["enabled"] = False

    artifacts = run_compile_hooks(
        plan=plan,
        normalized={"sources": {}},
        build_paths=BuildPaths(root=tmp_path),
        artifacts={"dataset_entries": {"data": {"files": ["data.root"]}}},
        when="after_datasets",
    )

    assert artifacts == {}


def test_compile_hook_entry_with_only_enabled_false_is_skipped(tmp_path: Path) -> None:
    # A workflow can switch a profile's hook off without repeating its spec and impl.
    plan = ExecutionPlan(
        registry={"compile_hooks": {"toy.dataset_metadata": {"enabled": False}}},
    )
    records: list[dict[str, Any]] = []

    artifacts = run_compile_hooks(
        plan=plan,
        normalized={"sources": {}},
        build_paths=BuildPaths(root=tmp_path),
        when="after_datasets",
        records=records,
    )

    assert artifacts == {}
    assert records == []


def test_compile_hook_enabled_true_still_runs(tmp_path: Path) -> None:
    plan = _plan_with_compile_hooks()
    plan.registry["compile_hooks"]["toy.dataset_metadata"]["enabled"] = True

    artifacts = run_compile_hooks(
        plan=plan,
        normalized={"sources": {}},
        build_paths=BuildPaths(root=tmp_path),
        artifacts={"dataset_entries": {"data": {"files": ["data.root"]}}},
        when="after_datasets",
    )

    assert "dataset_metadata" in artifacts


def test_compile_hook_enabled_must_be_a_boolean(tmp_path: Path) -> None:
    plan = _plan_with_compile_hooks()
    plan.registry["compile_hooks"]["toy.dataset_metadata"]["enabled"] = "no"

    with pytest.raises(TypeError, match="enabled"):
        run_compile_hooks(
            plan=plan,
            normalized={"sources": {}},
            build_paths=BuildPaths(root=tmp_path),
            when="after_datasets",
        )


def test_workflow_layer_can_switch_off_a_profile_compile_hook(tmp_path: Path) -> None:
    profile_registry = _plan_with_compile_hooks().registry
    merged = merge_registry_layers(
        [
            RegistryLayer(name="profile", kind="profile", registry=profile_registry),
            RegistryLayer(
                name="workflow",
                kind="workflow",
                registry={"compile_hooks": {"toy.dataset_metadata": {"enabled": False}}},
            ),
        ]
    )
    plan = ExecutionPlan(registry=merged.registry)

    artifacts = run_compile_hooks(
        plan=plan,
        normalized={"sources": {}},
        build_paths=BuildPaths(root=tmp_path),
        when="after_datasets",
    )

    assert artifacts == {}
    assert "toy.ignored" in merged.registry["compile_hooks"]


def test_compile_hook_artifacts_are_written_as_json(tmp_path: Path) -> None:
    plan = _plan_with_compile_hooks()

    ensure_build_layout(tmp_path)
    write_compile_artifacts(
        plan=plan,
        graph=nx.DiGraph(),
        outdir=tmp_path,
        normalized={"sources": {}},
    )

    metadata = json.loads(
        (tmp_path / "compile" / "dataset_metadata.json").read_text(encoding="utf-8")
    )
    assert metadata["hook"] == "after_datasets"
    assert metadata["datasets"] == ["data"]
    assert not (tmp_path / "compile" / "ignored.json").exists()


def test_after_compile_hook_runs_after_graph_artifacts_exist(
    tmp_path: Path,
    toy_workflow: dict[str, Any],
) -> None:
    workflow = dict(toy_workflow)
    registry = dict(workflow["registry"])
    registry["compile_hooks"] = {
        "toy.graph_render": {
            "spec": "tests.toy_components.compile_hooks:GRAPH_RENDER_HOOK_SPEC",
            "impl": "tests.toy_components.compile_hooks:graph_render_hook",
        }
    }
    workflow["registry"] = registry
    workflow_path = tmp_path / "workflow.yaml"
    workflow_path.write_text(yaml.safe_dump(workflow, sort_keys=False), encoding="utf-8")
    build_dir = tmp_path / "build"

    compile_workflow_file(workflow_path, outdir=build_dir)

    graph_render = json.loads(
        (build_dir / "compile" / "graph_render.json").read_text(encoding="utf-8")
    )
    assert graph_render == {
        "graph_d2_exists": True,
        "has_graph_d2_artifact": True,
    }


def test_compile_hook_errors_include_name_phase_and_impl(tmp_path: Path) -> None:
    plan = ExecutionPlan(
        registry={
            "compile_hooks": {
                "toy.fail": {
                    "spec": "tests.toy_components.compile_hooks:FAILING_HOOK_SPEC",
                    "impl": "tests.toy_components.compile_hooks:failing_compile_hook",
                }
            }
        }
    )

    with pytest.raises(
        RuntimeError,
        match=escape(
            "Compile hook 'toy.fail' failed during 'after_datasets' "
            "using 'tests.toy_components.compile_hooks:failing_compile_hook': "
            "compile boom"
        ),
    ):
        run_compile_hooks(
            plan=plan,
            normalized={},
            build_paths=BuildPaths(root=tmp_path),
            when="after_datasets",
        )


def test_dataset_entries_artifact_remains_declaration_only(tmp_path: Path) -> None:
    plan = _plan_with_compile_hooks()

    ensure_build_layout(tmp_path)
    write_compile_artifacts(
        plan=plan,
        graph=nx.DiGraph(),
        outdir=tmp_path,
        normalized={"sources": {}},
    )

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


def test_flow_compile_hook_logic_does_not_import_uproot() -> None:
    root = Path(__file__).resolve().parents[2]
    checked = [
        root / "src" / "hepflow" / "compiler" / "compile_hooks.py",
        root / "src" / "hepflow" / "compiler" / "artifacts.py",
    ]
    offenders = [
        str(path.relative_to(root))
        for path in checked
        if "uproot" in path.read_text(encoding="utf-8")
    ]
    assert offenders == []


def _plan_with_compile_hooks() -> ExecutionPlan:
    return ExecutionPlan(
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
        },
        registry={
            "compile_hooks": {
                "toy.dataset_metadata": {
                    "spec": "tests.toy_components.compile_hooks:DATASET_METADATA_HOOK_SPEC",
                    "impl": "tests.toy_components.compile_hooks:dataset_metadata_hook",
                },
                "toy.ignored": {
                    "spec": "tests.toy_components.compile_hooks:IGNORED_HOOK_SPEC",
                    "impl": "tests.toy_components.compile_hooks:ignored_hook",
                },
            }
        },
    )
