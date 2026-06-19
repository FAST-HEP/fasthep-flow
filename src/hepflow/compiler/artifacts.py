from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import networkx as nx

from hepflow.backends._dask._worker_env import (
    packed_pixi_environment_spec_from_execution,
)
from hepflow.build_layout import BuildPaths
from hepflow.compiler.compile_hooks import run_compile_hooks
from hepflow.compiler.graph_artifacts import _lowered_graph_to_json
from hepflow.model.plan import ExecutionPlan
from hepflow.utils import write_json, write_yaml


def write_compile_artifacts(
    *,
    plan: ExecutionPlan,
    graph: nx.DiGraph,
    outdir: str | Path,
    normalized: dict[str, Any] | None = None,
) -> None:
    out_path = Path(outdir)
    build_paths = BuildPaths(root=out_path)
    compile_path = out_path / "compile"
    write_yaml(_lowered_graph_to_json(graph), str(compile_path / "analysis.ir.yaml"))
    write_yaml(plan.data_flow, str(compile_path / "deps.yaml"))
    artifacts: dict[str, Any] = {}
    dataset_entries = _dataset_entries_artifact(plan.context.get("datasets") or {})
    artifacts["dataset_entries"] = dataset_entries
    (compile_path / "dataset_entries.json").write_text(
        json.dumps(
            dataset_entries,
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    write_compile_hook_artifacts(
        plan=plan,
        normalized=normalized,
        build_paths=build_paths,
        artifacts=artifacts,
        when="after_datasets",
    )
    write_yaml(
        {
            "nodes": len(plan.nodes),
            "partitions": len(plan.partitions),
            "datasets": sorted((plan.context.get("datasets") or {}).keys()),
            "registry_sections": sorted(plan.registry.keys()),
        },
        str(compile_path / "report.compile.yaml"),
    )
    write_worker_environment_artifact(plan=plan, compile_path=compile_path)
    write_render_artifacts(plan=plan, outdir=out_path)


def write_compile_hook_artifacts(
    *,
    plan: ExecutionPlan,
    normalized: dict[str, Any] | None,
    build_paths: BuildPaths,
    artifacts: dict[str, Any],
    when: str,
) -> None:
    hook_artifacts = run_compile_hooks(
        plan=plan,
        normalized=normalized,
        build_paths=build_paths,
        artifacts=artifacts,
        when=when,
    )
    compile_path = build_paths.compile_dir()
    for name, payload in hook_artifacts.items():
        (compile_path / f"{name}.json").write_text(
            json.dumps(payload, indent=2, sort_keys=True),
            encoding="utf-8",
        )


def _dataset_entries_artifact(datasets: dict[str, Any]) -> dict[str, Any]:
    """Return public dataset-entry metadata without placeholder null values."""
    return {
        str(name): _drop_none_values(dict(dataset or {}))
        for name, dataset in dict(datasets or {}).items()
    }


def _drop_none_values(value: dict[str, Any]) -> dict[str, Any]:
    return {key: item for key, item in value.items() if item is not None}


def write_worker_environment_artifact(
    *,
    plan: ExecutionPlan,
    compile_path: Path,
) -> None:
    spec = packed_pixi_environment_spec_from_execution(plan.execution)
    if spec is None:
        return
    write_json(spec.to_dict(), compile_path / "worker_environment.json")


def write_render_artifacts(
    *,
    plan: ExecutionPlan,
    outdir: str | Path,
    variation: str | None = None,
) -> None:
    paths = BuildPaths(root=Path(outdir), variation=variation)
    specs_dir = paths.render_specs_dir()
    specs_dir.mkdir(parents=True, exist_ok=True)
    render_specs: list[dict[str, Any]] = []
    for node in plan.nodes:
        if node.role != "sink":
            continue
        spec = dict(node.params.get("spec") or {})
        if not spec:
            continue
        item = {
            "node_id": node.id,
            "impl": node.impl,
            "out": node.params.get("out"),
            "spec": spec,
        }
        render_specs.append(item)
        safe_node_id = node.id.replace(".", "_").replace("/", "_")
        write_yaml(item, str(paths.render_spec(f"{safe_node_id}.yaml")))

    report_path = paths.render_dir() / "report.render.json"
    if variation:
        report_path = paths.render_spec("report.render.json")
    report_path.write_text(
        json.dumps({"renders": render_specs}, indent=2, sort_keys=True),
        encoding="utf-8",
    )
