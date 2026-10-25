from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import networkx as nx
from fasthep_toolbench.misc import file_fingerprint

from hepflow.backends.loaders import backend_build_directories
from hepflow.build_layout import BuildPaths, ensure_build_layout
from hepflow.compiler.compile_hooks import run_compile_hooks
from hepflow.compiler.graph_artifacts import (
    _lowered_graph_to_json,
    write_graph_artifacts,
)
from hepflow.compiler.inputs_manifest import (
    InputCollector,
    mapping_file_inputs,
    write_inputs_manifest,
)
from hepflow.model.plan import ExecutionPlan
from hepflow.model.worker_environment import worker_environment_plan_from_execution
from hepflow.utils import write_json, write_yaml


def write_plan_artifacts(
    *,
    plan: ExecutionPlan,
    graph: nx.DiGraph,
    outdir: str | Path,
    normalized: dict[str, Any],
) -> list[dict[str, Any]]:
    """Save one compiled plan and its artifacts before running render hooks.

    Returns one record per compile hook that ran, in run order (``after_datasets``
    hooks first, then ``after_compile``), with artifact paths and sha256 hashes.
    """
    paths = BuildPaths.from_plan(plan, outdir=outdir)
    ensure_build_layout(
        paths.root,
        variation=paths.variation,
        backend_directories=backend_build_directories(
            plan.registry, plan.execution, provenance=plan.provenance,
        ),
    )
    write_yaml(normalized, str(paths.compile_file("normalized.yaml")))
    records = write_compile_artifacts(
        plan=plan, graph=graph, outdir=paths.root, normalized=normalized,
    )
    graph_artifacts = write_graph_artifacts(graph, paths.graph_dir())
    plan_file = paths.compile_file("plan.yaml")
    write_yaml(plan.to_dict(), str(plan_file))
    records.extend(
        write_compile_hook_artifacts(
            plan=plan,
            normalized=normalized,
            build_paths=paths,
            artifacts={**graph_artifacts, "plan": str(plan_file)},
            when="after_compile",
        )
    )
    return records


def write_compile_artifacts(
    *,
    plan: ExecutionPlan,
    graph: nx.DiGraph,
    outdir: str | Path,
    normalized: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Write compile artifacts; return the ``after_datasets`` hook records."""
    out_path = Path(outdir)
    build_paths = BuildPaths.from_plan(plan, outdir=out_path)
    compile_path = build_paths.compile_dir()
    write_yaml(_lowered_graph_to_json(graph), str(compile_path / "analysis.ir.yaml"))
    write_yaml(plan.data_flow, str(compile_path / "deps.yaml"))
    if normalized is not None:
        provenance = dict(normalized.get("provenance") or {})
        collector = InputCollector.from_rows(provenance.get("inputs") or [])
        collector.extend(mapping_file_inputs(plan))
        write_inputs_manifest(
            compile_path,
            root=provenance.get("root") or normalized.get("project_root"),
            inputs=collector.entries(),
        )
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
    records = write_compile_hook_artifacts(
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
    write_render_artifacts(plan=plan, outdir=out_path, variation=build_paths.variation)
    return records


def write_compile_hook_artifacts(
    *,
    plan: ExecutionPlan,
    normalized: dict[str, Any] | None,
    build_paths: BuildPaths,
    artifacts: dict[str, Any],
    when: str,
) -> list[dict[str, Any]]:
    """Run one phase of compile hooks, write their artifacts, return the records.

    Each record lists its artifacts as ``{name: {"path": ..., "sha256": ...}}``
    with the hash taken from the written file.
    """
    raw_records: list[dict[str, Any]] = []
    hook_artifacts = run_compile_hooks(
        plan=plan,
        normalized=normalized,
        build_paths=build_paths,
        artifacts=artifacts,
        when=when,
        records=raw_records,
    )
    compile_path = build_paths.compile_dir()
    hashes: dict[str, dict[str, str]] = {}
    for name, payload in hook_artifacts.items():
        path = compile_path / f"{name}.json"
        path.write_text(
            json.dumps(payload, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        sha256, _size = file_fingerprint(path)
        hashes[name] = {"path": path.name, "sha256": sha256}
    return [
        {**record, "artifacts": {name: hashes[name] for name in record["artifacts"]}}
        for record in raw_records
    ]


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
    spec = worker_environment_plan_from_execution(plan.execution)
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
