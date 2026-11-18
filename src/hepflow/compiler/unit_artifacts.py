"""Compile output of the per-dataset mode: shared files plus one unit per dataset."""

from __future__ import annotations

import json
import tempfile
from collections.abc import Mapping
from dataclasses import replace
from pathlib import Path
from typing import Any

import networkx as nx
import yaml

from hepflow.backends.loaders import backend_build_directories
from hepflow.build_layout import BuildPaths, ensure_build_layout
from hepflow.compiler.artifacts import (
    _dataset_entries_artifact,
    write_bundle_manifest,
    write_compile_hook_artifacts,
    write_render_artifacts,
)
from hepflow.compiler.bundle import write_invocation
from hepflow.compiler.dataset_units import (
    MANIFEST_NAME,
    SHARED_DATASET_KEYS,
    PublishConflict,
    dataset_manifest,
    manifest_bytes,
    publish_if_absent,
    read_manifest,
    shared_plan_dict,
    unit_dir,
    write_manifest,
)
from hepflow.compiler.graph_artifacts import (
    _lowered_graph_to_json,
    write_graph_artifacts,
)
from hepflow.compiler.partitioning import resolve_chunk_size
from hepflow.compiler.per_dataset import stub_unselected_datasets
from hepflow.model.plan import ExecutionPlan
from hepflow.model.worker_environment import worker_environment_plan_from_execution
from hepflow.utils import write_json, write_yaml


def _yaml_bytes(doc: dict[str, Any]) -> bytes:
    return yaml.safe_dump(doc, sort_keys=False).encode("utf-8")


def _metadata_only_graph(graph: nx.DiGraph) -> nx.DiGraph:
    """Copy of the lowered graph whose dataset records keep their metadata only."""
    result = graph.copy()
    for node_id, attrs in graph.nodes(data=True):
        payload = attrs["payload"]
        datasets = payload.params.get("datasets")
        if not datasets:
            continue
        stubs = [
            {key: record[key] for key in SHARED_DATASET_KEYS if key in record}
            for record in datasets
        ]
        result.nodes[node_id]["payload"] = replace(
            payload, params={**payload.params, "datasets": stubs}
        )
    return result


def _publish_tree(source: Path, target: Path) -> None:
    """Publish every file below ``source`` to the same relative path under ``target``."""
    for path in sorted(source.rglob("*")):
        if path.is_file():
            publish_if_absent(target / path.relative_to(source), path.read_bytes())


def write_dataset_unit_artifacts(
    *,
    plan: ExecutionPlan,
    graph: nx.DiGraph,
    outdir: str | Path,
    normalized: dict[str, Any],
    datasets: list[str],
    chunk_size: int | None = None,
    snapshot: bool = False,
    invocation: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Publish the shared files of a plan and one unit for each of ``datasets``.

    Shared files (plan, dependencies, graph, worker environment, render specs) are
    created if absent and must be identical in every compile of the root. Each unit is
    complete once its ``manifest.yaml`` exists, which is written last; a unit with the same
    manifest is left untouched and one with another manifest fails the compile.
    """
    paths = BuildPaths.from_plan(plan, outdir=outdir)
    compile_path = paths.compile_dir()
    ensure_build_layout(
        paths.root,
        variation=paths.variation,
        backend_directories=backend_build_directories(
            plan.registry, plan.execution, provenance=plan.provenance
        ),
    )

    publish_if_absent(compile_path / "plan.yaml", _yaml_bytes(shared_plan_dict(plan)))
    publish_if_absent(compile_path / "deps.yaml", _yaml_bytes(plan.data_flow))
    spec = worker_environment_plan_from_execution(plan.execution)
    if spec is not None:
        with tempfile.TemporaryDirectory() as scratch:
            write_json(spec.to_dict(), Path(scratch) / "worker_environment.json")
            _publish_tree(Path(scratch), compile_path)
    with tempfile.TemporaryDirectory() as scratch:
        write_graph_artifacts(
            _metadata_only_graph(graph), Path(scratch) / "graph"
        )
        _publish_tree(Path(scratch) / "graph", paths.graph_dir())
    with tempfile.TemporaryDirectory() as scratch:
        write_render_artifacts(plan=plan, outdir=scratch, variation=paths.variation)
        _publish_tree(Path(scratch) / "render", paths.root / "render")

    records: list[dict[str, Any]] = []
    for name in datasets:
        records.extend(
            _write_unit(
                plan=plan,
                graph=graph,
                paths=paths,
                normalized=normalized,
                dataset=name,
                chunk_size=chunk_size,
                snapshot=snapshot,
                invocation=invocation,
            )
        )
    return records


def _write_unit(
    *,
    plan: ExecutionPlan,
    graph: nx.DiGraph,
    paths: BuildPaths,
    normalized: dict[str, Any],
    dataset: str,
    chunk_size: int | None,
    snapshot: bool,
    invocation: Mapping[str, Any] | None,
) -> list[dict[str, Any]]:
    record = dict((plan.context.get("datasets") or {}).get(dataset) or {})
    record["name"] = dataset
    manifest = dataset_manifest(
        record, chunk_size=resolve_chunk_size(chunk_size, record, plan.execution)
    )
    directory = unit_dir(paths.compile_dir(), dataset)
    existing = directory / MANIFEST_NAME
    if existing.is_file():
        if manifest_bytes(read_manifest(existing)) != manifest_bytes(manifest):
            write_manifest(existing, manifest)  # raises PublishConflict with the hashes
        return []

    directory.mkdir(parents=True, exist_ok=True)
    if invocation is not None:
        write_invocation(
            directory,
            workflow_path=str(invocation.get("workflow_path") or ""),
            outdir=str(paths.root),
            root=str(invocation.get("root") or ""),
            invocation=invocation.get("caller"),
        )
    unit_normalized = stub_unselected_datasets(normalized, dataset)
    write_yaml(unit_normalized, str(directory / "normalized.yaml"))
    write_yaml(_lowered_graph_to_json(graph), str(directory / "analysis.ir.yaml"))
    entries = _dataset_entries_artifact({dataset: record})
    (directory / "dataset_entries.json").write_text(
        json.dumps(entries, indent=2, sort_keys=True), encoding="utf-8"
    )
    write_yaml(
        {
            "nodes": len(plan.nodes),
            "partitions": sum(1 for p in plan.partitions if p.dataset == dataset),
            "datasets": [dataset],
            "registry_sections": sorted(plan.registry.keys()),
        },
        str(directory / "report.compile.yaml"),
    )
    shared = {"plan": str(paths.compile_file("plan.yaml"))}
    records = write_compile_hook_artifacts(
        plan=plan,
        normalized=unit_normalized,
        build_paths=paths,
        artifacts={"dataset_entries": entries},
        when="after_datasets",
        directory=directory,
    )
    records.extend(
        write_compile_hook_artifacts(
            plan=plan,
            normalized=unit_normalized,
            build_paths=paths,
            artifacts=shared,
            when="after_compile",
            directory=directory,
        )
    )
    write_bundle_manifest(
        plan=plan,
        normalized=unit_normalized,
        build_paths=paths,
        records=records,
        chunk_size=chunk_size,
        snapshot=snapshot,
        directory=directory,
    )
    write_manifest(existing, manifest)
    return records


__all__ = ["PublishConflict", "write_dataset_unit_artifacts"]
