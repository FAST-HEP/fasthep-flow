"""Compile output of the per-dataset mode: shared files plus one unit per dataset."""

from __future__ import annotations

import json
import secrets
import shutil
import tempfile
import time
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
    workflow_content_hash,
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

# How long a compile waits for another compile that holds a unit before it treats it as stale.
UNIT_WAIT_SECONDS = 10.0


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


def _version_hint(published: Path, candidate: dict[str, Any]) -> str:
    """Say so when the plans differ in the Flow version that compiled them."""

    def version(doc: Any) -> Any:
        return ((doc or {}).get("provenance") or {}).get("compilation", {}).get(
            "flow_version"
        )

    try:
        existing = yaml.safe_load(published.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError):
        return ""
    old, new = version(existing), version(candidate)
    if old == new:
        return ""
    return (
        f" (compiled by Flow {old} and {new}: a new Flow version needs a new out-dir"
        " or a recompile of every dataset)"
    )


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
    for name in datasets:
        unit_dir(compile_path, name)  # a bad name fails before anything is published
    ensure_build_layout(
        paths.root,
        variation=paths.variation,
        backend_directories=backend_build_directories(
            plan.registry, plan.execution, provenance=plan.provenance
        ),
    )

    workflow_hash = workflow_content_hash(normalized)
    shared_plan = shared_plan_dict(plan, workflow_hash=workflow_hash)
    try:
        publish_if_absent(compile_path / "plan.yaml", _yaml_bytes(shared_plan))
    except PublishConflict as error:
        raise PublishConflict(
            f"{error}{_version_hint(compile_path / 'plan.yaml', shared_plan)}"
        ) from None
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
        record,
        chunk_size=resolve_chunk_size(chunk_size, record, plan.execution),
        workflow_hash=workflow_content_hash(normalized),
    )
    directory = unit_dir(paths.compile_dir(), dataset)
    existing = directory / MANIFEST_NAME
    if directory.exists() and _wait_for_manifest(existing):
        _check_same_unit(existing, manifest)
        return []
    if directory.exists():
        # An earlier compile was interrupted before its unit was complete; redo that unit only.
        shutil.rmtree(directory)

    # The unit is built beside its final place. Directory rename onto an existing directory is
    # not refused on every filesystem (EOS FUSE replaces it), so the claim is an exclusive
    # `mkdir`, which is. The files then move in with the manifest last: a unit without a
    # manifest is incomplete, and a second writer of the same dataset never touches it.
    staging = directory.parent / f".{directory.name}.tmp-{secrets.token_hex(4)}"
    staging.mkdir(parents=True)
    try:
        records = _build_unit(
            staging,
            plan=plan,
            graph=graph,
            paths=paths,
            normalized=normalized,
            dataset=dataset,
            record=record,
            manifest=manifest,
            chunk_size=chunk_size,
            snapshot=snapshot,
            invocation=invocation,
        )
        try:
            directory.mkdir()
        except FileExistsError:
            if not _wait_for_manifest(existing):
                raise PublishConflict(
                    f"Dataset unit {directory} is being written by another compile or was "
                    "left incomplete by one; retry, or remove the directory if no compile runs."
                ) from None
            _check_same_unit(existing, manifest)  # another compile won the claim
            return []
        for entry in sorted(staging.iterdir(), key=lambda p: p.name == MANIFEST_NAME):
            entry.rename(directory / entry.name)  # manifest sorts last
    finally:
        shutil.rmtree(staging, ignore_errors=True)
    return records


def _wait_for_manifest(manifest_file: Path) -> bool:
    """Wait briefly for a compile that is filling the unit; False when no manifest appears."""
    deadline = time.monotonic() + UNIT_WAIT_SECONDS
    while not manifest_file.is_file():
        if time.monotonic() >= deadline:
            return False
        time.sleep(0.05)
    return True


def _check_same_unit(existing: Path, manifest: dict[str, Any]) -> None:
    if manifest_bytes(read_manifest(existing)) != manifest_bytes(manifest):
        write_manifest(existing, manifest)  # raises PublishConflict with both hashes


def _build_unit(
    directory: Path,
    *,
    plan: ExecutionPlan,
    graph: nx.DiGraph,
    paths: BuildPaths,
    normalized: dict[str, Any],
    dataset: str,
    record: dict[str, Any],
    manifest: dict[str, Any],
    chunk_size: int | None,
    snapshot: bool,
    invocation: Mapping[str, Any] | None,
) -> list[dict[str, Any]]:
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
    (directory / MANIFEST_NAME).write_bytes(manifest_bytes(manifest))  # last: unit complete
    return records


__all__ = ["PublishConflict", "write_dataset_unit_artifacts"]
