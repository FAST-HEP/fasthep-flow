from __future__ import annotations

import json
from collections import Counter
from contextlib import suppress
from pathlib import Path
from typing import Any

from hepflow.build_layout import BuildPaths


def format_provenance_summary(outdir: str | Path) -> str:
    """Return a compact text summary of an output directory's provenance."""
    root = Path(outdir)
    manifest, execution, _ = _load_bundle(root)
    records = list(manifest.get("records") or [])
    kinds = Counter(str(record.get("kind") or "artifact") for record in records)
    datasets = sorted(
        {
            str(partition.get("dataset"))
            for partition in list(execution.get("partitions") or [])
            if partition.get("dataset")
        }
    )
    software = dict(execution.get("software") or {})

    lines = [
        "Provenance summary",
        f"Run ID: {manifest.get('run_id') or execution.get('run_id') or ''}",
        f"Records: {len(records)}",
        f"Artifact kinds: {_format_counts(kinds)}",
        f"Datasets: {_format_list(datasets)}",
        "Software:",
    ]
    lines.extend(_format_mapping(software))
    return "\n".join(lines)


def format_provenance_artifact(artifact_path: str | Path) -> str:
    """Return compact text provenance for one artifact path."""
    artifact = Path(artifact_path)
    root = _find_outdir_for_artifact(artifact)
    manifest, execution, records = _load_bundle(root)
    entry = _find_manifest_record(manifest, root=root, artifact_path=artifact)
    record = records[str(entry["record"])]

    producer = dict(record.get("producer") or {})
    workflow = dict(execution.get("workflow") or {})
    runtime = dict(execution.get("execution") or {})
    software = dict(execution.get("software") or {})
    partitions = {
        str(partition.get("id")): dict(partition)
        for partition in list(execution.get("partitions") or [])
        if partition.get("id")
    }
    input_partitions = [
        partitions.get(str(item.get("partition_id")), {"id": item.get("partition_id")})
        for item in list(record.get("inputs") or [])
        if isinstance(item, dict)
    ]

    artifact_doc = dict(record.get("artifact") or {})
    lines = [
        "Artifact provenance",
        f"Artifact: {artifact_doc.get('path') or entry.get('artifact') or ''}",
        f"Kind: {artifact_doc.get('kind') or entry.get('kind') or ''}",
        "Producer:",
        f"  node: {producer.get('node_id') or ''}",
        f"  partition: {producer.get('partition_id') or ''}",
        f"  execution: {producer.get('execution_id') or ''}",
        "Inputs:",
    ]
    lines.extend(_format_input_partitions(input_partitions))
    lines.extend(
        [
            "Workflow:",
            f"  graph: {workflow.get('graph') or ''}",
            f"  plan: {workflow.get('plan') or ''}",
            f"  normalized: {workflow.get('normalized') or ''}",
            "Software:",
        ]
    )
    lines.extend(_format_mapping(software, indent="  "))
    lines.extend(
        [
            "Execution:",
            f"  host: {runtime.get('hostname') or ''}",
            f"  platform: {runtime.get('platform') or ''}",
            f"  python: {runtime.get('python_version') or ''}",
        ]
    )
    return "\n".join(lines)


def _load_bundle(
    outdir: Path,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, dict[str, Any]]]:
    paths = BuildPaths(root=outdir)
    manifest = _read_json(paths.provenance_manifest())
    execution_ref = manifest.get("execution") or "artifacts/provenance/execution.json"
    execution = _read_json(paths.root / str(execution_ref))
    records = {
        str(entry["record"]): _read_json(paths.root / str(entry["record"]))
        for entry in list(manifest.get("records") or [])
        if entry.get("record")
    }
    return manifest, execution, records


def _read_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise FileNotFoundError(f"Provenance file not found: {path}")
    doc = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(doc, dict):
        raise ValueError(f"Expected JSON object in {path}")
    return doc


def _find_outdir_for_artifact(path: Path) -> Path:
    candidate = path.resolve() if path.exists() else path.absolute()
    for parent in [candidate.parent, *candidate.parents]:
        if (parent / "artifacts" / "provenance" / "manifest.json").is_file():
            return parent
    raise FileNotFoundError(
        f"Could not find artifacts/provenance/manifest.json above {path}"
    )


def _find_manifest_record(
    manifest: dict[str, Any],
    *,
    root: Path,
    artifact_path: Path,
) -> dict[str, Any]:
    artifact_refs = _artifact_match_refs(root=root, artifact_path=artifact_path)
    for entry in list(manifest.get("records") or []):
        artifact = str(entry.get("artifact") or "")
        if artifact in artifact_refs:
            return dict(entry)
    raise FileNotFoundError(f"No provenance record found for artifact {artifact_path}")


def _artifact_match_refs(root: Path, artifact_path: Path) -> set[str]:
    refs = {artifact_path.as_posix(), str(artifact_path)}
    candidate = artifact_path.resolve() if artifact_path.exists() else artifact_path.absolute()
    refs.add(candidate.as_posix())
    with suppress(ValueError):
        refs.add(candidate.relative_to(root.resolve()).as_posix())
    return refs


def _format_counts(counts: Counter[str]) -> str:
    if not counts:
        return "(none)"
    return ", ".join(f"{name}={count}" for name, count in sorted(counts.items()))


def _format_list(items: list[str]) -> str:
    return ", ".join(items) if items else "(none)"


def _format_mapping(mapping: dict[str, Any], *, indent: str = "  ") -> list[str]:
    if not mapping:
        return [f"{indent}(none)"]
    return [f"{indent}{key}: {value}" for key, value in sorted(mapping.items())]


def _format_input_partitions(partitions: list[dict[str, Any]]) -> list[str]:
    if not partitions:
        return ["  (none)"]
    lines: list[str] = []
    for partition in partitions:
        part_id = partition.get("id") or ""
        dataset = partition.get("dataset") or ""
        source = partition.get("source") or ""
        file_path = partition.get("file") or ""
        part = partition.get("part") or ""
        lines.append(
            f"  {part_id} dataset={dataset} source={source} "
            f"file={file_path} part={part}"
        )
    return lines
