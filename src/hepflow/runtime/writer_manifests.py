from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from hepflow.build_layout import BuildPaths
from hepflow.model.io import OutputResult
from hepflow.model.plan import ExecutionPlan
from hepflow.runtime.provenance import write_artifact_provenance_records


def write_writer_manifests(
    plan: ExecutionPlan,
    *,
    stores: list[dict[tuple[str, str], Any]],
    outdir: str | Path,
) -> None:
    """Aggregate successful partition-writer results into one manifest per writer."""
    records_by_node: dict[str, list[dict[str, Any]]] = {}
    result_records: list[tuple[OutputResult, dict[str, Any]]] = []
    root = Path(outdir)
    for node in plan.nodes:
        if node.role != "sink":
            continue
        outputs = [
            output
            for store in stores
            for output in _writer_outputs(store.get((node.id, "artifact")))
        ]
        records = []
        for output in outputs:
            has_writer_manifest = isinstance(output.metadata.get("writer_manifest"), dict)
            copied_record = _artifact_record_from_output(output, node=node, outdir=root)
            result_records.append((output, copied_record))
            if has_writer_manifest:
                records.append(copied_record)
        if not records:
            continue
        records_by_node[node.id] = records

    provenance_links = write_artifact_provenance_records(
        plan=plan,
        writer_records=[record for _, record in result_records],
        outdir=outdir,
    )
    for output, record in result_records:
        provenance = provenance_links.get(_record_id(record))
        if not provenance:
            continue
        record["provenance"] = dict(provenance)
        output.provenance = dict(provenance)
        output.metadata["provenance"] = dict(provenance)
        writer_manifest = output.metadata.get("writer_manifest")
        if isinstance(writer_manifest, dict):
            writer_manifest["provenance"] = dict(provenance)

    for node in plan.nodes:
        records = records_by_node.get(node.id, [])
        if not records:
            continue
        manifest = _build_manifest(records)
        manifest_dir = BuildPaths(root=Path(outdir)).artifact_dir("files") / str(
            manifest["name"]
        )
        manifest_dir.mkdir(parents=True, exist_ok=True)
        (manifest_dir / "manifest.json").write_text(
            json.dumps(manifest, indent=2) + "\n",
            encoding="utf-8",
        )


def _writer_outputs(value: Any) -> list[OutputResult]:
    if isinstance(value, OutputResult):
        return [value]
    if isinstance(value, list):
        return [output for item in value for output in _writer_outputs(item)]
    return []


def _artifact_record_from_output(
    output: OutputResult,
    *,
    node: Any,
    outdir: Path,
) -> dict[str, Any]:
    writer_manifest = output.metadata.get("writer_manifest")
    if isinstance(writer_manifest, dict):
        return dict(writer_manifest)
    path, path_type = _artifact_path(output.path, outdir)
    return {
        "kind": str(
            output.metadata.get("artifact_kind")
            or output.metadata.get("kind")
            or output.format
            or output.kind
        ),
        "node_id": str(getattr(node, "id", "")),
        "input_node": _input_node(node),
        "path": path,
        "path_type": path_type,
        "dataset": output.metadata.get("dataset"),
        "partition": output.metadata.get("partition"),
        "attempt": output.metadata.get("attempt", 0),
    }


def _artifact_path(path: str | Path, outdir: Path) -> tuple[str, str]:
    candidate = Path(path)
    if not candidate.is_absolute():
        candidate = outdir / candidate
    resolved = candidate.resolve()
    resolved_outdir = outdir.resolve()
    try:
        return resolved.relative_to(resolved_outdir).as_posix(), "relative_to_outdir"
    except ValueError:
        return resolved.as_posix(), "absolute"


def _input_node(node: Any) -> str:
    inputs = list(getattr(node, "inputs", []) or [])
    if not inputs:
        return ""
    return str(getattr(inputs[0], "node_id", ""))


def _record_id(record: dict[str, Any]) -> str:
    payload = {
        "artifact": str(record.get("path") or ""),
        "node_id": str(record.get("node_id") or ""),
        "dataset": str(record.get("dataset") or ""),
        "partition": record.get("partition"),
        "attempt": record.get("attempt"),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    digest = hashlib.sha256(encoded).hexdigest()[:24]
    return f"artifact-{digest}"


def _build_manifest(records: list[dict[str, Any]]) -> dict[str, Any]:
    first = records[0]
    datasets: dict[str, dict[str, Any]] = {}
    total_entries = 0
    for record in sorted(
        records,
        key=lambda item: (
            str(item["dataset"]),
            int(item["partition"]),
            int(item["attempt"]),
            str(item["path"]),
        ),
    ):
        dataset_name = str(record["dataset"])
        entries = int(record.get("entries") or 0)
        dataset = datasets.setdefault(
            dataset_name,
            {"total_entries": 0, "files": []},
        )
        dataset["total_entries"] += entries
        total_entries += entries
        dataset["files"].append(
            {
                key: record[key]
                for key in (
                    "path",
                    "path_type",
                    "dataset",
                    "partition",
                    "attempt",
                    "entries",
                    "size_bytes",
                    "provenance",
                )
                if key in record
            }
        )
    return {
        "kind": first["kind"],
        "name": first["name"],
        "node_id": first["node_id"],
        "input_node": first["input_node"],
        "tree": first["tree"],
        "total_entries": total_entries,
        "datasets": datasets,
    }
