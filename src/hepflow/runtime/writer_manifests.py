from __future__ import annotations

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
    all_records: list[dict[str, Any]] = []
    for node in plan.nodes:
        if node.role != "sink":
            continue
        records = [
            record
            for store in stores
            for record in _writer_records(store.get((node.id, "artifact")))
        ]
        if not records:
            continue
        all_records.extend(records)
        manifest = _build_manifest(records)
        manifest_dir = BuildPaths(root=Path(outdir)).artifact_dir("files") / str(
            manifest["name"]
        )
        manifest_dir.mkdir(parents=True, exist_ok=True)
        (manifest_dir / "manifest.json").write_text(
            json.dumps(manifest, indent=2) + "\n",
            encoding="utf-8",
        )
    write_artifact_provenance_records(
        plan=plan,
        writer_records=all_records,
        outdir=outdir,
    )


def _writer_records(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, OutputResult):
        record = value.metadata.get("writer_manifest")
        return [dict(record)] if isinstance(record, dict) else []
    if isinstance(value, list):
        return [record for item in value for record in _writer_records(item)]
    return []


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
                )
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
