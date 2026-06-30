from __future__ import annotations

import hashlib
import json
import platform
import socket
import sys
import uuid
from importlib import metadata
from pathlib import Path
from typing import Any

from hepflow.build_layout import BuildPaths
from hepflow.model.plan import ExecutionPlan

PROVENANCE_VERSION = "1.0"


def write_artifact_provenance_records(
    *,
    plan: ExecutionPlan,
    writer_records: list[dict[str, Any]],
    outdir: str | Path,
) -> dict[str, dict[str, str]]:
    """
    Write generic artifact provenance records and a provenance manifest.

    Writer records are the first integration point because they already know
    produced artifact paths, node identity, data lineage, and
    file sizes without reopening output files.
    """
    if not writer_records:
        return {}

    paths = BuildPaths(root=Path(outdir))
    manifest_path = paths.provenance_manifest()
    execution_path = paths.provenance_execution()
    records_dir = paths.provenance_records_dir()
    records_dir.mkdir(parents=True, exist_ok=True)

    run_id = _existing_run_id(manifest_path) or _plan_run_id(plan) or str(uuid.uuid4())
    _write_json(
        execution_path,
        _execution_index(
            plan=plan,
            run_id=run_id,
            paths=paths,
            writer_records=writer_records,
        ),
    )

    manifest_records: list[dict[str, str]] = []
    provenance_by_record_id: dict[str, dict[str, str]] = {}
    seen_records: set[str] = set()
    for writer_record in sorted(writer_records, key=_record_sort_key):
        record_id = _record_id(writer_record)
        record_rel = f"artifacts/provenance/records/{record_id}.json"
        if record_id in seen_records:
            continue
        seen_records.add(record_id)
        artifact_path = str(writer_record["path"])
        record_doc = {
            "version": PROVENANCE_VERSION,
            "run_id": run_id,
            "artifact": {
                "path": artifact_path,
                "path_type": str(
                    writer_record.get("path_type") or "relative_to_outdir"
                ),
                "kind": str(writer_record.get("kind") or "artifact"),
            },
            "producer": _record_producer(writer_record),
            "inputs": _record_inputs(writer_record),
        }
        record_doc = _drop_none(record_doc)
        record_hash = _content_hash(record_doc)
        _write_json(records_dir / f"{record_id}.json", record_doc)
        provenance_link = {
            "record": record_rel,
            "record_hash": record_hash,
        }
        provenance_by_record_id[record_id] = provenance_link
        manifest_records.append(
            {
                "artifact": artifact_path,
                "kind": str(writer_record.get("kind") or "artifact"),
                "node_id": str(writer_record.get("node_id") or ""),
                "record": record_rel,
                "record_hash": record_hash,
            }
        )

    manifest = {
        "version": PROVENANCE_VERSION,
        "run_id": run_id,
        "execution": _path_ref(paths, execution_path),
        "records": manifest_records,
    }
    _write_json(manifest_path, manifest)
    links: dict[str, dict[str, str]] = {}
    for writer_record in writer_records:
        record_id = _record_id(writer_record)
        if record_id in provenance_by_record_id:
            links[record_id] = dict(provenance_by_record_id[record_id])
    return links


def _record_sort_key(record: dict[str, Any]) -> tuple[str, str, int, int, str]:
    return (
        str(record.get("node_id") or ""),
        str(record.get("dataset") or ""),
        _safe_int(record.get("partition")),
        _safe_int(record.get("attempt")),
        str(record.get("path") or ""),
    )


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


def _record_inputs(record: dict[str, Any]) -> list[dict[str, Any]]:
    raw_inputs = record.get("inputs")
    if isinstance(raw_inputs, list):
        return [
            {"partition_id": item.get("id")}
            for item in raw_inputs
            if isinstance(item, dict) and item.get("id")
        ]
    return []


def _record_producer(record: dict[str, Any]) -> dict[str, str]:
    node_id = str(record.get("node_id") or "")
    partition_id = _producer_partition_id(record)
    return _drop_none(
        {
            "node_id": node_id,
            "partition_id": partition_id,
            "execution_id": _node_execution_id(node_id, partition_id),
        }
    )


def _producer_partition_id(record: dict[str, Any]) -> str | None:
    inputs = _record_inputs(record)
    if inputs:
        partition_id = inputs[0].get("partition_id")
        return str(partition_id) if partition_id else None
    return None


def _node_execution_id(node_id: str, partition_id: str | None) -> str:
    if partition_id:
        return f"{node_id}::{partition_id}"
    return node_id


def _execution_index(
    *,
    plan: ExecutionPlan,
    run_id: str,
    paths: BuildPaths,
    writer_records: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "version": PROVENANCE_VERSION,
        "run_id": run_id,
        "workflow": _workflow_references(paths),
        "software": _software_versions(),
        "execution": _execution_context(),
        "partitions": _partition_index(plan, writer_records),
        "node_executions": _node_execution_index(writer_records),
    }


def _partition_index(
    plan: ExecutionPlan,
    records: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    partitions: dict[str, dict[str, Any]] = {}
    for partition in plan.partitions:
        item = partition.to_context()
        partitions[str(item["id"])] = item
    for record in records:
        raw_inputs = record.get("inputs")
        if not isinstance(raw_inputs, list):
            continue
        for item in raw_inputs:
            if not isinstance(item, dict) or not item.get("id"):
                continue
            partitions.setdefault(str(item["id"]), _partition_entry(item))
    return list(partitions.values())


def _partition_entry(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": item.get("id"),
        "dataset": item.get("dataset"),
        "file": item.get("file"),
        "source": item.get("source"),
        "part": item.get("part"),
        "start": item.get("start"),
        "stop": item.get("stop"),
    }


def _node_execution_index(records: list[dict[str, Any]]) -> list[dict[str, str]]:
    node_executions: dict[str, dict[str, str]] = {}
    for record in sorted(records, key=_record_sort_key):
        node_id = str(record.get("node_id") or "")
        if not node_id:
            continue
        partition_id = _producer_partition_id(record)
        execution_id = _node_execution_id(node_id, partition_id)
        node_executions.setdefault(
            execution_id,
            _drop_none(
                {
                    "id": execution_id,
                    "node_id": node_id,
                    "partition_id": partition_id,
                }
            ),
        )
    return list(node_executions.values())


def _workflow_references(paths: BuildPaths) -> dict[str, str]:
    return {
        "normalized": _path_ref(paths, paths.compile_file("normalized.yaml")),
        "graph": _path_ref(paths, paths.graph_file("graph.json")),
        "plan": _path_ref(paths, paths.compile_file("plan.yaml")),
    }


def _path_ref(paths: BuildPaths, path: Path) -> str:
    try:
        return path.resolve().relative_to(paths.root.resolve()).as_posix()
    except ValueError:
        return path.resolve().as_posix()


def _software_versions() -> dict[str, str]:
    package_names = [
        "fasthep-flow",
        "fasthep-carpenter",
        "fasthep-curator",
        "fasthep-render",
        "fasthep-cli",
    ]
    versions: dict[str, str] = {}
    for package_name in package_names:
        try:
            versions[package_name] = metadata.version(package_name)
        except metadata.PackageNotFoundError:
            continue
    return versions


def _execution_context() -> dict[str, str]:
    return {
        "cwd": str(Path.cwd()),
        "hostname": socket.gethostname(),
        "fqdn": socket.getfqdn(),
        "platform": platform.platform(),
        "python_version": platform.python_version(),
        "python_executable": sys.executable,
    }


def _content_hash(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def _existing_run_id(path: Path) -> str | None:
    if not path.is_file():
        return None
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    run_id = doc.get("run_id") if isinstance(doc, dict) else None
    return run_id if isinstance(run_id, str) and run_id else None


def _plan_run_id(plan: ExecutionPlan) -> str | None:
    run_id = plan.provenance.get("run_id")
    return run_id if isinstance(run_id, str) and run_id else None


def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _drop_none(value: dict[str, Any]) -> dict[str, Any]:
    return {key: item for key, item in value.items() if item is not None}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
