from __future__ import annotations

import json
import re
from collections import Counter
from contextlib import suppress
from pathlib import Path
from typing import Any

from hepflow.build_layout import BuildPaths

GraphFormat = str


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


def format_provenance_graph(
    artifact_path: str | Path,
    *,
    output_format: GraphFormat = "mermaid",
) -> str:
    """Return the provenance workflow ancestor graph for one artifact."""
    graph = build_provenance_graph(artifact_path)
    if output_format == "mermaid":
        return _render_graph_mermaid(graph)
    if output_format == "dot":
        return _render_graph_dot(graph)
    if output_format == "json":
        return json.dumps(graph, indent=2, sort_keys=True)
    raise ValueError(f"Unsupported provenance graph format: {output_format}")


def build_provenance_graph(artifact_path: str | Path) -> dict[str, Any]:
    artifact = Path(artifact_path)
    root = _find_outdir_for_artifact(artifact)
    manifest, execution, records = _load_bundle(root)
    entry = _find_manifest_record(manifest, root=root, artifact_path=artifact)
    record = records[str(entry["record"])]
    producer = dict(record.get("producer") or {})
    producer_node = str(producer.get("node_id") or "")
    if not producer_node:
        raise ValueError(f"Provenance record has no producer node: {entry['record']}")

    graph_ref = str(dict(execution.get("workflow") or {}).get("graph") or "")
    graph_doc = _read_json(root / graph_ref)
    graph_nodes = _graph_nodes_by_id(graph_doc)
    graph_edges = _graph_edges(graph_doc)
    selected = _ancestor_nodes(producer_node, graph_edges)
    selected.add(producer_node)

    record_entries = {
        str(item.get("record")): dict(item)
        for item in list(manifest.get("records") or [])
        if item.get("record")
    }
    records_by_node: dict[str, list[dict[str, Any]]] = {}
    for item in record_entries.values():
        node_id = str(item.get("node_id") or "")
        if node_id:
            records_by_node.setdefault(node_id, []).append(item)

    partitions = [
        dict(partition)
        for partition in list(execution.get("partitions") or [])
        if isinstance(partition, dict)
    ]

    nodes = [
        _graph_node_entry(
            node_id=node_id,
            graph_node=graph_nodes.get(node_id, {"id": node_id}),
            producer_node=producer_node,
            partitions=partitions,
            records=records_by_node.get(node_id, []),
        )
        for node_id in _ordered_selected_nodes(graph_doc, selected)
    ]
    edges = [
        {
            key: edge.get(key)
            for key in ("source", "target", "output", "input_name")
            if key in edge
        }
        for edge in graph_edges
        if edge["source"] in selected and edge["target"] in selected
    ]
    return {
        "artifact": dict(record.get("artifact") or {}),
        "producer": producer,
        "nodes": nodes,
        "edges": edges,
        "inputs": list(record.get("inputs") or []),
        "related_records": [
            record_entry
            for node in nodes
            for record_entry in records_by_node.get(str(node.get("id")), [])
        ],
    }


def operation_resource_records(execution: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Return resource records referenced by operation inputs."""
    resources = dict(execution.get("resources") or {})
    referenced = sorted(
        {
            str(resource_id)
            for item in list(dict(execution.get("executions") or {}).values())
            if isinstance(item, dict)
            for operation in list(item.get("operations") or [])
            if isinstance(operation, dict)
            for resource_id in list(dict(operation.get("inputs") or {}).get("resources") or [])
        }
    )
    return {
        resource_id: dict(resources[resource_id])
        for resource_id in referenced
        if isinstance(resources.get(resource_id), dict)
    }


def resolve_operation_resources(
    operation: dict[str, Any],
    execution: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    """Resolve one operation's input resource ids to run-level resource records."""
    resources = dict(execution.get("resources") or {})
    input_resources = list(dict(operation.get("inputs") or {}).get("resources") or [])
    missing = [resource_id for resource_id in input_resources if resource_id not in resources]
    if missing:
        raise KeyError(
            "Operation references unresolved provenance resources: "
            + ", ".join(str(item) for item in missing)
        )
    return {
        str(resource_id): dict(resources[str(resource_id)])
        for resource_id in input_resources
        if isinstance(resources.get(str(resource_id)), dict)
    }


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


def _graph_nodes_by_id(graph_doc: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        str(node.get("id")): dict(node)
        for node in list(graph_doc.get("nodes") or [])
        if isinstance(node, dict) and node.get("id")
    }


def _graph_edges(graph_doc: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        dict(edge)
        for edge in list(graph_doc.get("edges") or [])
        if isinstance(edge, dict) and edge.get("source") and edge.get("target")
    ]


def _ancestor_nodes(producer_node: str, edges: list[dict[str, Any]]) -> set[str]:
    parents: dict[str, list[str]] = {}
    for edge in edges:
        parents.setdefault(str(edge["target"]), []).append(str(edge["source"]))

    selected: set[str] = set()
    pending = list(parents.get(producer_node, []))
    while pending:
        node_id = pending.pop()
        if node_id in selected:
            continue
        selected.add(node_id)
        pending.extend(parents.get(node_id, []))
    return selected


def _ordered_selected_nodes(
    graph_doc: dict[str, Any],
    selected: set[str],
) -> list[str]:
    ordered = [
        str(node.get("id"))
        for node in list(graph_doc.get("nodes") or [])
        if isinstance(node, dict) and str(node.get("id")) in selected
    ]
    for node_id in sorted(selected):
        if node_id not in ordered:
            ordered.append(node_id)
    return ordered


def _graph_node_entry(
    *,
    node_id: str,
    graph_node: dict[str, Any],
    producer_node: str,
    partitions: list[dict[str, Any]],
    records: list[dict[str, Any]],
) -> dict[str, Any]:
    payload = str(graph_node.get("payload") or "")
    role = _graph_payload_field(payload, "role")
    impl = _graph_payload_field(payload, "impl")
    entry: dict[str, Any] = {
        "id": node_id,
        "role": role,
        "impl": impl,
        "producer": node_id == producer_node,
    }
    if role == "source":
        source_inputs = _source_inputs(node_id, payload, partitions)
        if source_inputs:
            entry["source_inputs"] = source_inputs
    if records:
        entry["artifacts"] = [
            {
                key: record.get(key)
                for key in ("artifact", "kind", "record")
                if key in record
            }
            for record in records
        ]
    return entry


def _graph_payload_field(payload: str, field: str) -> str:
    match = re.search(rf"{re.escape(field)}='([^']*)'", payload)
    return match.group(1) if match else ""


def _source_inputs(
    node_id: str,
    payload: str,
    partitions: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    source_name = node_id.removeprefix("read.")
    meta_match = re.search(r"'source_name': '([^']*)'", payload)
    if meta_match:
        source_name = meta_match.group(1)
    return [
        {
            key: partition.get(key)
            for key in ("id", "dataset", "source", "file", "part")
            if key in partition
        }
        for partition in partitions
        if str(partition.get("source") or "") == source_name
    ]


def _render_graph_mermaid(graph: dict[str, Any]) -> str:
    lines = ["flowchart TD"]
    for node in list(graph.get("nodes") or []):
        node_id = str(node["id"])
        label = _graph_node_label(dict(node), html=True)
        lines.append(f'  {_mermaid_id(node_id)}["{_escape_mermaid(label)}"]')
    for edge in list(graph.get("edges") or []):
        source = str(edge["source"])
        target = str(edge["target"])
        lines.append(f"  {_mermaid_id(source)} --> {_mermaid_id(target)}")
    return "\n".join(lines) + "\n"


def _render_graph_dot(graph: dict[str, Any]) -> str:
    lines = ["digraph provenance {"]
    for node in list(graph.get("nodes") or []):
        node_id = str(node["id"])
        label = _graph_node_label(dict(node), html=False)
        lines.append(f'  "{_dot_escape(node_id)}" [label="{_dot_escape(label)}"];')
    for edge in list(graph.get("edges") or []):
        lines.append(
            f'  "{_dot_escape(str(edge["source"]))}" -> '
            f'"{_dot_escape(str(edge["target"]))}";'
        )
    lines.append("}")
    return "\n".join(lines) + "\n"


def _graph_node_label(node: dict[str, Any], *, html: bool) -> str:
    sep = "<br/>" if html else "\\n"
    parts = [str(node.get("id") or "")]
    if node.get("role"):
        parts.append(str(node["role"]))
    if node.get("impl"):
        parts.append(str(node["impl"]))
    if node.get("producer"):
        parts.append("producer")
    artifacts = list(node.get("artifacts") or [])
    if artifacts:
        kinds = sorted({str(item.get("kind") or "artifact") for item in artifacts})
        parts.append(f"produces: {', '.join(kinds)}")
    source_inputs = list(node.get("source_inputs") or [])
    if source_inputs:
        datasets = sorted(
            {
                str(item.get("dataset"))
                for item in source_inputs
                if item.get("dataset")
            }
        )
        if datasets:
            parts.append(f"inputs: {', '.join(datasets)}")
    return sep.join(parts)


def _mermaid_id(node_id: str) -> str:
    return re.sub(r"[^0-9A-Za-z_]", "_", node_id)


def _escape_mermaid(value: str) -> str:
    return str(value).replace("\\", "\\\\").replace('"', "&quot;")


def _dot_escape(value: str) -> str:
    return str(value).replace("\\", "\\\\").replace('"', '\\"')


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
        bits = [str(partition.get("id") or "")]
        for key in ("dataset", "source", "file", "part"):
            if partition.get(key) is not None:
                bits.append(f"{key}={partition[key]}")
        lines.append("  " + " ".join(bit for bit in bits if bit))
    return lines


__all__ = [
    "build_provenance_graph",
    "format_provenance_artifact",
    "format_provenance_graph",
    "format_provenance_summary",
    "operation_resource_records",
    "resolve_operation_resources",
]
