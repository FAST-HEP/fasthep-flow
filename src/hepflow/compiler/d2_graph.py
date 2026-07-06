from __future__ import annotations

from dataclasses import dataclass, field
from importlib import resources
from typing import Any

import networkx as nx
from jinja2 import Environment

from hepflow.model.graph import GraphNode


@dataclass(frozen=True, slots=True)
class D2Node:
    id: str
    title: str
    role: str
    type: str
    meta: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class D2Edge:
    source: str
    target: str
    label: str
    kind: str


def lowered_graph_to_d2(graph: nx.DiGraph) -> str:
    """Render a lowered compile graph as editable D2 text."""
    nodes = [
        _normalize_node(node_id, graph.nodes[node_id]["payload"], graph)
        for node_id in graph.nodes
    ]
    edges = [
        _normalize_edge(source, target, edge_data, graph)
        for source, target, edge_data in graph.edges(data=True)
    ]
    template = _template()
    return template.render(nodes=nodes, edges=edges)


def _normalize_node(node_id: str, payload: GraphNode, graph: nx.DiGraph) -> D2Node:
    meta = dict(payload.meta or {})
    meta["node_id"] = node_id
    if payload.role == "observer":
        target_title = _observed_target_title(payload, graph)
        if target_title:
            meta["display"] = {"Target": target_title}
    return D2Node(
        id=node_id,
        title=_node_short_title(payload, graph),
        role=str(payload.role),
        type=_node_type(payload),
        meta=meta,
    )


def _normalize_edge(
    source: str,
    target: str,
    edge_data: dict[str, Any],
    graph: nx.DiGraph,
) -> D2Edge:
    label = str(edge_data.get("output") or edge_data.get("input_name") or "")
    target_payload = graph.nodes[target]["payload"]
    if target_payload.role == "observer":
        label = f"observes {label}" if label else "observes"
        kind = "report"
    else:
        kind = _edge_kind(label)
    return D2Edge(
        source=source,
        target=target,
        label=label or "unknown",
        kind=kind,
    )


def _node_short_title(payload: GraphNode, graph: nx.DiGraph | None = None) -> str:
    meta = dict(payload.meta or {})
    if payload.role == "source":
        return _first_nonempty(meta.get("source_name"), payload.id)
    if payload.role == "transform":
        return _first_nonempty(meta.get("stage_id"), payload.id)
    if payload.role == "sink":
        if payload.id.startswith("render."):
            return _first_nonempty(meta.get("render_id"), payload.id.removeprefix("render."))
        return _first_nonempty(meta.get("stage_id"), payload.id)
    if payload.role == "observer":
        title = _observer_title(payload)
        target_title = _observed_target_title(payload, graph)
        if title != "Observer" or not target_title:
            return title
        return f"{title}: {target_title}"
    return payload.id


def _node_type(payload: GraphNode) -> str:
    meta = dict(payload.meta or {})
    if payload.role == "source":
        return _first_nonempty(meta.get("author_kind"), payload.impl)
    if payload.role == "transform":
        return _first_nonempty(meta.get("author_op"), payload.impl)
    if payload.role == "sink":
        return _first_nonempty(meta.get("author_kind"), payload.impl)
    return payload.impl


def _observer_title(payload: GraphNode) -> str:
    if payload.impl == "hep.schema_snapshot":
        return "Schema snapshot"
    return _first_nonempty(payload.params.get("name"), payload.impl, "Observer")


def _observed_target_title(payload: GraphNode, graph: nx.DiGraph | None) -> str | None:
    if graph is None:
        return None
    observed_node = str((payload.meta or {}).get("observed_node") or "")
    if not observed_node or observed_node not in graph.nodes:
        return None
    target_payload = graph.nodes[observed_node]["payload"]
    return _node_short_title(target_payload, graph)


def _edge_kind(label: str) -> str:
    value = str(label or "").strip()
    if value in {"stream", "artifact", "report"}:
        return value
    if value:
        return "product"
    return "unknown"


def _first_nonempty(*values: Any) -> str:
    for value in values:
        if isinstance(value, str) and value.strip():
            return value.strip()
    return "unknown"


def _template():
    template_text = (
        resources.files("hepflow.render.templates")
        .joinpath("compile_graph.d2.j2")
        .read_text(encoding="utf-8")
    )
    env = Environment(
        autoescape=False,
        keep_trailing_newline=True,
        trim_blocks=True,
        lstrip_blocks=True,
    )
    env.filters["d2_string"] = _d2_string
    return env.from_string(template_text)


def _d2_string(value: str) -> str:
    return '"' + str(value).replace("\\", "\\\\").replace('"', '\\"') + '"'
