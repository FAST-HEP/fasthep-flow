from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

import networkx as nx

Role = Literal["source", "transform", "observer", "sink"]

@dataclass(slots=True)
class GraphNode:
    """
    Payload stored on each graph node.
    """

    id: str
    role: Role
    impl: str
    params: dict[str, Any] = field(default_factory=dict)
    outputs: dict[str, str] = field(default_factory=dict)
    meta: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class EdgeBinding:
    """
    Describes how one node output is connected to an input port on another node.
    """

    output: str
    input_name: str


def new_graph() -> nx.DiGraph:
    """
    Create an empty directed graph for the lowered IR.

    Node attributes:
      payload: GraphNode

    Edge attributes:
      output: upstream output port name
      input_name: downstream input name
    """
    return nx.DiGraph()


def add_graph_node(graph: nx.DiGraph, node: GraphNode) -> None:
    if node.id in graph:
        raise ValueError(f"Duplicate graph node id: {node.id}")
    graph.add_node(node.id, payload=node)


def add_graph_edge(
    graph: nx.DiGraph,
    upstream: str,
    downstream: str,
    *,
    output: str = "stream",
    input_name: str = "stream",
) -> None:
    if upstream not in graph:
        raise KeyError(f"Unknown upstream node: {upstream}")
    if downstream not in graph:
        raise KeyError(f"Unknown downstream node: {downstream}")

    graph.add_edge(
        upstream,
        downstream,
        output=output,
        input_name=input_name,
    )


def get_graph_node(graph: nx.DiGraph, node_id: str) -> GraphNode:
    try:
        return graph.nodes[node_id]["payload"]
    except KeyError as exc:
        raise KeyError(f"Unknown graph node id: {node_id}") from exc


def upstream_binding(
    graph: nx.DiGraph,
    node_id: str,
    input_name: str,
) -> tuple[str, str] | None:
    """
    Return the upstream (node_id, output_port) feeding a given input_name,
    or None if there is no such edge.
    """
    for upstream, _, edge_data in graph.in_edges(node_id, data=True):
        if edge_data.get("input_name") == input_name:
            return upstream, edge_data.get("output", "stream")
    return None
