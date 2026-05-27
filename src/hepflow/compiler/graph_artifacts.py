from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import networkx as nx


def write_graph_artifacts(
    graph: nx.DiGraph,
    outdir: str | Path,
    *,
    execution_hooks: list[dict[str, Any]] | None = None,
    with_hooks: bool = False,
) -> dict[str, str]:
    out_path = Path(outdir)
    out_path.mkdir(parents=True, exist_ok=True)

    mermaid_path = out_path / "graph.mmd"
    mermaid_path.write_text(
        _lowered_graph_to_mermaid(
            graph,
            execution_hooks=execution_hooks,
            with_hooks=with_hooks,
        ),
        encoding="utf-8",
    )

    dot_path = out_path / "graph.dot"
    dot_path.write_text(_lowered_graph_to_dot(graph), encoding="utf-8")

    json_path = out_path / "graph.json"
    json_path.write_text(
        json.dumps(_lowered_graph_to_json(graph), indent=2, sort_keys=True),
        encoding="utf-8",
    )

    png_path = out_path / "graph.png"
    _write_graph_png_if_available(graph, png_path)

    return {
        "graph_mermaid": str(mermaid_path),
        "graph_dot": str(dot_path),
        "graph_json": str(json_path),
        "graph_png": str(png_path),
    }


def _lowered_graph_to_json(graph: nx.DiGraph) -> dict[str, Any]:
    return {
        "nodes": [
            {
                "id": node_id,
                **{
                    key: _json_safe_graph_value(value)
                    for key, value in attrs.items()
                },
            }
            for node_id, attrs in graph.nodes(data=True)
        ],
        "edges": [
            {
                "source": source,
                "target": target,
                **{
                    key: _json_safe_graph_value(value)
                    for key, value in attrs.items()
                },
            }
            for source, target, attrs in graph.edges(data=True)
        ],
    }


def _json_safe_graph_value(value: Any) -> Any:
    if hasattr(value, "to_dict"):
        return _json_safe_graph_value(value.to_dict())
    if isinstance(value, dict):
        return {str(key): _json_safe_graph_value(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_json_safe_graph_value(item) for item in value]
    if isinstance(value, str | int | float | bool) or value is None:
        return value
    return str(value)


def _lowered_graph_to_mermaid(
    graph: nx.DiGraph,
    *,
    execution_hooks: list[dict[str, Any]] | None = None,
    with_hooks: bool = False,
) -> str:
    lines = ["flowchart TD"]

    for node_id in graph.nodes:
        payload = graph.nodes[node_id]["payload"]
        label = f"{payload.id}<br/>{payload.role}<br/>{payload.impl}"
        lines.append(f'  {_mermaid_id(node_id)}["{_escape_mermaid(label)}"]')

    for upstream, downstream, edge_data in graph.edges(data=True):
        output_name = str(edge_data.get("output") or "stream")
        input_name = str(edge_data.get("input_name") or "stream")
        label = _escape_mermaid(output_name + " -> " + input_name)
        lines.append(
            f"  {_mermaid_id(upstream)} -->|{label}| {_mermaid_id(downstream)}"
        )

    if with_hooks and execution_hooks:
        lines.append("  subgraph Execution Hooks")
        for index, hook in enumerate(execution_hooks):
            kind = str(hook.get("kind") or "hook")
            events = list(hook.get("events") or [])
            event_label = ", ".join(str(event) for event in events) or "all"
            hook_id = f"hook_{index}_{_mermaid_id(kind)}"
            label = f"{event_label}: {kind}"
            lines.append(f'    {hook_id}["{_escape_mermaid(label)}"]')
        lines.append("  end")

    return "\n".join(lines) + "\n"


def _lowered_graph_to_dot(graph: nx.DiGraph) -> str:
    lines = ["digraph hepflow {"]
    for node_id in graph.nodes:
        payload = graph.nodes[node_id]["payload"]
        label = _dot_escape(f"{payload.id}\\n{payload.role}\\n{payload.impl}")
        lines.append(f'  "{node_id}" [label="{label}"];')

    for upstream, downstream, edge_data in graph.edges(data=True):
        output_name = str(edge_data.get("output") or "stream")
        lines.append(
            f'  "{upstream}" -> "{downstream}" [label="{_dot_escape(output_name)}"];'
        )

    lines.append("}")
    return "\n".join(lines) + "\n"


def _write_graph_png_if_available(graph: nx.DiGraph, path: Path) -> None:
    try:
        from networkx.drawing.nx_pydot import to_pydot  # noqa: PLC0415

        to_pydot(graph).write_png(str(path))
    except Exception:
        return


def _mermaid_id(node_id: str) -> str:
    return node_id.replace(".", "_").replace("-", "_")


def _escape_mermaid(value: str) -> str:
    return (
        str(value)
        .replace("\\", "\\\\")
        .replace('"', "&quot;")
        .replace("\n", "<br/>")
    )


def _dot_escape(value: str) -> str:
    return str(value).replace("\\", "\\\\").replace('"', '\\"')
