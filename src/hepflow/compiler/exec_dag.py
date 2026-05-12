# hepflow/compiler/exec_dag.py
from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any
import networkx as nx

from hepflow.model.issues import FlowIssue, IssueLevel


@dataclass(frozen=True)
class DataflowAnalysis:
    edge_syms: dict[tuple[str, str, str], list[str]]
    issues: list[FlowIssue]
    provided_by: dict[str, str]
    requires_by_node: dict[str, list[str]]
    provides_by_node: dict[str, list[str]]


@dataclass(frozen=True)
class ExecDag:
    """
    Thin wrapper around a networkx DiGraph representing plan.exec_graph.

    Nodes are plan exec node ids.
    Edges represent node->node dependencies (InputRef with node+port).
    """

    g: nx.DiGraph

    @staticmethod
    def from_plan(plan: dict[str, Any]) -> "ExecDag":
        eg = plan.get("exec_graph") or []
        if not isinstance(eg, list):
            raise TypeError("plan.exec_graph must be a list")

        g = nx.DiGraph()

        # add nodes
        for n in eg:
            nid = str(n.get("id"))
            if not nid:
                raise ValueError("exec_graph node missing 'id'")
            g.add_node(
                nid,
                op=str(n.get("op", "")),
                params=n.get("params") or {},
            )

        # add edges (node inputs only)
        for n in eg:
            nid = str(n["id"])
            in_refs = n.get("in") or []
            if not isinstance(in_refs, list):
                raise TypeError(f"exec node '{nid}' field 'in' must be a list")
            for ref in in_refs:
                if not isinstance(ref, dict):
                    raise TypeError(f"exec node '{nid}' input ref must be dict")
                src = ref.get("node")
                port = ref.get("port")
                if src and port:
                    src = str(src)
                    if src not in g:
                        raise ValueError(
                            f"exec node '{nid}' references unknown upstream node '{src}'"
                        )
                    g.add_edge(src, nid, port=str(port))
        return ExecDag(g)

    def is_dag(self) -> bool:
        return nx.is_directed_acyclic_graph(self.g)

    def topo_order(self) -> list[str]:
        return list(nx.topological_sort(self.g))

    def ancestors(self, node_id: str) -> set[str]:
        return set(nx.ancestors(self.g, node_id))

    def descendants(self, node_id: str) -> set[str]:
        return set(nx.descendants(self.g, node_id))

    def analyze_dataflow(self, plan: dict[str, Any]) -> DataflowAnalysis:
        """
        Build a dataflow view using ONLY plan.yaml content:
          - exec_graph[].deps.requires/provides
          - fieldmap
          - streams + primary_stream

        Produces:
          - edge_syms: (src, dst, kind={"real","suggest"}) -> [symbols...]
          - issues: e.g. REQUIRES_NOT_IN_PATH (impossible requires)
        """
        primary_stream = str(plan.get("primary_stream") or "events")
        fieldmap = plan.get("fieldmap") or {}
        streams = plan.get("streams") or {}

        exec_graph = plan.get("exec_graph") or []
        if not isinstance(exec_graph, list):
            raise TypeError("plan.exec_graph must be a list")

        node_ids: set[str] = set()
        node_op: dict[str, str] = {}
        requires_by_node: dict[str, list[str]] = {}
        provides_by_node: dict[str, list[str]] = {}

        for n in exec_graph:
            nid = str(n.get("id") or "")
            if not nid:
                raise ValueError("exec_graph node missing id")
            node_ids.add(nid)
            node_op[nid] = str(n.get("op") or "")

            deps = n.get("deps") or {}
            reqs = [str(x) for x in (deps.get("requires") or [])]
            provs = [str(x) for x in (deps.get("provides") or [])]
            requires_by_node[nid] = reqs
            provides_by_node[nid] = provs

        # stage-provided symbols: first-wins or last-wins?
        # We use first-wins to be deterministic in topo order, but you can
        # swap to last-wins if you prefer current behaviour.
        provided_by: dict[str, str] = {}

        # Use topo order for stable "first provider wins" resolution
        topo = self.topo_order()
        for nid in topo:
            for sym in provides_by_node.get(nid, []):
                if sym not in provided_by:
                    provided_by[sym] = nid

        def stream_node_id(sid: str) -> str:
            return f"S__{sid}"

        def provider_stream_for_symbol(sym: str) -> str:
            fm = fieldmap.get(sym)
            if isinstance(fm, dict):
                s = fm.get("stream")
                if isinstance(s, str) and s:
                    return s
            return primary_stream

        def _dedupe_preserve(items: Iterable[str]) -> list[str]:
            seen: set[str] = set()
            out: list[str] = []
            for x in items:
                if x not in seen:
                    seen.add(x)
                    out.append(x)
            return out

        edge_syms: dict[tuple[str, str, str], list[str]] = defaultdict(list)
        issues: list[FlowIssue] = []

        # Cache ancestors for speed when plans grow
        ancestors_cache: dict[str, set[str]] = {}

        def ancestors_of(nid: str) -> set[str]:
            if nid not in ancestors_cache:
                ancestors_cache[nid] = self.ancestors(nid)
            return ancestors_cache[nid]

        for nid in topo:
            reqs = requires_by_node.get(nid, [])

            for sym in reqs:
                # If there's an internal provider stage for this symbol
                prov_stage = provided_by.get(sym)

                if prov_stage is not None:
                    if prov_stage == nid:
                        # Self-provided + required in same node:
                        # keep as a suggestion edge for visibility (harmless).
                        edge_syms[(prov_stage, nid, "suggest")].append(sym)
                        issues.append(
                            FlowIssue(
                                level=IssueLevel.INFO,
                                code="REQUIRES_SELF_PROVIDED",
                                message=f"Stage '{nid}' both requires and provides symbol '{sym}'.",
                                meta={
                                    "node": nid,
                                    "symbol": sym,
                                    "op": node_op.get(nid, ""),
                                },
                            )
                        )
                        continue

                    # Check whether provider is actually on the execution path (ancestor)
                    if prov_stage in ancestors_of(nid):
                        edge_syms[(prov_stage, nid, "real")].append(sym)
                    else:
                        # Not in path => impossible requirement given current exec dependencies.
                        # Add a suggested dashed edge and emit a warning.
                        edge_syms[(prov_stage, nid, "suggest")].append(sym)
                        issues.append(
                            FlowIssue(
                                level=IssueLevel.ERROR,
                                code="REQUIRES_NOT_IN_PATH",
                                message=(
                                    f"Stage '{nid}' requires symbol '{sym}', provided by '{prov_stage}', "
                                    "but the provider is not an ancestor in exec_graph. "
                                    "This will fail at runtime unless you add the missing dependency."
                                ),
                                meta={
                                    "node": nid,
                                    "op": node_op.get(nid, ""),
                                    "symbol": sym,
                                    "provider": prov_stage,
                                    "provider_op": node_op.get(prov_stage, ""),
                                    "suggestion": {
                                        "add_in": {
                                            "node": prov_stage,
                                            "port": "events",
                                        },
                                    },
                                },
                            )
                        )
                    continue

                # Otherwise treat as external symbol: map to a stream via fieldmap or primary_stream
                sid = provider_stream_for_symbol(sym)
                if sid not in streams:
                    issues.append(
                        FlowIssue(
                            level=IssueLevel.WARN,
                            code="REQUIRES_UNKNOWN_STREAM",
                            message=(
                                f"Stage '{nid}' requires external symbol '{sym}', which maps to stream '{sid}', "
                                "but that stream is not present in plan.streams."
                            ),
                            meta={
                                "node": nid,
                                "op": node_op.get(nid, ""),
                                "symbol": sym,
                                "stream": sid,
                                "primary_stream": primary_stream,
                            },
                        )
                    )
                    # Still emit edge to a virtual stream node id so the diagram shows it
                    edge_syms[(stream_node_id(sid), nid, "suggest")].append(sym)
                else:
                    edge_syms[(stream_node_id(sid), nid, "real")].append(sym)

        # normalize labels (dedupe)
        edge_syms = {k: _dedupe_preserve(v) for k, v in edge_syms.items()}

        return DataflowAnalysis(
            edge_syms=edge_syms,
            issues=issues,
            provided_by=provided_by,
            requires_by_node=requires_by_node,
            provides_by_node=provides_by_node,
        )

    def to_mermaid(self, *, wrapping_width: int = 500) -> str:
        """
        Mermaid flowchart (top-down).

        - default node label: <id><br /><op>
        - for hep.selection.cutflow, include a short selection summary
        """
        lines: list[str] = [
            "---",
            "config:",
            "  flowchart:",
            f"    wrappingWidth: {int(wrapping_width)}",
            "---",
            "flowchart TD",
        ]

        for nid, attrs in self.g.nodes(data=True):
            op = attrs.get("op") or ""
            params = attrs.get("params") or {}

            label_parts = [str(nid)]
            if op:
                label_parts.append(str(op))

            if op == "hep.selection.cutflow":
                label_parts.extend(
                    _cutflow_lines_for_mermaid(
                        params,
                        max_lines=6,
                        max_width=120,
                    )
                )

            label = "<br />".join(label_parts)
            lines.append(f'  {nid}["{_escape_mermaid(label)}"]')

        for a, b, attrs in self.g.edges(data=True):
            port = attrs.get("port")
            if port:
                lines.append(f"  {a} -->|{_escape_mermaid(str(port))}| {b}")
            else:
                lines.append(f"  {a} --> {b}")

        return "\n".join(lines)

    def to_mermaid_dataflow(self, plan: dict[str, Any]) -> str:
        """
        Mermaid flowchart TD showing symbol/data dependencies.
        Nodes: streams + stages.
        Edges:
          - real: normal arrow
          - suggest: dashed red arrow
        Labels: symbols aggregated.
        """
        analysis = self.analyze_dataflow(plan)

        lines: list[str] = ["flowchart TD"]

        streams = plan.get("streams") or {}
        exec_graph = plan.get("exec_graph") or []
        node_op: dict[str, str] = {
            str(n.get("id")): str(n.get("op") or "") for n in exec_graph
        }

        def stream_node_id(sid: str) -> str:
            return f"S__{sid}"

        # stream nodes
        for sid in sorted(streams.keys()):
            sn = stream_node_id(sid)
            skind = str((streams.get(sid) or {}).get("kind", "stream"))
            lines.append(
                f'  {sn}["{_escape_mermaid(sid)}<br />{_escape_mermaid(skind)}"]:::stream'
            )

        # stage nodes
        for nid, op in node_op.items():
            label = f"{nid}<br />{op}" if op else nid
            lines.append(f'  {nid}["{_escape_mermaid(label)}"]:::stage')

        # edges
        for (src, dst, kind), syms in analysis.edge_syms.items():
            label = _chunk_labels(syms, max_items=6)
            if kind == "suggest":
                lines.append(
                    f"  {src} -.->|{_escape_mermaid(label)}| {dst}:::suggest_edge"
                )
            else:
                lines.append(f"  {src} -->|{_escape_mermaid(label)}| {dst}")

        lines.append("")
        lines.append("classDef stream fill:#eef,stroke:#88a,stroke-width:1px;")
        lines.append("classDef stage fill:#efe,stroke:#8a8,stroke-width:1px;")
        # Mermaid doesn't let you style edges directly via classDef,
        # but you *can* style the destination node; we also add a CSS-ish
        # trick by tagging a pseudo-class on the arrow line, which most
        # Mermaid renderers accept (and harmless if ignored).
        lines.append(
            "classDef suggest_edge stroke:#c33,stroke-width:2px,stroke-dasharray: 5 5;"
        )

        return "\n".join(lines)


def _escape_mermaid(s: str) -> str:
    # minimal escaping for quotes/newlines
    return s.replace('"', '\\"').replace("\n", "\\n")


def _chunk_labels(items: list[str], *, max_items: int = 6) -> str:
    """Make edge labels readable."""
    if len(items) <= max_items:
        return ", ".join(items)
    head = ", ".join(items[:max_items])
    return f"{head}, +{len(items) - max_items} more"


def _format_cut_expr(cut: Any) -> str:
    """
    Make common structured cut expressions more readable in Mermaid labels.
    """
    if isinstance(cut, dict):
        red = cut.get("reduce")
        if isinstance(red, dict):
            op = red.get("op")
            over = red.get("over")
            if op and over:
                return f"{op}({over})"
    return str(cut)


def _short_mermaid_line(s: str, *, max_width: int = 90) -> str:
    s = str(s).replace("\n", " ").strip()
    if len(s) > max_width:
        return s[: max_width - 3] + "..."
    return s


def _cutflow_lines_for_mermaid(
    params: dict[str, Any] | None,
    *,
    max_lines: int = 6,
    max_width: int = 90,
) -> list[str]:
    """
    Render hep.selection.cutflow params.selection into short label lines.

    Conventions:
    - All: one line per cut
    - Any: one compact line: 'ANY: cut1 OR cut2 OR ...'
    - other block names: '<block>: cut1; cut2; ...'
    """
    if not isinstance(params, dict):
        return []

    selection = params.get("selection") or {}
    if not isinstance(selection, dict):
        return []

    out: list[str] = []

    for block_name, cuts in selection.items():
        if not isinstance(cuts, list):
            continue

        block = str(block_name).strip().lower()

        if block == "all":
            for cut in cuts:
                out.append(
                    _short_mermaid_line(_format_cut_expr(cut), max_width=max_width)
                )
                if len(out) >= max_lines:
                    return out

        elif block == "any":
            joined = " OR ".join(
                _short_mermaid_line(_format_cut_expr(c), max_width=max_width // 2)
                for c in cuts
            )
            out.append(_short_mermaid_line(f"ANY: {joined}", max_width=max_width))
            if len(out) >= max_lines:
                return out

        else:
            joined = "; ".join(
                _short_mermaid_line(_format_cut_expr(c), max_width=max_width // 2)
                for c in cuts
            )
            out.append(
                _short_mermaid_line(f"{block_name}: {joined}", max_width=max_width)
            )
            if len(out) >= max_lines:
                return out

    return out
