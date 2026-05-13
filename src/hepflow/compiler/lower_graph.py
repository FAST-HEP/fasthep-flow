from __future__ import annotations

from copy import deepcopy
from typing import Any

import networkx as nx

from hepflow.compiler.styles import (
    collect_styles,
    deep_merge,
    resolve_style_ref,
    resolve_style_tree,
)
from hepflow.model.graph import GraphNode, add_graph_edge, add_graph_node, new_graph
from hepflow.model.lifecycle import normalize_lifecycle_event


def lower_author_to_graph(author: dict[str, Any]) -> nx.DiGraph:
    """
    Lower an author document into an explicit directed graph.

    Current scope:
      - sources -> reader nodes
      - source inspect -> observer nodes
      - analysis stages -> transform nodes
      - stage inspect -> observer nodes
      - stage write -> sink nodes
      - stage render -> sink nodes

    Current default chaining:
      - first stage consumes the first declared source
      - later stages consume the previous stage's stream output
      - attached inspect/write/render target the node they are attached to
    """
    graph = new_graph()
    graph.graph["analysis_globals"] = dict(
        (author.get("analysis") or {}).get("globals") or {}
    )

    data_block = dict(author.get("data", {}))
    datasets = list(data_block.get("datasets", []))
    defaults = dict(data_block.get("defaults", {}))

    style_defs = collect_styles(author)

    sources = dict(author.get("sources", {}))
    if not sources:
        raise ValueError("No sources declared in author document")

    joins = dict(author.get("joins", {}))

    stages = list(author.get("analysis", {}).get("stages", []))

    stream_entry_nodes: dict[str, str] = {}
    stream_effective_nodes: dict[str, str] = {}

    for source_name, source_cfg_raw in sources.items():
        source_cfg = dict(source_cfg_raw)
        branches = _branches_for_stream(author, source_name)
        if branches:
            source_cfg["branches"] = branches

        source_node = _make_source_node(
            source_name=source_name,
            source_cfg=source_cfg,
            datasets=datasets,
            defaults=defaults,
        )
        add_graph_node(graph, source_node)

        stream_entry_nodes[source_name] = source_node.id
        stream_effective_nodes[source_name] = source_node.id

        for idx, inspect_cfg in enumerate(_as_list(source_cfg.get("inspect"))):
            inspect_node = _make_inspect_node(
                node_id=f"inspect.{source_name}.{idx}",
                parent_kind="source",
                parent_name=source_name,
                inspect_cfg=inspect_cfg,
            )
            add_graph_node(graph, inspect_node)
            add_graph_edge(
                graph,
                source_node.id,
                inspect_node.id,
                output="stream",
                input_name="target",
            )

    _insert_projection_nodes(
        graph=graph,
        author=author,
        stream_entry_nodes=stream_entry_nodes,
        stream_effective_nodes=stream_effective_nodes,
        stream_ids=sources.keys(),
    )

    for join_name, join_cfg in joins.items():
        inputs_cfg = join_cfg["inputs"]

        join_node = GraphNode(
            id=f"join.{join_name}",
            role="transform",
            impl="hep.zip_join",
            params={
                "inputs": [
                    {
                        "name": inp["source"],
                        "prefix": inp["prefix"],
                    }
                    for inp in inputs_cfg
                ],
                "on_mismatch": join_cfg["on_mismatch"],
            },
            outputs={"stream": "event_stream"},
            meta={
                "join_name": join_name,
            },
        )

        add_graph_node(graph, join_node)

        for inp in inputs_cfg:
            upstream = stream_effective_nodes[inp["source"]]

            add_graph_edge(
                graph,
                upstream,
                join_node.id,
                output="stream",
                input_name=inp["source"],
            )

        stream_entry_nodes[join_name] = join_node.id
        stream_effective_nodes[join_name] = join_node.id

    _insert_projection_nodes(
        graph=graph,
        author=author,
        stream_entry_nodes=stream_entry_nodes,
        stream_effective_nodes=stream_effective_nodes,
        stream_ids=joins.keys(),
    )

    previous_stage_stream_source: str | None = None

    stage_nodes: dict[str, str] = {}

    for stage in stages:
        stage_id = stage["id"]
        op = stage["op"]

        if str(op).startswith("hep.render."):
            _lower_render_stage(
                graph=graph,
                stage=stage,
                style_defs=style_defs,
                stage_nodes=stage_nodes,
                stream_effective_nodes=stream_effective_nodes,
            )
            continue

        stage_node = _make_stage_node(stage)
        add_graph_node(graph, stage_node)
        stage_nodes[stage_id] = stage_node.id

        explicit_from = stage.get("from", stage.get("in"))
        if explicit_from is not None:
            upstream_stage_node = _resolve_stage_input_reference(
                reference=str(explicit_from),
                stage_nodes=stage_nodes,
                stream_effective_nodes=stream_effective_nodes,
            )
            add_graph_edge(
                graph,
                upstream_stage_node,
                stage_node.id,
                output="stream",
                input_name="stream",
            )
        else:
            if previous_stage_stream_source is None:
                previous_stage_stream_source = _resolve_initial_stage_stream(
                    author=author,
                    stream_effective_nodes=stream_effective_nodes,
                )
            add_graph_edge(
                graph,
                previous_stage_stream_source,
                stage_node.id,
                output="stream",
                input_name="stream",
            )

        if "stream" in stage_node.outputs:
            previous_stage_stream_source = stage_node.id

        for idx, inspect_cfg in enumerate(_as_list(stage.get("inspect"))):
            inspect_node = _make_inspect_node(
                node_id=f"inspect.{stage_id}.{idx}",
                parent_kind="stage",
                parent_name=stage_id,
                inspect_cfg=inspect_cfg,
            )
            add_graph_node(graph, inspect_node)

            output_name = _default_attachment_output(stage_node.outputs)
            add_graph_edge(
                graph,
                stage_node.id,
                inspect_node.id,
                output=output_name,
                input_name="target",
            )

        for idx, write_cfg in enumerate(_as_list(stage.get("write"))):
            write_node = _make_write_node(
                node_id=f"write.{stage_id}.{idx}",
                stage_id=stage_id,
                write_cfg=write_cfg,
            )
            add_graph_node(graph, write_node)

            upstream_node_id, output_name = _resolve_attachment_source(
                write_cfg=write_cfg,
                current_stage_id=stage_id,
                stage_nodes=stage_nodes,
                current_outputs=stage_node.outputs,
            )
            add_graph_edge(
                graph,
                upstream_node_id,
                write_node.id,
                output=output_name,
                input_name="target",
            )

        for idx, render_cfg in enumerate(_normalize_render(stage.get("render"))):
            render_node = _make_render_node(
                node_id=f"render.{stage_id}.{idx}",
                stage_id=stage_id,
                render_cfg=render_cfg,
                style_defs=style_defs,
                attached_stage_impl=stage_node.impl,
                attached_stage_params=stage_node.params,
            )
            add_graph_node(graph, render_node)

            upstream_node_id, output_name = _resolve_attachment_source(
                write_cfg=render_cfg,
                current_stage_id=stage_id,
                stage_nodes=stage_nodes,
                current_outputs=stage_node.outputs,
            )
            add_graph_edge(
                graph,
                upstream_node_id,
                render_node.id,
                output=output_name,
                input_name="target",
            )

    _attach_top_level_observers(
        graph=graph,
        observer_cfgs=list(author.get("observers") or []),
    )

    if not nx.is_directed_acyclic_graph(graph):
        raise ValueError("Lowered graph contains a cycle")

    return graph


def _make_source_node(
    *,
    source_name: str,
    source_cfg: dict[str, Any],
    datasets: list[dict[str, Any]],
    defaults: dict[str, Any],
) -> GraphNode:
    source_cfg = deepcopy(source_cfg)

    kind = source_cfg.pop("kind")
    source_cfg.pop("inspect", None)
    stream_type = source_cfg.get("stream_type", "event_stream")

    return GraphNode(
        id=f"read.{source_name}",
        role="source",
        impl=kind,
        params={
            "datasets": deepcopy(datasets),
            "defaults": deepcopy(defaults),
            **source_cfg,
        },
        outputs={"stream": stream_type},
        meta={
            "source_name": source_name,
            "author_kind": kind,
        },
    )


def _make_stage_node(stage: dict[str, Any]) -> GraphNode:
    stage = deepcopy(stage)

    stage_id = stage["id"]
    op = stage["op"]
    params = deepcopy(stage.get("params", {}))
    if op == "hep.hist":
        params = _canonicalize_hist_params(params)

    return GraphNode(
        id=f"stage.{stage_id}",
        role="transform",
        impl=op,
        params=params,
        outputs=_infer_stage_outputs(op),
        meta={
            "stage_id": stage_id,
            "author_op": op,
        },
    )


def _lower_render_stage(
    *,
    graph: nx.DiGraph,
    stage: dict[str, Any],
    style_defs: dict[str, dict[str, Any]],
    stage_nodes: dict[str, str],
    stream_effective_nodes: dict[str, str],
) -> GraphNode:
    stage_id = str(stage["id"])
    render_node = _make_render_stage_node(
        node_id=f"render.{stage_id}.0",
        stage=stage,
        style_defs=style_defs,
    )
    add_graph_node(graph, render_node)

    from_items = stage.get("from", stage.get("in"))
    if not isinstance(from_items, list):
        raise TypeError(
            f"Render stage '{stage_id}' field 'from' must be a list of input mappings"
        )

    for item in from_items:
        if not isinstance(item, dict):
            raise TypeError(
                f"Render stage '{stage_id}' input entries must be mappings, "
                f"got {type(item).__name__}"
            )
        if "node" not in item:
            raise ValueError(f"Render stage '{stage_id}' input is missing required 'node'")

        upstream = _resolve_stage_input_reference(
            reference=str(item["node"]),
            stage_nodes=stage_nodes,
            stream_effective_nodes=stream_effective_nodes,
        )
        add_graph_edge(
            graph,
            upstream,
            render_node.id,
            output=str(item.get("port", "hist")),
            input_name=str(item.get("as", "target")),
        )

    return render_node


def _make_render_stage_node(
    *,
    node_id: str,
    stage: dict[str, Any],
    style_defs: dict[str, dict[str, Any]],
) -> GraphNode:
    stage = deepcopy(stage)
    stage_id = str(stage["id"])
    op = str(stage["op"])
    render_params = dict(stage.get("params") or {})

    raw_style = render_params.pop("style", {})
    spec = resolve_style_ref(raw_style, style_defs)
    spec = resolve_style_tree(spec, style_defs)
    if "op" not in spec:
        spec["op"] = op
    spec = deep_merge(spec, render_params)

    params: dict[str, Any] = {
        "spec": spec,
        "when": normalize_lifecycle_event(stage.get("when", "final")),
    }
    if "out" in stage:
        params["out"] = stage["out"]

    return GraphNode(
        id=node_id,
        role="sink",
        impl=op,
        params=params,
        outputs={"artifact": "artifact"},
        meta={
            "stage_id": stage_id,
            "author_op": op,
        },
    )


def _expand_hist_dataset_axis(params: dict[str, Any]) -> dict[str, Any]:
    params = dict(params)
    dataset_axis = params.pop("dataset_axis", None)
    axes = list(params.get("axes") or [])

    if dataset_axis is True:
        axes = [
            {
                "name": "dataset",
                "type": "category",
                "source": "dataset_name",
                "bins": None,
            },
            *axes,
        ]
    elif isinstance(dataset_axis, dict):
        axes = [dataset_axis, *axes]
    elif dataset_axis in (None, False):
        pass
    else:
        raise TypeError("dataset_axis must be true, false, null, or an axis mapping")

    params["axes"] = axes
    return params


def _canonicalize_hist_params(params: dict[str, Any]) -> dict[str, Any]:
    params = dict(params)
    params = _expand_hist_dataset_axis(params)

    weight_expr = params.get("weight_expr")
    if weight_expr is not None:
        if "storage" not in params:
            params["storage"] = "weighted"
    elif "storage" not in params:
        params["storage"] = "count"

    return params


def _insert_projection_nodes(
    *,
    graph: nx.DiGraph,
    author: dict[str, Any],
    stream_entry_nodes: dict[str, str],
    stream_effective_nodes: dict[str, str],
    stream_ids,
) -> None:
    del stream_entry_nodes
    for stream_id in stream_ids:
        aliases = _aliases_for_stream(author, str(stream_id))
        if not aliases:
            continue
        if stream_id not in stream_effective_nodes:
            raise ValueError(f"fields reference unknown stream '{stream_id}'")

        project_node = GraphNode(
            id=f"project.{stream_id}",
            role="transform",
            impl="hep.project_fields",
            params={
                "stream_id": str(stream_id),
                "aliases": aliases,
            },
            outputs={"stream": "event_stream"},
            meta={
                "stream_id": str(stream_id),
                "inserted_by": "lower_graph",
            },
        )
        add_graph_node(graph, project_node)
        add_graph_edge(
            graph,
            stream_effective_nodes[str(stream_id)],
            project_node.id,
            output="stream",
            input_name="stream",
        )
        stream_effective_nodes[str(stream_id)] = project_node.id


def _resolve_initial_stage_stream(
    *,
    author: dict[str, Any],
    stream_effective_nodes: dict[str, str],
) -> str:
    primary_stream = author.get("primary_stream")
    if primary_stream is not None:
        try:
            return stream_effective_nodes[str(primary_stream)]
        except KeyError as exc:
            raise ValueError(
                f"primary_stream references unknown stream '{primary_stream}'"
            ) from exc

    if len(stream_effective_nodes) == 1:
        return next(iter(stream_effective_nodes.values()))

    raise ValueError("Multiple input streams available; set primary_stream or define a join.")


def _resolve_stage_input_reference(
    *,
    reference: str,
    stage_nodes: dict[str, str],
    stream_effective_nodes: dict[str, str],
) -> str:
    if reference in stage_nodes:
        return stage_nodes[reference]
    if reference in stream_effective_nodes:
        return stream_effective_nodes[reference]
    raise ValueError(f"Unknown stage input reference '{reference}'")


def _attach_top_level_observers(
    *,
    graph: nx.DiGraph,
    observer_cfgs: list[dict[str, Any]],
) -> None:
    for observer_index, observer in enumerate(observer_cfgs):
        kind = str(observer["kind"])
        at_list = list(observer.get("at") or [])

        for target_node_id in at_list:
            if target_node_id not in graph.nodes:
                raise ValueError(
                    f"Top-level observer {kind!r} references unknown node '{target_node_id}'"
                )

            target_payload = graph.nodes[target_node_id]["payload"]
            output_name = _preferred_observer_output(target_payload.outputs)
            safe_kind = _safe_graph_observer_name(kind)
            safe_target = _safe_graph_observer_name(target_node_id)
            observer_node_id = (
                f"observe.{safe_kind}.{observer_index}.{safe_target}"
            )

            params = {
                key: value
                for key, value in dict(observer).items()
                if key not in {"kind", "at"}
            }
            params["node_id"] = str(target_node_id)

            observer_node = GraphNode(
                id=observer_node_id,
                role="observer",
                impl=kind,
                params=params,
                outputs={"report": "report"},
                meta={
                    "observed_node": str(target_node_id),
                    "inserted_by": "lower_graph",
                },
            )
            add_graph_node(graph, observer_node)
            add_graph_edge(
                graph,
                str(target_node_id),
                observer_node_id,
                output=output_name,
                input_name="target",
            )


def _preferred_observer_output(outputs: dict[str, str]) -> str:
    if "stream" in outputs:
        return "stream"
    if "hist" in outputs:
        return "hist"
    return next(iter(outputs))


def _safe_graph_observer_name(value: str) -> str:
    return str(value).replace(".", "_").replace("/", "_")


def _infer_render_axes_from_hist(
    render_spec: dict[str, Any],
    hist_params: dict[str, Any],
) -> dict[str, Any]:
    spec = deepcopy(render_spec)
    axes = dict(spec.get("axes") or {})
    hist_axes = list(hist_params.get("axes") or [])

    plot_axes = [
        ax
        for ax in hist_axes
        if ax.get("source") != "dataset_name" and ax.get("name") != "dataset"
    ]

    if plot_axes and "x" not in axes:
        axes["x"] = _render_axis_from_hist_axis(plot_axes[0])

    if len(plot_axes) >= 2 and "y" not in axes:
        axes["y"] = _render_axis_from_hist_axis(plot_axes[1])
    elif "y" not in axes:
        axes["y"] = {"name": "events", "label": "Events"}

    if len(plot_axes) >= 2 and "z" not in axes:
        axes["z"] = {"name": "events", "label": "Events"}

    spec["axes"] = axes
    return spec


def _render_axis_from_hist_axis(axis: dict[str, Any]) -> dict[str, Any]:
    out = {
        "name": axis["name"],
        "label": axis.get("label") or axis["name"],
    }
    bins = axis.get("bins")
    if isinstance(bins, dict) and "low" in bins and "high" in bins:
        out["limits"] = [bins["low"], bins["high"]]
    return out


def _make_inspect_node(
    *,
    node_id: str,
    parent_kind: str,
    parent_name: str,
    inspect_cfg: dict[str, Any],
) -> GraphNode:
    inspect_cfg = deepcopy(inspect_cfg)

    kind = inspect_cfg.pop("kind")
    return GraphNode(
        id=node_id,
        role="observer",
        impl=kind,
        params=inspect_cfg,
        outputs={"report": "report"},
        meta={
            "parent_kind": parent_kind,
            "parent_name": parent_name,
            "author_kind": kind,
        },
    )


def _make_write_node(
    *,
    node_id: str,
    stage_id: str,
    write_cfg: dict[str, Any],
) -> GraphNode:
    write_cfg = deepcopy(write_cfg)

    kind = write_cfg.pop("kind")
    write_cfg.pop("from", None)
    write_cfg["when"] = normalize_lifecycle_event(write_cfg.get("when", "partition"))

    return GraphNode(
        id=node_id,
        role="sink",
        impl=kind,
        params=write_cfg,
        outputs={"artifact": "artifact"},
        meta={
            "stage_id": stage_id,
            "author_kind": kind,
        },
    )


def _make_render_node(
    *,
    node_id: str,
    stage_id: str,
    render_cfg: dict[str, Any],
    style_defs: dict[str, dict[str, Any]],
    attached_stage_impl: str | None = None,
    attached_stage_params: dict[str, Any] | None = None,
) -> GraphNode:
    render_cfg = deepcopy(render_cfg)

    if "style" not in render_cfg:
        raise ValueError(
            f"Render entry on stage '{stage_id}' is missing required field 'style'"
        )

    style_ref = render_cfg.pop("style")
    render_cfg.pop("from", None)
    render_cfg["when"] = normalize_lifecycle_event(render_cfg.get("when", "final"))

    resolved_style = resolve_style_ref(style_ref, style_defs)
    resolved_style = resolve_style_tree(resolved_style, style_defs)
    if attached_stage_impl == "hep.hist":
        resolved_style = _infer_render_axes_from_hist(
            resolved_style,
            dict(attached_stage_params or {}),
        )

    render_op = resolved_style.get("op")
    if not isinstance(render_op, str) or not render_op.strip():
        raise ValueError(
            f"Resolved render style for stage '{stage_id}' is missing a valid 'op'"
        )

    return GraphNode(
        id=node_id,
        role="sink",
        impl=render_op,
        params={
            "spec": resolved_style,
            **render_cfg,
        },
        outputs={"artifact": "artifact"},
        meta={
            "stage_id": stage_id,
            "style_ref": style_ref,
            "author_op": render_op,
        },
    )


def _resolve_attachment_source(
    *,
    write_cfg: dict[str, Any],
    current_stage_id: str,
    stage_nodes: dict[str, str],
    current_outputs: dict[str, str],
) -> tuple[str, str]:
    explicit_from = write_cfg.get("from")
    if explicit_from is not None:
        return stage_nodes[explicit_from], "stream"

    return stage_nodes[current_stage_id], _default_attachment_output(current_outputs)


def _default_attachment_output(outputs: dict[str, str]) -> str:
    if "stream" in outputs:
        return "stream"
    if "hist" in outputs:
        return "hist"

    try:
        return next(iter(outputs))
    except StopIteration as exc:
        raise ValueError("Node has no outputs to attach to") from exc


def _infer_stage_outputs(op: str) -> dict[str, str]:
    """
    Minimal output inference for current examples.

    Later this should come from registry metadata.
    """
    if op == "hep.hist":
        return {"hist": "histogram"}

    if op == "hep.selection.cutflow":
        return {
            "stream": "event_stream",
            "cutflow": "report",
        }

    return {"stream": "event_stream"}


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _normalize_render(render: Any) -> list[dict[str, Any]]:
    if render is None:
        return []
    if isinstance(render, list):
        return [dict(item) for item in render]
    if isinstance(render, dict):
        return [dict(render)]
    raise TypeError(f"Unsupported render block type: {type(render)!r}")


def _aliases_for_stream(author: dict[str, Any], stream_id: str) -> dict[str, str]:
    fields = dict(author.get("fields", {}) or {})
    aliases: dict[str, str] = {}

    for alias, spec in fields.items():
        if not isinstance(spec, dict):
            continue
        if spec.get("stream") != stream_id:
            continue

        branch = spec.get("branch")
        if isinstance(branch, str) and branch:
            aliases[str(alias)] = branch

    return aliases


def _branches_for_stream(author: dict[str, Any], stream_id: str) -> list[str]:
    fields = dict(author.get("fields", {}) or {})
    branches: list[str] = []

    for _, spec in fields.items():
        if not isinstance(spec, dict):
            continue
        if spec.get("stream") != stream_id:
            continue

        branch = spec.get("branch")
        if isinstance(branch, str) and branch:
            branches.append(branch)

    # Stable unique order
    seen: set[str] = set()
    out: list[str] = []
    for b in branches:
        if b in seen:
            continue
        seen.add(b)
        out.append(b)
    return out
