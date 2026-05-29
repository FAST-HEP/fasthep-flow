from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import networkx as nx

from hepflow.build_layout import render_dir, render_specs_dir
from hepflow.compiler.graph_artifacts import _lowered_graph_to_json
from hepflow.model.plan import ExecutionNode, ExecutionPlan
from hepflow.runtime.materialize import (
    cutflow_product_reference,
    histogram_product_reference,
)
from hepflow.utils import write_yaml


def write_compile_artifacts(
    *,
    plan: ExecutionPlan,
    graph: nx.DiGraph,
    outdir: str | Path,
) -> None:
    out_path = Path(outdir)
    compile_path = out_path / "compile"
    write_yaml(_lowered_graph_to_json(graph), str(compile_path / "analysis.ir.yaml"))
    write_yaml(plan.data_flow, str(compile_path / "deps.yaml"))
    (compile_path / "dataset_entries.json").write_text(
        json.dumps(plan.context.get("datasets") or {}, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    write_yaml(
        {
            "nodes": len(plan.nodes),
            "partitions": len(plan.partitions),
            "datasets": sorted((plan.context.get("datasets") or {}).keys()),
            "registry_sections": sorted(plan.registry.keys()),
        },
        str(compile_path / "report.compile.yaml"),
    )
    _write_render_artifacts(plan=plan, outdir=out_path)


def _write_render_artifacts(*, plan: ExecutionPlan, outdir: Path) -> None:
    specs_dir = render_specs_dir(outdir)
    render_specs: list[dict[str, Any]] = []
    for node in plan.nodes:
        if node.role != "sink":
            continue
        spec = dict(node.params.get("spec") or {})
        if not spec:
            continue
        item = {
            "node_id": node.id,
            "impl": node.impl,
            "out": node.params.get("out"),
            **_render_product_refs(plan=plan, node=node),
            "spec": spec,
        }
        render_specs.append(item)
        safe_node_id = node.id.replace(".", "_").replace("/", "_")
        write_yaml(item, str(specs_dir / f"{safe_node_id}.yaml"))

    (render_dir(outdir) / "report.render.json").write_text(
        json.dumps({"renders": render_specs}, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def _render_product_refs(
    *,
    plan: ExecutionPlan,
    node: ExecutionNode,
) -> dict[str, Any]:
    products: dict[str, dict[str, str]] = {}
    for ref in node.inputs:
        try:
            producer = plan.get_node(ref.node_id)
        except KeyError:
            continue
        name = ref.input_name if ref.input_name != "target" else "hist"
        output_kind = producer.outputs.get(ref.output_name)
        if output_kind == "histogram":
            products[name] = histogram_product_reference(producer.id, producer.meta)
            continue
        if output_kind == "cutflow":
            name = ref.input_name if ref.input_name != "target" else "cutflow"
            products[name] = cutflow_product_reference(producer.id, producer.meta)

    if not products:
        return {}
    if len(products) == 1:
        _, only_product = next(iter(products.items()))
        return {
            "product": dict(only_product),
            "products": {name: dict(product) for name, product in products.items()},
        }
    return {"products": products}
