from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from hepflow.build_layout import artifact_family_dir
from hepflow.model.plan import ExecutionPlan
from hepflow.utils import write_json, write_pickle


def materialize_final_histograms(
    plan: ExecutionPlan,
    *,
    value_store: dict[tuple[str, str], Any],
    outdir: str | Path,
) -> list[dict[str, str]]:
    # TODO: generalize this behind an explicit materialization policy
    # (never/final/partition/all) before adding partition-level products.
    histograms_dir = artifact_family_dir(outdir, "histograms")
    histograms_dir.mkdir(parents=True, exist_ok=True)

    items: list[dict[str, str]] = []
    for node in plan.nodes:
        for output_name, output_kind in node.outputs.items():
            if output_kind != "histogram":
                continue
            key = (node.id, output_name)
            if key not in value_store:
                continue

            histogram_id = _product_id(node.id, node.meta)
            relative_path = Path("artifacts") / "histograms" / f"{histogram_id}.pkl"
            write_pickle(value_store[key], Path(outdir) / relative_path)
            items.append(
                {
                    "id": histogram_id,
                    "path": relative_path.as_posix(),
                    "producer": node.id,
                }
            )

    _write_histogram_manifest(histograms_dir, items)
    return items


def histogram_product_reference(producer_id: str, meta: dict[str, Any]) -> dict[str, str]:
    histogram_id = _product_id(producer_id, meta)
    return {
        "kind": "histogram",
        "path": f"artifacts/histograms/{histogram_id}.pkl",
    }


def materialize_final_cutflows(
    plan: ExecutionPlan,
    *,
    value_store: dict[tuple[str, str], Any],
    outdir: str | Path,
) -> list[dict[str, str]]:
    # TODO: generalize this behind an explicit materialization policy
    # (never/final/partition/all) before adding partition-level products.
    cutflows_dir = artifact_family_dir(outdir, "cutflows")
    cutflows_dir.mkdir(parents=True, exist_ok=True)

    items: list[dict[str, str]] = []
    for node in plan.nodes:
        for output_name, output_kind in node.outputs.items():
            if output_kind != "cutflow":
                continue
            key = (node.id, output_name)
            if key not in value_store:
                continue

            cutflow_id = _product_id(node.id, node.meta)
            relative_path = Path("artifacts") / "cutflows" / f"{cutflow_id}.json"
            graph = _canonical_cutflow_graph(
                producer_id=node.id,
                params=node.params,
                product=value_store[key],
            )
            value_store[key] = graph
            write_json(graph, Path(outdir) / relative_path)
            items.append(
                {
                    "id": cutflow_id,
                    "path": relative_path.as_posix(),
                    "producer": node.id,
                }
            )

    _write_manifest(cutflows_dir, "cutflows", items)
    return items


def cutflow_product_reference(producer_id: str, meta: dict[str, Any]) -> dict[str, str]:
    cutflow_id = _product_id(producer_id, meta)
    return {
        "kind": "cutflow",
        "path": f"artifacts/cutflows/{cutflow_id}.json",
    }


def _write_histogram_manifest(
    histograms_dir: Path,
    items: list[dict[str, str]],
) -> None:
    _write_manifest(histograms_dir, "histograms", items)


def _write_manifest(
    output_dir: Path,
    key: str,
    items: list[dict[str, str]],
) -> None:
    manifest = {key: sorted(items, key=lambda item: item["id"])}
    (output_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _product_id(node_id: str, meta: dict[str, Any]) -> str:
    stage_id = meta.get("stage_id")
    if isinstance(stage_id, str) and stage_id:
        return _safe_filename(stage_id)
    return _safe_filename(node_id.removeprefix("stage."))


def _safe_filename(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("._")
    return cleaned or "histogram"


def _canonical_cutflow_graph(
    *,
    producer_id: str,
    params: dict[str, Any],
    product: Any,
) -> dict[str, Any]:
    if isinstance(product, dict) and product.get("kind") == "cutflow":
        safe_product = _json_safe(product)
        if isinstance(safe_product, dict):
            return safe_product

    datasets = _cutflow_datasets(product)
    stats = _cutflow_stats_by_dataset(product)
    graph: dict[str, Any] = {
        "version": "1.0",
        "kind": "cutflow",
        "producer": producer_id,
        "datasets": datasets,
        "nodes": [],
        "edges": [],
    }

    for selection_name, steps, parents in _selection_groups(params.get("selection")):
        previous: str | None = None
        for index, step in enumerate(steps):
            node_id = f"{selection_name}[{index}]"
            node_parents = [previous] if previous is not None else parents
            node = {
                "id": node_id,
                "selection": selection_name,
                "index": index,
                "label": _cut_label(step),
                "expr": _json_safe(_cut_expr(step)),
                "kind": _cut_kind(step),
                "parents": node_parents,
                "stats": {
                    dataset: _selection_stats(
                        stats.get(dataset, {}).get(node_id, {})
                    )
                    for dataset in datasets
                },
            }
            graph["nodes"].append(node)

            if previous is not None:
                graph["edges"].append(
                    {"source": previous, "target": node_id, "kind": "sequence"}
                )
            else:
                graph["edges"].extend(
                    {"source": parent, "target": node_id, "kind": "branch"}
                    for parent in parents
                )
            previous = node_id

    if not graph["nodes"]:
        graph["nodes"] = _fallback_cutflow_nodes(stats, datasets)

    return graph


def _selection_groups(selection: Any) -> list[tuple[str, list[Any], list[str]]]:
    if not isinstance(selection, dict):
        return []

    groups: list[tuple[str, list[Any], list[str]]] = []
    for name, raw in selection.items():
        if isinstance(raw, list):
            groups.append((str(name), raw, []))
            continue
        if not isinstance(raw, dict):
            continue

        steps = raw.get("steps", raw.get("cuts", []))
        if not isinstance(steps, list):
            continue
        parent_value = raw.get("parents", raw.get("from", raw.get("parent", [])))
        groups.append((str(name), steps, _parent_ids(parent_value)))
    return groups


def _parent_ids(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value] if value else []
    if isinstance(value, list | tuple):
        return [str(item) for item in value if str(item)]
    return [str(value)]


def _cut_label(step: Any) -> str:
    if isinstance(step, str):
        return step
    if isinstance(step, dict):
        if isinstance(step.get("label"), str):
            return step["label"]
        if "expr" in step:
            return str(step["expr"])
        if isinstance(step.get("reduce"), dict):
            reduce_spec = step["reduce"]
            op = reduce_spec.get("op", "reduce")
            over = reduce_spec.get("over", "")
            return f"{op}({over})"
    return str(step)


def _cut_expr(step: Any) -> Any:
    if isinstance(step, str):
        return step
    if isinstance(step, dict):
        if "expr" in step:
            return step["expr"]
        if "reduce" in step:
            return {"reduce": step["reduce"]}
    return step


def _cut_kind(step: Any) -> str:
    if isinstance(step, dict) and "reduce" in step:
        return "reduce"
    return "expression"


def _cutflow_datasets(product: Any) -> list[str]:
    stats = _cutflow_stats_by_dataset(product)
    return sorted(stats) if stats else ["default"]


def _cutflow_stats_by_dataset(product: Any) -> dict[str, dict[str, dict[str, Any]]]:
    if not isinstance(product, dict):
        return {}

    if isinstance(product.get("cutflows"), list):
        items = product["cutflows"]
    else:
        items = [product]

    by_dataset: dict[str, dict[str, dict[str, Any]]] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        dataset = str(item.get("dataset") or "default")
        cuts = item.get("cuts", [])
        if not isinstance(cuts, list):
            continue
        dataset_stats = by_dataset.setdefault(dataset, {})
        for row in cuts:
            if not isinstance(row, dict):
                continue
            name = str(row.get("name") or row.get("id") or "")
            if name:
                dataset_stats[name] = row
    return by_dataset


def _selection_stats(row: dict[str, Any]) -> dict[str, Any]:
    if row:
        n_out = row.get("n_out", row.get("n", 0))
        n_in = row.get("n_in", n_out)
        sumw_out = row.get("sumw_out", row.get("sumw", n_out))
        sumw_in = row.get("sumw_in", row.get("sumw", n_in))
        sumw2_out = row.get("sumw2_out", row.get("sumw2", n_out))
        sumw2_in = row.get("sumw2_in", row.get("sumw2", n_in))
    else:
        n_in = n_out = 0
        sumw_in = sumw_out = sumw2_in = sumw2_out = 0.0

    return {
        "n_in": int(n_in),
        "n_out": int(n_out),
        "sumw_in": float(sumw_in),
        "sumw_out": float(sumw_out),
        "sumw2_in": float(sumw2_in),
        "sumw2_out": float(sumw2_out),
    }


def _fallback_cutflow_nodes(
    stats: dict[str, dict[str, dict[str, Any]]],
    datasets: list[str],
) -> list[dict[str, Any]]:
    node_ids = sorted({node_id for rows in stats.values() for node_id in rows})
    nodes: list[dict[str, Any]] = []
    for node_id in node_ids:
        nodes.append(
            {
                "id": node_id,
                "selection": node_id.split("[", 1)[0],
                "index": _node_index(node_id),
                "label": node_id,
                "expr": node_id,
                "kind": "expression",
                "parents": [],
                "stats": {
                    dataset: _selection_stats(
                        stats.get(dataset, {}).get(node_id, {})
                    )
                    for dataset in datasets
                },
            }
        )
    return nodes


def _node_index(node_id: str) -> int:
    match = re.search(r"\[(\d+)\]$", node_id)
    return int(match.group(1)) if match else 0


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_json_safe(item) for item in value]
    if isinstance(value, str | int | float | bool) or value is None:
        return value
    if hasattr(value, "item"):
        return _json_safe(value.item())
    return str(value)
