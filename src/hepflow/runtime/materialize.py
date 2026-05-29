from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from hepflow.model.plan import ExecutionNode, ExecutionPlan
from hepflow.registry.loaders import runtime_registry_from_config
from hepflow.registry.runtime import RuntimeRegistry


def materialize_final_products(
    plan: ExecutionPlan,
    *,
    value_store: dict[tuple[str, str], Any],
    outdir: str | Path,
    registry_cfg: dict[str, Any] | None = None,
    runtime_registry: RuntimeRegistry | None = None,
) -> list[dict[str, str]]:
    runtime_registry = runtime_registry or runtime_registry_from_config(
        registry_cfg or plan.registry
    )

    items: list[dict[str, str]] = []
    for node in plan.nodes:
        for output_name, product_kind in node.outputs.items():
            key = (node.id, output_name)
            if key not in value_store:
                continue
            handler = runtime_registry.product_handlers.get(product_kind)
            if handler is None or handler.materialize is None:
                continue

            result = handler.materialize(
                value_store[key],
                node=node,
                output_name=output_name,
                outdir=outdir,
            )
            value_store[key] = result.get("value", value_store[key])
            items.extend(_manifest_items(result.get("items")))

    return items


def write_product_manifest(
    output_dir: Path,
    key: str,
    items: list[dict[str, str]],
) -> None:
    manifest = {key: sorted(items, key=lambda item: item["id"])}
    (output_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def product_id(node: ExecutionNode) -> str:
    return _product_id(node.id, node.meta)


def _product_id(node_id: str, meta: dict[str, Any]) -> str:
    stage_id = meta.get("stage_id")
    if isinstance(stage_id, str) and stage_id:
        return _safe_filename(stage_id)
    return _safe_filename(node_id.removeprefix("stage."))


def _safe_filename(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("._")
    return cleaned or "product"


def _manifest_items(value: Any) -> list[dict[str, str]]:
    if isinstance(value, list):
        return [item for item in value if _is_manifest_item(item)]
    if _is_manifest_item(value):
        return [value]
    return []


def _is_manifest_item(value: Any) -> bool:
    return (
        isinstance(value, dict)
        and isinstance(value.get("id"), str)
        and isinstance(value.get("path"), str)
        and isinstance(value.get("producer"), str)
    )
