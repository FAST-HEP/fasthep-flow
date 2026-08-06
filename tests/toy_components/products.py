from __future__ import annotations

from pathlib import Path
from typing import Any

from hepflow.utils import write_json


def merge_toy_products(
    values: list[dict[str, Any]],
    *,
    node: Any,
    output_name: str,
    dataset_name: str | None = None,
) -> dict[str, Any] | list[dict[str, Any]]:
    del node, output_name, dataset_name
    if len(values) == 1:
        return values[0]
    return list(values)


def materialize_toy_product(
    value: dict[str, Any],
    *,
    node: Any,
    output_name: str,
    outdir: str | Path,
) -> dict[str, Any]:
    product_id = str((node.meta or {}).get("stage_id") or node.id.removeprefix("stage."))
    product_path = Path(outdir) / "artifacts" / "toy_products" / f"{product_id}.json"
    product_path.parent.mkdir(parents=True, exist_ok=True)
    write_json(value, product_path)
    return {
        "value": value,
        "items": [
            {
                "id": product_id,
                "path": product_path.relative_to(Path(outdir)).as_posix(),
                "producer": node.id,
            }
        ],
    }
