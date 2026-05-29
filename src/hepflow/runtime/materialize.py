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
            write_json(_json_safe(value_store[key]), Path(outdir) / relative_path)
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
