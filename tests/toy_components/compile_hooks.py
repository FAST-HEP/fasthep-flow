from __future__ import annotations

from typing import Any


def dataset_metadata_hook(ctx: Any, **params: Any) -> dict[str, Any]:
    del params
    return {
        "dataset_metadata": {
            "hook": "after_datasets",
            "datasets": sorted((ctx.plan_context.get("datasets") or {}).keys()),
            "has_dataset_entries": "dataset_entries" in ctx.artifacts,
        }
    }


def ignored_hook(ctx: Any, **params: Any) -> dict[str, Any]:
    del ctx, params
    return {"ignored": {"ran": True}}


def failing_compile_hook(ctx: Any, **params: Any) -> dict[str, Any]:
    del ctx, params
    raise ValueError("compile boom")
