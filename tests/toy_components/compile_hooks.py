from __future__ import annotations

from typing import Any

DATASET_METADATA_HOOK_SPEC = {
    "name": "toy.dataset_metadata",
    "kind": "compile_hook",
    "version": "1.0",
    "lifecycle": {"when": "after_datasets"},
    "inputs": ["datasets"],
    "outputs": ["dataset_metadata"],
}

IGNORED_HOOK_SPEC = {
    "name": "toy.ignored",
    "kind": "compile_hook",
    "version": "1.0",
    "lifecycle": {"when": "before_runtime"},
    "outputs": ["ignored"],
}

FAILING_HOOK_SPEC = {
    "name": "toy.fail",
    "kind": "compile_hook",
    "version": "1.0",
    "lifecycle": {"when": "after_datasets"},
}

OLD_SHAPE_HOOK_SPEC = {
    "name": "toy.old_shape",
    "kind": "compile_hook",
    "when": "after_datasets",
    "outputs": ["dataset_metadata"],
}


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
