from __future__ import annotations

from pathlib import Path
from typing import Any

DATASET_METADATA_HOOK_SPEC = {
    "name": "toy.dataset_metadata",
    "kind": "compile_hook",
    "version": "1.0",
    "lifecycle": {"when": "after_datasets"},
    "input": {"artifacts": ["datasets"]},
    "result": {"artifacts": ["dataset_metadata"]},
}

IGNORED_HOOK_SPEC = {
    "name": "toy.ignored",
    "kind": "compile_hook",
    "version": "1.0",
    "lifecycle": {"when": "before_runtime"},
    "result": {"artifacts": ["ignored"]},
}

FAILING_HOOK_SPEC = {
    "name": "toy.fail",
    "kind": "compile_hook",
    "version": "1.0",
    "lifecycle": {"when": "after_datasets"},
}

GRAPH_RENDER_HOOK_SPEC = {
    "name": "toy.graph_render",
    "kind": "compile_hook",
    "version": "1.0",
    "lifecycle": {"when": "after_compile"},
    "input": {"artifacts": ["graph_d2"]},
    "result": {"artifacts": ["graph_render"]},
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


def graph_render_hook(ctx: Any, **params: Any) -> dict[str, Any]:
    del params
    graph_d2 = ctx.artifacts.get("graph_d2")
    return {
        "graph_render": {
            "has_graph_d2_artifact": graph_d2 is not None,
            "graph_d2_exists": bool(graph_d2 and Path(graph_d2).is_file()),
        }
    }
