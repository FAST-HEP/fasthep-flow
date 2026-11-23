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

SILENT_HOOK_SPEC = {
    "name": "toy.silent",
    "kind": "compile_hook",
    "version": "1.0",
    "lifecycle": {"when": "after_datasets"},
}


def silent_hook(ctx: Any, **params: Any) -> None:
    del ctx, params


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


OUTDIR_ARTIFACT_HOOK_SPEC = {
    "name": "toy.outdir_artifact",
    "kind": "compile_hook",
    "version": "1.0",
    "lifecycle": {"when": "after_compile"},
    "result": {"artifacts": ["outdir_artifact"]},
}

FAILING_AFTER_COMPILE_HOOK_SPEC = {
    "name": "toy.fail_after_compile",
    "kind": "compile_hook",
    "version": "1.0",
    "lifecycle": {"when": "after_compile"},
}


def outdir_artifact_hook(ctx: Any, **params: Any) -> dict[str, Any]:
    """An artifact that names a file below the output directory (like a rendered graph)."""
    del params
    return {"outdir_artifact": {"svg": str(ctx.build_paths.graph_file("graph.svg"))}}


FILE_COUNTS_HOOK_SPEC = {
    "name": "toy.file_counts",
    "kind": "compile_hook",
    "version": "1.0",
    "lifecycle": {"when": "after_datasets"},
    "input": {"artifacts": ["datasets"]},
    "result": {"artifacts": ["dataset_metadata"]},
}


def file_counts_hook(
    ctx: Any, *, counts: dict[str, int] | None = None, add_per_file: bool = False, **params: Any
) -> dict[str, Any]:
    """Like the curator hook: per-file entries only with ``add_per_file``.

    Records every dataset of the context it sees, so a test can tell which datasets a hook
    is given. ``counts`` maps file path to entries.
    """
    del params
    datasets = dict(ctx.plan_context.get("datasets") or {})
    return {
        "dataset_metadata": {
            "datasets": {
                name: {
                    "files": (
                        {
                            path: {"entries": (counts or {})[path]}
                            for path in record.get("files") or []
                            if path in (counts or {})
                        }
                        if add_per_file
                        else {}
                    ),
                    "total_entries": sum(
                        (counts or {}).get(path, 0) for path in record.get("files") or []
                    ),
                }
                for name, record in datasets.items()
            }
        }
    }
