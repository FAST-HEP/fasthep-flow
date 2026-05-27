from __future__ import annotations

from pathlib import Path
from typing import Any

from hepflow.compiler.profiles import (
    load_profile_registry_layer,
    normalize_profile_names,
)
from hepflow.registry.defaults import (
    default_expr_registry_config,
    default_runtime_registry_config,
)
from hepflow.registry.merge import (
    RegistryLayer,
    RegistryMergeResult,
    merge_registry_layers,
)


def resolve_author_registry(
    author: dict[str, Any],
    *,
    author_path: Path,
) -> RegistryMergeResult:
    project_root = author_path.parent
    use_block = author.get("use") or {}
    if not isinstance(use_block, dict):
        raise ValueError("use must be a mapping")

    profile_names = normalize_profile_names(use_block.get("profiles"))
    builtin_registry = {
        **default_expr_registry_config(),
        **default_runtime_registry_config(),
    }
    layers = [
        RegistryLayer(name="builtin", kind="builtin", registry=builtin_registry),
        *[
            load_profile_registry_layer(name, project_root=project_root)
            for name in profile_names
        ],
        RegistryLayer(
            name="author",
            kind="author",
            registry=dict(author.get("registry") or {}),
            path=str(author_path),
        ),
    ]
    return merge_registry_layers(layers)
