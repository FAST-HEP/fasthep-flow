from __future__ import annotations

from typing import Any

from hepflow.compiler.context import CompileContext
from hepflow.compiler.profiles import ProfileExpansion, ProfileLoader
from hepflow.registry.defaults import (
    default_expr_registry_config,
    default_runtime_registry_config,
)
from hepflow.registry.merge import (
    RegistryLayer,
    RegistryMergeResult,
    merge_registry_layers,
)


def merge_workflow_registry(
    workflow: dict[str, Any],
    *,
    context: CompileContext,
    loader: ProfileLoader,
    expansion: ProfileExpansion,
) -> RegistryMergeResult:
    """Merge builtin, profile and workflow registries (the profiles are already expanded)."""
    builtin_registry = {
        **default_expr_registry_config(),
        **default_runtime_registry_config(),
    }
    profile_layers = []
    for name in expansion.names:
        config, provenance = loader.load(name)
        profile_layers.append(
            RegistryLayer(
                name=name,
                kind="profile",
                registry=dict(config.get("registry") or {}),
                path=provenance["path"],
            )
        )
    layers = [
        RegistryLayer(name="builtin", kind="builtin", registry=builtin_registry),
        *profile_layers,
        RegistryLayer(
            name="workflow",
            kind="workflow",
            registry=dict(workflow.get("registry") or {}),
            path=str(context.workflow_path),
        ),
    ]
    result = merge_registry_layers(layers)
    result.provenance["optional_profiles_skipped"] = expansion.skipped_optional
    return result
