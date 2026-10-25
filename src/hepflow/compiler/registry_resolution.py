from __future__ import annotations

from typing import Any

from hepflow.compiler.context import CompileContext
from hepflow.compiler.profiles import (
    expand_profile_names_with_provenance,
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


def resolve_workflow_registry(
    workflow: dict[str, Any],
    *,
    context: CompileContext,
) -> RegistryMergeResult:
    project_root = context.project_root
    entry_dir = context.entry_dir
    use_block = workflow.get("use") or {}
    if not isinstance(use_block, dict):
        raise ValueError("use must be a mapping")

    profile_expansion = expand_profile_names_with_provenance(
        normalize_profile_names(use_block.get("profiles")),
        project_root=project_root,
        entry_dir=entry_dir,
    )
    profile_names = profile_expansion.names
    builtin_registry = {
        **default_expr_registry_config(),
        **default_runtime_registry_config(),
    }
    layers = [
        RegistryLayer(name="builtin", kind="builtin", registry=builtin_registry),
        *[
            load_profile_registry_layer(
                name, project_root=project_root, entry_dir=entry_dir
            )
            for name in profile_names
        ],
        RegistryLayer(
            name="workflow",
            kind="workflow",
            registry=dict(workflow.get("registry") or {}),
            path=str(context.workflow_path),
        ),
    ]
    result = merge_registry_layers(layers)
    result.provenance["optional_profiles_skipped"] = (
        profile_expansion.skipped_optional
    )
    return result
