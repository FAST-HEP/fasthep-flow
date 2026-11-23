"""Resolve the profile-driven sections of a workflow in one pass.

``registry``, ``execution`` and ``execution_hooks`` are each layered as builtin, selected
profiles, workflow. They share the ``use.profiles`` list, its expansion and the profile files, so
one :class:`~hepflow.compiler.profiles.ProfileLoader` expands the names once and reads each
profile file once per compile.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from hepflow.compiler.context import CompileContext
from hepflow.compiler.execution import (
    merge_workflow_execution,
    merge_workflow_execution_hooks,
)
from hepflow.compiler.profiles import ProfileLoader, normalize_profile_names
from hepflow.compiler.registry_resolution import merge_workflow_registry
from hepflow.registry.merge import RegistryMergeResult


@dataclass(frozen=True)
class WorkflowConfig:
    """The three resolved sections with the provenance of their layers."""

    registry: RegistryMergeResult
    execution: dict[str, Any]  # {"execution": ..., "provenance": {"execution_layers": ...}}
    execution_hooks: dict[str, Any]  # {"execution_hooks": ..., "provenance": {...}}


def resolve_workflow_config(
    workflow: dict[str, Any], *, context: CompileContext
) -> WorkflowConfig:
    """Resolve registry, execution and execution hooks of ``workflow`` for one compile."""
    use_block = workflow.get("use") or {}
    if not isinstance(use_block, dict):
        raise ValueError("use must be a mapping")
    loader = ProfileLoader(project_root=context.project_root, entry_dir=context.entry_dir)
    expansion = loader.expand(normalize_profile_names(use_block.get("profiles")))
    return WorkflowConfig(
        registry=merge_workflow_registry(
            workflow, context=context, loader=loader, expansion=expansion
        ),
        execution=merge_workflow_execution(
            workflow, context=context, loader=loader, expansion=expansion
        ),
        execution_hooks=merge_workflow_execution_hooks(
            workflow, context=context, loader=loader, expansion=expansion
        ),
    )
