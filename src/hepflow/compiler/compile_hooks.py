from __future__ import annotations

import warnings
from typing import Any

from hepflow.build_layout import BuildPaths
from hepflow.compiler.data_flow import (
    DependencyContext,
    component_spec_for_node,
    context_symbols_from_plan,
    expression_registry_symbol_names,
    input_stream_fields_by_context,
)
from hepflow.compiler.hooks.model import CompileHookContext
from hepflow.compiler.hooks.runner import (
    run_parameter_hook_chains_for_node,
    run_phase_hooks,
)
from hepflow.model.plan import ExecutionPlan
from hepflow.model.plan_applicability import active_plan_nodes_for_dataset
from hepflow.registry.defaults import (
    default_expr_registry_config,
    default_runtime_registry_config,
    merge_registry_config,
)


def run_compile_hooks(
    *,
    plan: ExecutionPlan,
    normalized: dict[str, Any] | None,
    build_paths: BuildPaths,
    when: str,
    artifacts: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Run registry-provided compile hooks for one compile phase."""
    artifact_map = dict(artifacts or {})
    ctx = CompileHookContext(
        normalized=dict(normalized or {}),
        plan_context=dict(plan.context or {}),
        build_paths=build_paths,
        artifacts=artifact_map,
    )
    return run_phase_hooks(registry=plan.registry, context=ctx, when=when)


def run_param_compile_hooks(
    plan: ExecutionPlan,
    *,
    registry_cfg: dict[str, Any] | None = None,
    warn: bool = True,
) -> None:
    registry = merge_registry_config(
        {
            **default_expr_registry_config(),
            **default_runtime_registry_config(),
        },
        registry_cfg or plan.registry or {},
    )
    known_functions, known_constants = expression_registry_symbol_names(registry)
    context_symbols = context_symbols_from_plan(plan, registry)
    dep_ctx = DependencyContext(
        known_functions=known_functions,
        known_constants=known_constants,
        context_symbols=context_symbols,
    )
    active_contexts = _active_contexts(plan)
    input_fields_by_context = input_stream_fields_by_context(
        plan=plan,
        active_contexts=active_contexts,
        registry=registry,
        dep_ctx=dep_ctx,
    )

    for node in plan.nodes:
        if node.role not in {"transform", "sink"}:
            continue
        spec = component_spec_for_node(node, registry)
        if spec is None:
            continue
        provenance_by_param = run_parameter_hook_chains_for_node(
            node=node,
            spec=spec,
            registry=registry,
            input_fields_by_context=input_fields_by_context,
        )
        for param_name, records in provenance_by_param.items():
            _record_param_hook_provenance(
                node_id=node.id,
                node_meta=node.meta,
                param_name=param_name,
                records=records,
                spec_name=spec.name,
                emit_warnings=warn,
            )


def _active_contexts(
    plan: ExecutionPlan,
) -> list[tuple[str | None, dict[str, Any] | None, set[str]]]:
    if any(isinstance(node.meta.get("applies_to"), dict) for node in plan.nodes):
        return [
            (
                str(dataset_name),
                dict(dataset or {}),
                {
                    node.id
                    for node in active_plan_nodes_for_dataset(
                        plan,
                        dataset=dict(dataset or {}),
                    )
                },
            )
            for dataset_name, dataset in sorted(dict(plan.context.get("datasets") or {}).items())
        ]
    return [(None, None, {node.id for node in plan.nodes})]


def _record_param_hook_provenance(
    *,
    node_id: str,
    node_meta: dict[str, Any],
    param_name: str,
    records: list[dict[str, Any]],
    spec_name: str,
    emit_warnings: bool,
) -> None:
    compile_hooks = node_meta.setdefault("compile_hooks", {})
    compile_hooks[param_name] = [dict(record) for record in records]
    if not emit_warnings:
        return
    for record in records:
        unmatched = record.get("unmatched")
        if not isinstance(unmatched, list):
            continue
        for pattern in unmatched:
            warnings.warn(
                f"{spec_name} parameter {param_name!r} field_glob pattern "
                f"{pattern!r} matched no input stream fields for {node_id}",
                stacklevel=3,
            )
