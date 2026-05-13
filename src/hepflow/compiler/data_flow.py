from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Any

from hepflow.model.component_spec import RuntimeComponentSpec
from hepflow.model.data_flow import DataDependencyResult
from hepflow.model.plan import ExecutionPlan
from hepflow.registry.defaults import (
    default_expr_registry_config,
    default_runtime_registry_config,
    merge_registry_config,
)
from hepflow.registry.loaders import load_object
from hepflow.runtime.hooks.loaders import load_hook_spec


@dataclass(slots=True, frozen=True)
class DependencyContext:
    known_functions: set[str]
    known_constants: set[str]
    context_symbols: set[str]


def expression_registry_symbol_names(
    registry_cfg: dict[str, Any] | None,
) -> tuple[set[str], set[str]]:
    registry_cfg = registry_cfg or {}
    return (
        {str(name) for name in (registry_cfg.get("functions") or {})},
        {str(name) for name in (registry_cfg.get("constants") or {})},
    )


def context_symbols_from_plan(
    plan: ExecutionPlan,
    registry_cfg: dict[str, Any] | None = None,
) -> set[str]:
    if registry_cfg is None:
        registry = merge_registry_config(
            {
                **default_expr_registry_config(),
                **default_runtime_registry_config(),
            },
            plan.registry or {},
        )
    else:
        registry = registry_cfg
    symbols = {str(name) for name in (plan.context.get("globals") or {})}

    for hook in list(plan.execution_hooks or []):
        if not isinstance(hook, dict):
            continue
        kind = str(hook.get("kind") or "")
        if not kind:
            continue
        spec = load_hook_spec(registry, kind)
        symbols.update(str(item) for item in spec.context_outputs)

    return symbols


def infer_data_flow(
    plan: ExecutionPlan,
    *,
    registry_cfg: dict[str, Any] | None = None,
) -> dict[str, Any]:
    # TODO: rename this module once the public data-flow model stabilizes.
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

    primary_stream = _primary_stream_id(plan)
    aliases_by_stream = _aliases_by_stream(plan)

    produced_data: set[str] = set()
    source_required_data: dict[str, set[str]] = defaultdict(set)
    source_required_branches: dict[str, set[str]] = defaultdict(set)
    consumers: dict[str, list[str]] = defaultdict(list)
    origins: dict[str, dict[str, Any]] = {}

    for stream_id, aliases in aliases_by_stream.items():
        for alias, branch in aliases.items():
            origins[alias] = {
                "kind": "alias",
                "stream": stream_id,
                "branch": branch,
            }

    for node in plan.nodes:
        if node.role != "transform":
            continue

        spec = _component_spec_for_node(node.impl, registry)
        if spec is None:
            continue
        deps = parse_component_data_dependencies(
            spec=spec,
            params=node.params,
            dep_ctx=dep_ctx,
        )

        stream_id = str(node.params.get("stream_id") or primary_stream)
        for consumed in sorted(deps.consumes):
            consumers[consumed].append(node.id)

            if consumed in produced_data and node.impl != "hep.project_fields":
                continue

            required_stream = stream_id if node.impl == "hep.project_fields" else primary_stream
            branch = _resolve_required_branch(
                consumed,
                stream_id=required_stream,
                aliases_by_stream=aliases_by_stream,
            )
            source_required_data[required_stream].add(consumed)
            source_required_branches[required_stream].add(branch)

        for produced in sorted(deps.produces):
            produced_data.add(produced)
            if node.impl == "hep.project_fields" and produced in aliases_by_stream.get(stream_id, {}):
                origins[produced] = {
                    "kind": "alias",
                    "stream": stream_id,
                    "branch": aliases_by_stream[stream_id][produced],
                }
                continue
            origins[produced] = {
                "kind": "produced",
                "node": node.id,
            }

    source_required_branches = _route_required_branches_to_leaf_sources(
        plan,
        source_required_branches,
    )

    required_sources = {
        stream_id: {
            "data": sorted(source_required_data.get(stream_id, set())),
            "branches": sorted(branches),
        }
        for stream_id, branches in sorted(source_required_branches.items())
    }

    return {
        "required_sources": required_sources,
        "consumers": dict(sorted(consumers.items())),
        "origins": {key: origins[key] for key in sorted(origins)},
        "notes": [
            "Data flow is inferred for the primary event stream first; joined source branch decomposition is TODO.",
        ],
    }


def apply_data_flow_to_sources(plan: ExecutionPlan) -> None:
    required_sources = (plan.data_flow or {}).get("required_sources") or {}

    for node in plan.nodes:
        if node.role != "source":
            continue
        source_name = str(node.meta.get("source_name") or node.id.removeprefix("read."))
        required = required_sources.get(source_name) or {}
        branches = {str(branch) for branch in (required.get("branches") or [])}
        if not branches:
            continue

        existing = {str(branch) for branch in (node.params.get("branches") or [])}
        node.params["branches"] = sorted(existing | branches)


def parse_component_data_dependencies(
    *,
    spec: Any,
    params: dict[str, Any],
    dep_ctx: DependencyContext,
) -> DataDependencyResult:
    component_spec = RuntimeComponentSpec.from_obj(spec)
    parser_ref = (component_spec.dependencies or {}).get("parser")
    if not parser_ref:
        return DataDependencyResult()
    if not isinstance(parser_ref, str):
        raise TypeError(
            f"Dependency parser reference for {component_spec.name!r} must be a string"
        )

    parser = load_object(parser_ref)
    if not callable(parser):
        raise TypeError(f"Dependency parser for {component_spec.name!r} is not callable")
    return parser(
        params,
        known_functions=dep_ctx.known_functions,
        known_constants=dep_ctx.known_constants,
        context_symbols=dep_ctx.context_symbols,
    )


def _component_spec_for_node(
    impl: str,
    registry: dict[str, Any],
) -> RuntimeComponentSpec | None:
    transforms = registry.get("transforms") or {}
    entry = transforms.get(impl)
    if not isinstance(entry, dict):
        return None

    spec_ref = entry.get("spec")
    if not isinstance(spec_ref, str):
        return None

    spec = load_object(spec_ref)
    return RuntimeComponentSpec.from_obj(spec)


def _primary_stream_id(plan: ExecutionPlan) -> str:
    for node in plan.nodes:
        if node.role == "source":
            return str(node.meta.get("source_name") or node.id.removeprefix("read."))
    return "events"


def _aliases_by_stream(plan: ExecutionPlan) -> dict[str, dict[str, str]]:
    aliases: dict[str, dict[str, str]] = defaultdict(dict)
    for node in plan.nodes:
        if node.impl != "hep.project_fields":
            continue
        stream_id = str(node.params.get("stream_id") or _primary_stream_id(plan))
        for alias, branch in dict(node.params.get("aliases") or {}).items():
            if isinstance(alias, str) and isinstance(branch, str):
                aliases[stream_id][alias] = branch
    return {stream_id: dict(items) for stream_id, items in aliases.items()}


def _resolve_required_branch(
    column: str,
    *,
    stream_id: str,
    aliases_by_stream: dict[str, dict[str, str]],
) -> str:
    return aliases_by_stream.get(stream_id, {}).get(column, column)


def _route_required_branches_to_leaf_sources(
    plan: ExecutionPlan,
    required_branches: dict[str, set[str]],
) -> dict[str, set[str]]:
    join_routes = _join_prefix_routes(plan)
    if not join_routes:
        return required_branches

    routed: dict[str, set[str]] = defaultdict(set)
    for stream_id, branches in required_branches.items():
        prefix_to_source = join_routes.get(stream_id)
        if not prefix_to_source:
            routed[stream_id].update(branches)
            continue

        for branch in branches:
            if "." not in branch:
                routed[stream_id].add(branch)
                continue
            prefix, leaf_branch = branch.split(".", 1)
            source_name = prefix_to_source.get(prefix)
            if source_name is None:
                routed[stream_id].add(branch)
                continue
            routed[source_name].add(leaf_branch)

    return dict(routed)


def _join_prefix_routes(plan: ExecutionPlan) -> dict[str, dict[str, str]]:
    routes: dict[str, dict[str, str]] = {}
    for node in plan.nodes:
        if node.impl != "hep.zip_join":
            continue
        stream_id = node.id.removeprefix("join.")
        prefix_to_source: dict[str, str] = {}
        for item in list(node.params.get("inputs") or []):
            if not isinstance(item, dict):
                continue
            source_name = item.get("name") or item.get("source")
            prefix = item.get("prefix")
            if isinstance(source_name, str) and isinstance(prefix, str):
                prefix_to_source[prefix] = source_name
        if prefix_to_source:
            routes[stream_id] = prefix_to_source
    return routes
