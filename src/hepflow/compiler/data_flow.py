from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Any

from hepflow.compiler.expr_symbols import data_symbols_in_expr
from hepflow.model.component_spec import RuntimeComponentSpec
from hepflow.model.data_flow import DataDependencyResult
from hepflow.model.plan import ExecutionNode, ExecutionPlan
from hepflow.model.plan_applicability import active_plan_nodes_for_dataset
from hepflow.registry.defaults import (
    default_expr_registry_config,
    default_runtime_registry_config,
    merge_registry_config,
)
from hepflow.registry.loaders import load_object
from hepflow.runtime.hooks.loaders import hook_spec_context_symbols, load_hook_spec


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
        symbols.update(str(item) for item in hook_spec_context_symbols(spec))

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

    origins: dict[str, dict[str, Any]] = {}

    for stream_id, aliases in aliases_by_stream.items():
        for alias, branch in aliases.items():
            origins[alias] = {
                "kind": "alias",
                "stream": stream_id,
                "branch": branch,
            }

    common = _infer_data_flow_for_nodes(
        plan=plan,
        nodes=plan.nodes,
        registry=registry,
        dep_ctx=dep_ctx,
        primary_stream=primary_stream,
        aliases_by_stream=aliases_by_stream,
        origins=origins,
        dataset=None,
    )

    has_dataset_applicability = any(
        isinstance(node.meta.get("applies_to"), dict) for node in plan.nodes
    )
    datasets = dict(plan.context.get("datasets") or {}) if has_dataset_applicability else {}
    required_by_dataset: dict[str, dict[str, Any]] = {}
    for dataset_name, dataset in sorted(datasets.items()):
        dataset_origins = dict(origins)
        dataset_flow = _infer_data_flow_for_nodes(
            plan=plan,
            nodes=active_plan_nodes_for_dataset(
                plan,
                dataset=dict(dataset or {}),
            ),
            registry=registry,
            dep_ctx=dep_ctx,
            primary_stream=primary_stream,
            aliases_by_stream=aliases_by_stream,
            origins=dataset_origins,
            dataset=dict(dataset or {}),
        )
        required_by_dataset[str(dataset_name)] = dataset_flow["required_sources"]

    notes = [
        "Data flow is inferred for the primary event stream first; joined source branch decomposition is TODO.",
    ]
    if required_by_dataset:
        notes.append(
            "required_sources_by_dataset applies node dataset applicability before branch pruning."
        )

    return {
        "required_sources": common["required_sources"],
        "required_sources_by_dataset": required_by_dataset,
        "consumers": common["consumers"],
        "origins": {key: origins[key] for key in sorted(origins)},
        "notes": notes,
    }


def _infer_data_flow_for_nodes(
    *,
    plan: ExecutionPlan,
    nodes: list[ExecutionNode],
    registry: dict[str, Any],
    dep_ctx: DependencyContext,
    primary_stream: str,
    aliases_by_stream: dict[str, dict[str, str]],
    origins: dict[str, dict[str, Any]],
    dataset: dict[str, Any] | None,
) -> dict[str, Any]:
    produced_data: set[str] = set()
    source_required_data: dict[str, set[str]] = defaultdict(set)
    source_required_branches: dict[str, set[str]] = defaultdict(set)
    consumers: dict[str, list[str]] = defaultdict(list)

    for node in nodes:
        if node.role not in {"transform", "sink"}:
            continue

        spec = _component_spec_for_node(node, registry)
        if spec is None:
            continue
        deps = parse_component_data_dependencies(
            spec=spec,
            params=_dependency_params_for_dataset(node.params, dataset=dataset),
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
    }


def _dependency_params_for_dataset(
    params: dict[str, Any],
    *,
    dataset: dict[str, Any] | None,
) -> dict[str, Any]:
    if dataset is None:
        return params
    variations = params.get("variations")
    if not isinstance(variations, dict):
        return params
    apply_to = variations.get("apply_to")
    if not isinstance(apply_to, dict):
        return params
    eventtype = apply_to.get("eventtype")
    if eventtype is None or str(dataset.get("eventtype")) == str(eventtype):
        return params
    pruned = dict(params)
    pruned_variations = dict(variations)
    pruned_variations["weights"] = {}
    pruned["variations"] = pruned_variations
    return pruned


def apply_data_flow_to_sources(plan: ExecutionPlan) -> None:
    required_sources = (plan.data_flow or {}).get("required_sources") or {}
    required_by_dataset = (plan.data_flow or {}).get("required_sources_by_dataset") or {}

    for node in plan.nodes:
        if node.role != "source":
            continue
        source_name = str(node.meta.get("source_name") or node.id.removeprefix("read."))

        if required_by_dataset:
            branches_by_dataset: dict[str, list[str]] = {}
            for dataset_name, dataset_sources in dict(required_by_dataset).items():
                if not isinstance(dataset_sources, dict):
                    continue
                dataset_required = dataset_sources.get(source_name) or {}
                dataset_branches = {
                    str(branch)
                    for branch in list(
                        dict(dataset_required).get("branches") or []
                    )
                }
                if dataset_branches:
                    branches_by_dataset[str(dataset_name)] = sorted(dataset_branches)
            if branches_by_dataset:
                node.params["branches_by_dataset"] = branches_by_dataset
            continue

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
    result = DataDependencyResult()
    declared_provides = _provided_symbols_from_spec(
        component_spec,
        params=params,
    )
    result.produces.update(declared_provides)
    result.consumes.update(
        _required_symbols_from_spec(
            component_spec,
            params=params,
            dep_ctx=dep_ctx,
            produced=declared_provides,
        )
    )
    return result


def _component_spec_for_node(
    node: ExecutionNode,
    registry: dict[str, Any],
) -> RuntimeComponentSpec | None:
    category = {
        "transform": "transforms",
        "sink": "sinks",
    }.get(node.role)
    if category is None:
        return None
    entries = registry.get(category) or {}
    entry = entries.get(node.impl)
    if not isinstance(entry, dict):
        return None

    spec_ref = entry.get("spec")
    if not isinstance(spec_ref, str):
        return None

    spec = load_object(spec_ref)
    return RuntimeComponentSpec.from_obj(spec)


def _required_symbols_from_spec(
    spec: RuntimeComponentSpec,
    *,
    params: dict[str, Any],
    dep_ctx: DependencyContext,
    produced: set[str],
) -> set[str]:
    rules = (spec.requires or {}).get("symbols") or []
    if not isinstance(rules, list):
        raise TypeError(f"requires.symbols for {spec.name!r} must be a list")

    symbols: set[str] = set()
    for index, rule in enumerate(rules):
        if not isinstance(rule, dict):
            raise TypeError(
                f"requires.symbols[{index}] for {spec.name!r} must be a mapping"
            )
        kind = rule.get("kind")
        source = rule.get("from")
        if kind not in {
            "cutflow",
            "expr",
            "expr_or_field",
            "field_list",
            "field_prefix",
        }:
            raise ValueError(
                f"Unsupported requires.symbols kind for {spec.name!r}: {kind!r}"
            )
        if not isinstance(source, str) or not source.startswith("params."):
            raise ValueError(
                f"requires.symbols[{index}].from for {spec.name!r} must reference params.*"
            )
        values = _values_from_param_reference(
            params,
            source=source,
            spec=spec,
        )
        for value in values:
            if value is None:
                continue
            if value == "__variation__":
                continue
            if kind == "field_list":
                symbols.update(_field_names(value, source=source, spec_name=spec.name))
                continue
            if kind == "field_prefix":
                suffixes = rule.get("suffixes")
                if not isinstance(value, str) or not value.strip():
                    raise ValueError(
                        f"{source} for {spec.name!r} must be a non-empty string"
                    )
                if not isinstance(suffixes, list) or not all(
                    isinstance(item, str) and item for item in suffixes
                ):
                    raise ValueError(
                        f"field_prefix rule for {spec.name!r} requires string suffixes"
                    )
                symbols.update(f"{value.strip()}_{suffix}" for suffix in suffixes)
                continue
            if kind == "cutflow":
                for expression in _cutflow_expressions(value):
                    symbols.update(
                        data_symbols_in_expr(
                            expression,
                            known_functions=dep_ctx.known_functions,
                            known_constants=dep_ctx.known_constants,
                            context_symbols=dep_ctx.context_symbols,
                            produced=produced,
                        )
                    )
                continue
            if not isinstance(value, str) or not value.strip():
                raise ValueError(
                    f"{source} for {spec.name!r} must be a non-empty string "
                    f"for kind {kind!r}"
                )
            symbols.update(
                data_symbols_in_expr(
                    value,
                    known_functions=dep_ctx.known_functions,
                    known_constants=dep_ctx.known_constants,
                    context_symbols=dep_ctx.context_symbols,
                    produced=produced,
                )
            )
    return symbols


def _values_from_param_reference(
    params: dict[str, Any],
    *,
    source: str,
    spec: RuntimeComponentSpec,
) -> list[Any]:
    values: list[Any] = [params]
    for depth, segment in enumerate(source.split(".")[1:]):
        next_values: list[Any] = []
        for value in values:
            if value is None:
                continue
            if segment == "*":
                if isinstance(value, list):
                    next_values.extend(value)
                    continue
                if isinstance(value, dict):
                    next_values.extend(value.values())
                    continue
                raise TypeError(
                    f"Wildcard in {source} for {spec.name!r} requires a list or mapping"
                )
            if not isinstance(value, dict):
                raise TypeError(
                    f"Cannot resolve {source} for {spec.name!r}: "
                    f"{segment!r} is not inside a mapping"
                )
            if segment in value:
                next_values.append(value[segment])
            elif depth == 0:
                schema = spec.params.get(segment)
                if isinstance(schema, dict) and "default" in schema:
                    next_values.append(schema["default"])
        values = next_values
    return values


def _provided_symbols_from_spec(
    spec: RuntimeComponentSpec,
    *,
    params: dict[str, Any],
) -> set[str]:
    rules = (spec.provides or {}).get("symbols") or []
    if not isinstance(rules, list):
        raise TypeError(f"provides.symbols for {spec.name!r} must be a list")
    symbols: set[str] = set()
    for index, rule in enumerate(rules):
        if not isinstance(rule, dict) or rule.get("kind") != "field_list":
            raise ValueError(
                f"provides.symbols[{index}] for {spec.name!r} must use kind 'field_list'"
            )
        source = rule.get("from")
        if not isinstance(source, str) or not source.startswith("params."):
            raise ValueError(
                f"provides.symbols[{index}].from for {spec.name!r} must reference params.*"
            )
        for value in _values_from_param_reference(params, source=source, spec=spec):
            if value is not None:
                symbols.update(_field_names(value, source=source, spec_name=spec.name))
    return symbols


def _field_names(value: Any, *, source: str, spec_name: str) -> set[str]:
    if isinstance(value, str):
        values = [value]
    elif isinstance(value, list):
        values = value
    elif isinstance(value, dict):
        values = list(value)
    else:
        raise TypeError(f"{source} for {spec_name!r} must contain field names")
    if not all(isinstance(item, str) and item.strip() for item in values):
        raise ValueError(f"{source} for {spec_name!r} contains an invalid field name")
    return {item.strip() for item in values}


def _cutflow_expressions(selection: Any) -> list[str]:
    if not isinstance(selection, dict):
        return []
    expressions: list[str] = []
    for raw_group in selection.values():
        if isinstance(raw_group, list):
            raw_steps: Any = raw_group
        elif isinstance(raw_group, dict):
            raw_steps = raw_group.get("steps", raw_group.get("cuts", []))
        else:
            continue
        if not isinstance(raw_steps, list):
            continue
        for step in raw_steps:
            if isinstance(step, str) and step.strip():
                expressions.append(step)
            elif isinstance(step, dict):
                expression = step.get("expr")
                if isinstance(expression, str) and expression.strip():
                    expressions.append(expression)
                reduce_spec = step.get("reduce")
                if isinstance(reduce_spec, dict):
                    over = reduce_spec.get("over")
                    if isinstance(over, str) and over.strip():
                        expressions.append(over)
    return expressions


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
