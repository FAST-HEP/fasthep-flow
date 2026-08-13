from __future__ import annotations

import fnmatch
import warnings
from collections import defaultdict
from typing import Any

from hepflow.compiler.expr_symbols import data_symbols_in_expr
from hepflow.model.component_spec import RuntimeComponentSpec
from hepflow.model.data_flow import DataDependencyResult, DependencyContext
from hepflow.model.hooks import CompileHookResult, ParamCompileHookContext
from hepflow.model.plan import ExecutionNode, ExecutionPlan
from hepflow.model.plan_applicability import (
    active_plan_nodes_for_dataset,
    resolve_active_input_ref,
)
from hepflow.registry.defaults import (
    default_expr_registry_config,
    default_runtime_registry_config,
    merge_registry_config,
)
from hepflow.registry.loaders import load_object
from hepflow.runtime.hooks.loaders import hook_spec_context_symbols, load_hook_spec


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
                    for branch in list(dict(dataset_required).get("branches") or [])
                    if not _is_glob_pattern(str(branch))
                }
                if dataset_branches:
                    branches_by_dataset[str(dataset_name)] = sorted(dataset_branches)
            if branches_by_dataset:
                node.params["branches_by_dataset"] = branches_by_dataset
            continue

        required = required_sources.get(source_name) or {}
        branches = {
            str(branch)
            for branch in (required.get("branches") or [])
            if not _is_glob_pattern(str(branch))
        }
        if not branches:
            continue

        existing = [
            str(branch)
            for branch in (node.params.get("branches") or [])
            if not _is_glob_pattern(str(branch))
        ]
        node.params["branches"] = _merge_ordered_fields(
            [existing, sorted(branches - set(existing))]
        )


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

    datasets = dict(plan.context.get("datasets") or {})
    active_contexts: list[tuple[str | None, dict[str, Any] | None, set[str]]] = []
    if any(isinstance(node.meta.get("applies_to"), dict) for node in plan.nodes):
        for dataset_name, dataset in sorted(datasets.items()):
            active_contexts.append(
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
            )
    else:
        active_contexts.append((None, None, {node.id for node in plan.nodes}))

    input_fields_by_context: dict[str | None, dict[str, list[str]]] = {}
    for context_name, dataset, active_ids in active_contexts:
        input_fields_by_context[context_name] = _stream_fields_for_nodes(
            plan=plan,
            active_ids=active_ids,
            dataset_name=context_name,
            dataset=dataset,
            registry=registry,
            dep_ctx=dep_ctx,
        )

    for node in plan.nodes:
        if node.role not in {"transform", "sink"}:
            continue
        spec = _component_spec_for_node(node, registry)
        if spec is None:
            continue
        _run_param_hook_chains_for_node(
            node=node,
            spec=spec,
            registry=registry,
            input_fields_by_context=input_fields_by_context,
            warn=warn,
        )


def _stream_fields_for_nodes(
    *,
    plan: ExecutionPlan,
    active_ids: set[str],
    dataset_name: str | None,
    dataset: dict[str, Any] | None,
    registry: dict[str, Any],
    dep_ctx: DependencyContext,
) -> dict[str, list[str]]:
    node_output_fields: dict[str, list[str]] = {}
    node_input_fields: dict[str, list[str]] = {}

    for node in plan.nodes:
        if node.id not in active_ids:
            continue

        if node.role == "source":
            node_output_fields[node.id] = _source_output_fields(
                node,
                dataset_name=dataset_name,
            )
            continue

        input_fields = _primary_input_fields(
            plan=plan,
            node=node,
            node_output_fields=node_output_fields,
            dataset=dataset,
        )
        node_input_fields[node.id] = input_fields
        spec = _component_spec_for_node(node, registry)
        if spec is not None:
            deps = parse_component_data_dependencies(
                spec=spec,
                params=_dependency_params_for_dataset(
                    dict(node.params),
                    dataset=dataset,
                ),
                dep_ctx=dep_ctx,
            )
        else:
            deps = DataDependencyResult()

        if node.role == "sink":
            node_output_fields[node.id] = []
            continue

        node_output_fields[node.id] = _transform_output_fields(
            node=node,
            input_fields=input_fields,
            deps=deps,
            params=node.params,
        )

    return node_input_fields


def _source_output_fields(
    node: ExecutionNode,
    *,
    dataset_name: str | None,
) -> list[str]:
    by_dataset = node.params.get("branches_by_dataset")
    if dataset_name is not None and isinstance(by_dataset, dict):
        fields = by_dataset.get(dataset_name)
        if isinstance(fields, list):
            return _literal_fields(_string_list(fields))
    for key in ("branches", "fields"):
        fields = node.params.get(key)
        if isinstance(fields, list):
            return _literal_fields(_string_list(fields))
    return []


def _primary_input_fields(
    *,
    plan: ExecutionPlan,
    node: ExecutionNode,
    node_output_fields: dict[str, list[str]],
    dataset: dict[str, Any] | None,
) -> list[str]:
    for ref in node.inputs:
        active_ref = resolve_active_input_ref(plan, ref, dataset=dataset)
        if active_ref.input_name == "dependency":
            continue
        if active_ref.output_name != "stream":
            continue
        return list(node_output_fields.get(active_ref.node_id, []))
    return []


def _run_param_hook_chains_for_node(
    *,
    node: ExecutionNode,
    spec: RuntimeComponentSpec,
    registry: dict[str, Any],
    input_fields_by_context: dict[str | None, dict[str, list[str]]],
    warn: bool,
) -> None:
    contexts = {
        context_name: fields_by_node.get(node.id, [])
        for context_name, fields_by_node in input_fields_by_context.items()
        if node.id in fields_by_node
    }
    if not contexts:
        return

    for param_name, schema in dict(spec.params or {}).items():
        if not isinstance(schema, dict):
            continue
        hooks = _param_hook_chain(schema, param_name=param_name, spec_name=spec.name)
        if not hooks:
            continue
        original = node.params.get(param_name, schema.get("default"))
        if original in (None, False):
            continue

        context_outputs: dict[str | None, tuple[Any, list[dict[str, Any]]]] = {}
        for context_name, input_fields in contexts.items():
            value = original
            provenance: list[dict[str, Any]] = []
            context = ParamCompileHookContext(
                input_stream_fields=tuple(input_fields),
            )
            for hook_options in hooks:
                result = _run_single_param_hook(
                    value=value,
                    hook_options=hook_options,
                    context=context,
                    registry=registry,
                    param_name=param_name,
                    spec_name=spec.name,
                )
                value = result.value
                provenance.append(result.provenance)
            context_outputs[context_name] = (value, provenance)

        selected_value, selected_provenance = _require_identical_param_hook_outputs(
            node=node,
            param_name=param_name,
            context_outputs=context_outputs,
        )
        node.params[param_name] = selected_value
        _record_param_hook_provenance(
            node=node,
            param_name=param_name,
            records=selected_provenance,
            spec_name=spec.name,
            emit_warnings=warn,
        )


def _transform_output_fields(
    *,
    node: ExecutionNode,
    input_fields: list[str],
    deps: DataDependencyResult,
    params: dict[str, Any],
) -> list[str]:
    if node.impl == "hep.project_fields":
        aliases = dict(params.get("aliases") or {})
        return [str(alias) for alias in aliases if isinstance(alias, str)]
    if node.impl == "hep.align_schema":
        return _align_schema_output_fields(input_fields=input_fields, params=params)
    return _merge_ordered_fields([input_fields, sorted(deps.produces)])


def _align_schema_output_fields(
    *,
    input_fields: list[str],
    params: dict[str, Any],
) -> list[str]:
    schema = params.get("schema")
    fields = []
    if isinstance(schema, dict) and isinstance(schema.get("fields"), dict):
        fields = [str(name) for name in schema["fields"]]
    keep = params.get("keep")
    drop = params.get("drop")
    if keep not in (None, False):
        extras = [
            field
            for field in _string_list(keep)
            if field in input_fields and field not in fields
        ]
        return _merge_ordered_fields([fields, extras])
    if str(params.get("extra", "keep")) != "keep":
        return fields
    dropped = set(_string_list(drop)) if drop not in (None, False) else set()
    extras = [
        field for field in input_fields if field not in fields and field not in dropped
    ]
    return _merge_ordered_fields([fields, extras])


def expand_field_glob(
    *,
    value: Any,
    options: dict[str, Any],
    context: ParamCompileHookContext,
) -> CompileHookResult:
    against = str(options.get("against") or "")
    if against != "input.stream":
        raise ValueError("flow.expand_field_glob currently supports against='input.stream'")
    patterns = _string_list_param(
        value,
        param_name=str(options.get("_param_name") or "value"),
        spec_name=str(options.get("_spec_name") or "component"),
    )
    expanded, unmatched = _expand_field_glob_patterns(
        patterns,
        available_fields=list(context.input_stream_fields),
    )
    return CompileHookResult(
        value=expanded,
        provenance={
            "hook": "flow.expand_field_glob",
            "against": against,
            "input": patterns,
            "output": expanded,
            "unmatched": unmatched,
        },
    )


def _param_hook_chain(
    schema: dict[str, Any],
    *,
    param_name: str,
    spec_name: str,
) -> list[dict[str, Any]]:
    raw_hooks = schema.get("hooks")
    if raw_hooks is None:
        return []
    if not isinstance(raw_hooks, list):
        raise TypeError(f"{spec_name} parameter {param_name!r} hooks must be a list")
    hooks: list[dict[str, Any]] = []
    for index, raw_hook in enumerate(raw_hooks):
        if not isinstance(raw_hook, dict):
            raise TypeError(
                f"{spec_name} parameter {param_name!r} hooks[{index}] must be a mapping"
            )
        name = raw_hook.get("name")
        if not isinstance(name, str) or not name.strip():
            raise ValueError(
                f"{spec_name} parameter {param_name!r} hooks[{index}] "
                "requires non-empty 'name'"
            )
        hook = dict(raw_hook)
        hook["_param_name"] = param_name
        hook["_spec_name"] = spec_name
        hooks.append(hook)
    return hooks


def _run_single_param_hook(
    *,
    value: Any,
    hook_options: dict[str, Any],
    context: ParamCompileHookContext,
    registry: dict[str, Any],
    param_name: str,
    spec_name: str,
) -> CompileHookResult:
    hook_name = str(hook_options["name"])
    entry = dict((registry.get("compile_hooks") or {}).get(hook_name) or {})
    impl_ref = entry.get("impl")
    if not isinstance(impl_ref, str) or not impl_ref.strip():
        raise KeyError(f"Parameter compile hook {hook_name!r} is not registered")
    impl = load_object(impl_ref)
    if not callable(impl):
        raise TypeError(f"Parameter compile hook {hook_name!r} implementation is not callable")
    try:
        result = impl(
            value=value,
            options={k: v for k, v in hook_options.items() if k != "name"},
            context=context,
        )
    except Exception as exc:
        raise RuntimeError(
            f"Parameter compile hook {hook_name!r} failed for "
            f"{spec_name} parameter {param_name!r}: {exc}"
        ) from exc
    if isinstance(result, CompileHookResult):
        return result
    if isinstance(result, dict) and "value" in result:
        provenance = result.get("provenance") or {}
        if not isinstance(provenance, dict):
            raise TypeError(
                f"Parameter compile hook {hook_name!r} provenance must be a mapping"
            )
        return CompileHookResult(value=result["value"], provenance=provenance)
    raise TypeError(
        f"Parameter compile hook {hook_name!r} must return CompileHookResult"
    )


def _require_identical_param_hook_outputs(
    *,
    node: ExecutionNode,
    param_name: str,
    context_outputs: dict[str | None, tuple[Any, list[dict[str, Any]]]],
) -> tuple[Any, list[dict[str, Any]]]:
    values = list(context_outputs.items())
    if not values:
        raise ValueError(f"No compile-hook contexts available for {node.id}.{param_name}")
    base_context, (base_value, base_provenance) = values[0]
    for context_name, (value, _provenance) in values[1:]:
        if value != base_value:
            raise ValueError(
                f"Parameter compile hooks for {node.id} parameter {param_name!r} "
                "expanded differently across applicable contexts "
                f"{base_context!r} and {context_name!r}"
            )
    return base_value, base_provenance


def _expand_field_glob_patterns(
    patterns: list[str],
    *,
    available_fields: list[str],
) -> tuple[list[str], list[str]]:
    expanded: list[str] = []
    seen: set[str] = set()
    unmatched: list[str] = []
    for pattern in patterns:
        matches = [field for field in available_fields if fnmatch.fnmatchcase(field, pattern)]
        if not matches:
            unmatched.append(pattern)
            continue
        for field in matches:
            if field in seen:
                continue
            expanded.append(field)
            seen.add(field)
    return expanded, unmatched


def _is_glob_pattern(value: str) -> bool:
    return any(char in value for char in "*?[")


def _literal_fields(fields: list[str]) -> list[str]:
    return [field for field in fields if not _is_glob_pattern(field)]


def _record_param_hook_provenance(
    *,
    node: ExecutionNode,
    param_name: str,
    records: list[dict[str, Any]],
    spec_name: str,
    emit_warnings: bool,
) -> None:
    compile_hooks = node.meta.setdefault("compile_hooks", {})
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
                f"{pattern!r} matched no input stream fields for {node.id}",
                stacklevel=3,
            )


def _string_list_param(value: Any, *, param_name: str, spec_name: str) -> list[str]:
    if isinstance(value, str):
        values = [value]
    elif isinstance(value, list):
        values = value
    else:
        raise TypeError(f"{spec_name} parameter {param_name!r} must be a list of strings")
    if not all(isinstance(item, str) and item.strip() for item in values):
        raise ValueError(f"{spec_name} parameter {param_name!r} contains an invalid field")
    return [str(item).strip() for item in values]


def _string_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if isinstance(item, str)]
    return []


def _merge_ordered_fields(field_groups: Any) -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()
    for fields in field_groups:
        for field in fields:
            if field in seen:
                continue
            merged.append(field)
            seen.add(field)
    return merged


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
    return _merge_custom_dependency_parser(
        result,
        spec=component_spec,
        params=params,
        dep_ctx=dep_ctx,
    )


def _merge_custom_dependency_parser(
    result: DataDependencyResult,
    *,
    spec: RuntimeComponentSpec,
    params: dict[str, Any],
    dep_ctx: DependencyContext,
) -> DataDependencyResult:
    parser_ref = spec.dependency_parser
    if parser_ref is None:
        return result

    parser = load_object(parser_ref) if isinstance(parser_ref, str) else parser_ref
    if not callable(parser):
        raise TypeError(f"dependency_parser for {spec.name!r} must be callable")

    parsed = parser(
        params,
        known_functions=dep_ctx.known_functions,
        known_constants=dep_ctx.known_constants,
        context_symbols=dep_ctx.context_symbols,
    )
    if not isinstance(parsed, DataDependencyResult):
        raise TypeError(
            f"dependency_parser for {spec.name!r} must return DataDependencyResult"
        )

    result.consumes.update(parsed.consumes)
    result.produces.update(parsed.produces)
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
            "relative_expr",
            "scoped_expr",
        }:
            raise ValueError(
                f"Unsupported requires.symbols kind for {spec.name!r}: {kind!r}"
            )
        if not isinstance(source, str) or not source.startswith("params."):
            raise ValueError(
                f"requires.symbols[{index}].from for {spec.name!r} must reference params.*"
            )
        if _rule_should_skip(rule, params=params, spec=spec):
            continue
        values = _values_from_param_reference(params, source=source, spec=spec)
        for value in values:
            if value is None:
                continue
            if value == "__variation__":
                continue
            if kind == "field_list":
                symbols.update(_field_names(value, source=source, spec_name=spec.name))
                continue
            if kind == "field_prefix":
                suffixes = _suffixes_for_rule(rule, params=params, spec=spec)
                symbols.update(
                    f"{prefix}_{suffix}"
                    for prefix in _field_names(
                        value,
                        source=source,
                        spec_name=spec.name,
                    )
                    for suffix in suffixes
                )
                continue
            if kind == "relative_expr":
                prefix_source = rule.get("prefix_from")
                if not isinstance(prefix_source, str) or not prefix_source.startswith(
                    "params."
                ):
                    raise ValueError(
                        f"relative_expr rule for {spec.name!r} requires "
                        "'prefix_from' referencing params.*"
                    )
                prefixes = _param_field_names(
                    params,
                    source=prefix_source,
                    spec=spec,
                    param_name="prefix_from",
                )
                for expression in _expression_values(value, source=source, spec=spec):
                    fields = data_symbols_in_expr(
                        expression,
                        known_functions=dep_ctx.known_functions,
                        known_constants=dep_ctx.known_constants,
                        context_symbols=dep_ctx.context_symbols,
                        produced=produced,
                    )
                    symbols.update(
                        f"{prefix}_{field}" for prefix in prefixes for field in fields
                    )
                continue
            if kind == "scoped_expr":
                for expression in _expression_values(value, source=source, spec=spec):
                    symbols.update(
                        _scoped_expression_dependencies(
                            expression,
                            rule=rule,
                            params=params,
                            spec=spec,
                            dep_ctx=dep_ctx,
                        )
                    )
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


def _suffixes_for_rule(
    rule: dict[str, Any],
    *,
    params: dict[str, Any],
    spec: RuntimeComponentSpec,
) -> set[str]:
    suffixes = rule.get("suffixes")
    suffixes_from = rule.get("suffixes_from")
    if suffixes is not None and suffixes_from is not None:
        raise ValueError(
            f"field_prefix rule for {spec.name!r} cannot set both "
            "'suffixes' and 'suffixes_from'"
        )
    if suffixes_from is not None:
        if not isinstance(suffixes_from, str) or not suffixes_from.startswith("params."):
            raise ValueError(
                f"field_prefix suffixes_from for {spec.name!r} must reference params.*"
            )
        resolved = _param_field_names(
            params,
            source=suffixes_from,
            spec=spec,
            param_name="suffixes_from",
            optional=bool(rule.get("optional")),
        )
    else:
        if not isinstance(suffixes, list) or not all(
            isinstance(item, str) and item for item in suffixes
        ):
            raise ValueError(
                f"field_prefix rule for {spec.name!r} requires string suffixes"
            )
        resolved = {item.strip() for item in suffixes}
    exclude_from = rule.get("exclude_suffixes_from")
    if exclude_from is not None:
        if not isinstance(exclude_from, str) or not exclude_from.startswith("params."):
            raise ValueError(
                f"field_prefix exclude_suffixes_from for {spec.name!r} "
                "must reference params.*"
            )
        resolved -= _param_field_names(
            params,
            source=exclude_from,
            spec=spec,
            param_name="exclude_suffixes_from",
            optional=True,
        )
    return resolved


def _param_field_names(
    params: dict[str, Any],
    *,
    source: str,
    spec: RuntimeComponentSpec,
    param_name: str,
    optional: bool = False,
) -> set[str]:
    fields: set[str] = set()
    for value in _values_from_param_reference(params, source=source, spec=spec):
        if value is None or value is False:
            continue
        fields.update(_field_names(value, source=source, spec_name=spec.name))
    if not fields and not optional:
        raise ValueError(
            f"{param_name}={source!r} for {spec.name!r} did not resolve to fields"
        )
    return fields


def _rule_should_skip(
    rule: dict[str, Any],
    *,
    params: dict[str, Any],
    spec: RuntimeComponentSpec,
) -> bool:
    skip_if_false = rule.get("skip_if_false")
    if skip_if_false is None:
        return False
    if not isinstance(skip_if_false, str) or not skip_if_false.startswith("params."):
        raise ValueError(f"skip_if_false for {spec.name!r} must reference params.*")
    return any(
        value is False
        for value in _values_from_param_reference(
            params,
            source=skip_if_false,
            spec=spec,
        )
    )


def _expression_values(
    value: Any,
    *,
    source: str,
    spec: RuntimeComponentSpec,
) -> list[str]:
    if value is None or value is False:
        return []
    if isinstance(value, str):
        values = [value]
    elif isinstance(value, list):
        values = value
    else:
        raise TypeError(f"{source} for {spec.name!r} must contain expression strings")
    expressions: list[str] = []
    for item in values:
        if not isinstance(item, str) or not item.strip():
            raise ValueError(f"{source} for {spec.name!r} contains an invalid expression")
        expressions.append(item.strip())
    return expressions


def _scoped_expression_dependencies(
    expression: str,
    *,
    rule: dict[str, Any],
    params: dict[str, Any],
    spec: RuntimeComponentSpec,
    dep_ctx: DependencyContext,
) -> set[str]:
    symbols = data_symbols_in_expr(
        expression,
        known_functions=dep_ctx.known_functions,
        known_constants=dep_ctx.known_constants,
        context_symbols=dep_ctx.context_symbols,
        produced=set(),
    )
    allowed = {str(item) for item in list(rule.get("allowed") or [])}
    runtime_symbols = {str(item) for item in list(rule.get("runtime_symbols") or [])}
    symbol_prefixes = [str(item) for item in list(rule.get("symbol_prefixes") or [])]
    dependency = rule.get("dependency", "prefixes")

    dependencies: set[str] = set()
    for symbol in sorted(symbols):
        if symbol in runtime_symbols:
            continue
        if symbol in allowed:
            if dependency not in {"none", None}:
                raise ValueError(
                    f"scoped_expr symbol {symbol!r} for {spec.name!r} has no "
                    "declared dependency mapping"
                )
            continue
        matched_prefix = next(
            (prefix for prefix in symbol_prefixes if symbol.startswith(prefix)),
            None,
        )
        if matched_prefix is None:
            expected = sorted([*allowed, *(f"{item}<field>" for item in symbol_prefixes)])
            raise ValueError(
                f"Unsupported scoped expression symbol {symbol!r} for {spec.name!r}; "
                f"expected one of {expected}"
            )
        field = symbol.removeprefix(matched_prefix)
        if not field:
            raise ValueError(
                f"Scoped expression symbol {symbol!r} for {spec.name!r} "
                "does not include a field suffix"
            )
        prefixes_from = rule.get("prefixes_from")
        if not isinstance(prefixes_from, str) or not prefixes_from.startswith("params."):
            raise ValueError(
                f"scoped_expr rule for {spec.name!r} requires "
                "'prefixes_from' referencing params.*"
            )
        prefixes = _param_field_names(
            params,
            source=prefixes_from,
            spec=spec,
            param_name="prefixes_from",
        )
        dependencies.update(f"{prefix}_{field}" for prefix in prefixes)
    return dependencies


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
        if not isinstance(rule, dict):
            raise ValueError(
                f"provides.symbols[{index}] for {spec.name!r} must be a mapping"
            )
        kind = rule.get("kind")
        if kind not in {"field_list", "field_prefix", "count", "template"}:
            raise ValueError(
                f"Unsupported provides.symbols kind for {spec.name!r}: {kind!r}"
            )
        source = rule.get("from")
        if kind == "template":
            symbols.update(_template_provided_symbols(rule, params=params, spec=spec))
            continue
        if not isinstance(source, str) or not source.startswith("params."):
            raise ValueError(
                f"provides.symbols[{index}].from for {spec.name!r} must reference params.*"
            )
        if _rule_should_skip(rule, params=params, spec=spec):
            continue
        for value in _values_from_param_reference(params, source=source, spec=spec):
            if value is None or value is False:
                continue
            if kind == "field_list":
                symbols.update(_field_names(value, source=source, spec_name=spec.name))
            elif kind == "field_prefix":
                suffixes = _suffixes_for_rule(rule, params=params, spec=spec)
                symbols.update(
                    f"{prefix}_{suffix}"
                    for prefix in _provided_prefixes(
                        rule,
                        value,
                        source=source,
                        spec_name=spec.name,
                    )
                    for suffix in suffixes
                )
            elif kind == "count":
                symbols.update(
                    f"n{prefix}"
                    for prefix in _provided_prefixes(
                        rule,
                        value,
                        source=source,
                        spec_name=spec.name,
                    )
                )
    return symbols


def _provided_prefixes(
    rule: dict[str, Any],
    value: Any,
    *,
    source: str,
    spec_name: str,
) -> set[str]:
    prefixes = _field_names(value, source=source, spec_name=spec_name)
    suffix = rule.get("prefix_suffix")
    if suffix is None:
        return prefixes
    if not isinstance(suffix, str):
        raise ValueError(f"prefix_suffix for {spec_name!r} must be a string")
    return {f"{prefix}{suffix}" for prefix in prefixes}


def _template_provided_symbols(
    rule: dict[str, Any],
    *,
    params: dict[str, Any],
    spec: RuntimeComponentSpec,
) -> set[str]:
    when_true = rule.get("when_true")
    if when_true is not None:
        if not isinstance(when_true, str) or not when_true.startswith("params."):
            raise ValueError(f"template when_true for {spec.name!r} must reference params.*")
        try:
            values = _values_from_param_reference(params, source=when_true, spec=spec)
        except TypeError:
            return set()
        if not any(value is True for value in values):
            return set()

    condition = rule.get("unless_false")
    if condition is not None:
        if not isinstance(condition, str) or not condition.startswith("params."):
            raise ValueError(
                f"template unless_false for {spec.name!r} must reference params.*"
            )
        if any(
            value is False
            for value in _values_from_param_reference(params, source=condition, spec=spec)
        ):
            return set()

    source = rule.get("from")
    if source is not None:
        if not isinstance(source, str) or not source.startswith("params."):
            raise ValueError(f"template from for {spec.name!r} must reference params.*")
        values = _values_from_param_reference(params, source=source, spec=spec)
        resolved = {
            str(value).strip()
            for value in values
            if isinstance(value, str) and value.strip()
        }
        if resolved:
            return resolved
        if any(value is False or value is None for value in values):
            return set()

    template = rule.get("template")
    if not isinstance(template, str) or not template.strip():
        raise ValueError(f"template rule for {spec.name!r} requires a string template")
    return {_format_param_template(template, params=params, spec=spec)}


def _format_param_template(
    template: str,
    *,
    params: dict[str, Any],
    spec: RuntimeComponentSpec,
) -> str:
    values: dict[str, str] = {}
    for raw in template.split("{")[1:]:
        token = raw.split("}", 1)[0]
        if not token.startswith("params."):
            raise ValueError(
                f"Template token {token!r} for {spec.name!r} must reference params.*"
            )
        resolved = _values_from_param_reference(params, source=token, spec=spec)
        names = [
            str(value).strip()
            for value in resolved
            if isinstance(value, str) and value.strip()
        ]
        if len(names) != 1:
            raise ValueError(
                f"Template token {token!r} for {spec.name!r} must resolve to one string"
            )
        values[token] = names[0]
    out = template
    for token, value in values.items():
        out = out.replace("{" + token + "}", value)
    return out


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
