from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import Any

from hepflow.compiler.expr_symbols import data_symbols_in_expr
from hepflow.model.component_spec import RuntimeComponentSpec
from hepflow.model.data_flow import DataDependencyResult, DependencyContext
from hepflow.model.plan import ExecutionNode, ExecutionPlan, PlanInputRef
from hepflow.model.plan_applicability import (
    active_plan_nodes_for_dataset,
    inactive_inputs_behavior_for_node,
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

    has_dataset_applicability = any(
        isinstance(node.meta.get("applies_to"), dict) for node in plan.nodes
    )
    common = _analyze_stream_data_flow(
        plan=plan,
        nodes=plan.nodes,
        registry=registry,
        dep_ctx=dep_ctx,
        primary_stream=primary_stream,
        aliases_by_stream=aliases_by_stream,
        dataset=None,
    )

    datasets = (
        dict(plan.context.get("datasets") or {}) if has_dataset_applicability else {}
    )
    required_by_dataset: dict[str, dict[str, Any]] = {}
    lineage_by_dataset: dict[str, dict[str, dict[str, str]]] = {}
    input_fields_by_dataset: dict[str, dict[str, list[str]]] = {}
    for dataset_name, dataset in sorted(datasets.items()):
        dataset_flow = _analyze_stream_data_flow(
            plan=plan,
            nodes=active_plan_nodes_for_dataset(
                plan,
                dataset=dict(dataset or {}),
            ),
            registry=registry,
            dep_ctx=dep_ctx,
            primary_stream=primary_stream,
            aliases_by_stream=aliases_by_stream,
            dataset=dict(dataset or {}),
        )
        required_by_dataset[str(dataset_name)] = dataset_flow["required_sources"]
        lineage_by_dataset[str(dataset_name)] = dataset_flow["stream_lineage"]
        input_fields_by_dataset[str(dataset_name)] = dataset_flow[
            "input_fields_by_node"
        ]

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
        "origins": common["origins"],
        "input_fields_by_node": common["input_fields_by_node"],
        "input_fields_by_dataset": input_fields_by_dataset,
        "_stream_lineage": common["stream_lineage"],
        "_stream_lineage_by_dataset": lineage_by_dataset,
        "notes": notes,
    }


@dataclass(frozen=True, slots=True)
class StreamRef:
    node_id: str
    output_name: str = "stream"


@dataclass(frozen=True, slots=True)
class StreamLineage:
    identity: str


@dataclass(slots=True)
class StreamState:
    stream: StreamRef
    fields: list[str] = dataclass_field(default_factory=list)
    origins: dict[str, dict[str, Any]] = dataclass_field(default_factory=dict)
    lineage: StreamLineage | None = None


@dataclass(slots=True)
class StreamAnalysisResult:
    required_sources: dict[str, dict[str, list[str]]]
    consumers: dict[str, list[str]]
    origins: dict[str, dict[str, Any]]
    input_fields_by_node: dict[str, list[str]]
    stream_states: dict[StreamRef, StreamState]
    stream_lineage: dict[str, dict[str, str]]


def _analyze_stream_data_flow(
    *,
    plan: ExecutionPlan,
    nodes: list[ExecutionNode],
    registry: dict[str, Any],
    dep_ctx: DependencyContext,
    primary_stream: str,
    aliases_by_stream: dict[str, dict[str, str]],
    dataset: dict[str, Any] | None,
) -> dict[str, Any]:
    source_required_data: dict[str, set[str]] = defaultdict(set)
    source_required_branches: dict[str, set[str]] = defaultdict(set)
    consumers: dict[str, list[str]] = defaultdict(list)
    input_fields_by_node: dict[str, list[str]] = {}
    stream_states: dict[StreamRef, StreamState] = {}
    active_ids = {node.id for node in nodes}

    for node in nodes:
        if node.role == "source":
            state = _source_stream_state(
                plan=plan,
                node=node,
                dataset_name=_dataset_name(dataset),
                aliases_by_stream=aliases_by_stream,
            )
            stream_states[state.stream] = state
            continue

        if node.role not in {"transform", "sink"}:
            continue

        spec = component_spec_for_node(node, registry)
        if spec is not None:
            try:
                deps = parse_component_data_dependencies(
                    spec=spec,
                    params=_dependency_params_for_dataset(node.params, dataset=dataset),
                    dep_ctx=dep_ctx,
                )
            except Exception as exc:
                raise ValueError(
                    "Failed to parse data dependencies for "
                    f"node {node.id!r} ({node.impl}): {exc}"
                ) from exc
        else:
            deps = DataDependencyResult()

        stream_id = str(node.params.get("stream_id") or primary_stream)
        omit_inactive_inputs = inactive_inputs_behavior_for_node(plan, node) == "omit"
        input_state = _primary_input_stream_state(
            plan=plan,
            node=node,
            active_ids=active_ids,
            stream_states=stream_states,
            dataset=dataset,
            omit_inactive_inputs=omit_inactive_inputs,
        )
        input_states = _event_stream_input_states(
            plan=plan,
            node=node,
            active_ids=active_ids,
            stream_states=stream_states,
            dataset=dataset,
            omit_inactive_inputs=omit_inactive_inputs,
        )
        dependency_states = _event_stream_dependency_states(
            plan=plan,
            node=node,
            active_ids=active_ids,
            stream_states=stream_states,
            dataset=dataset,
            omit_inactive_inputs=omit_inactive_inputs,
        )
        input_fields = list(input_state.fields) if input_state is not None else []
        input_origins = dict(input_state.origins) if input_state is not None else {}
        input_fields_by_node[node.id] = input_fields
        effective_input_fields = list(input_fields)

        for consumed in sorted(deps.consumes):
            consumers[consumed].append(node.id)
            dependency_origin = _dependency_origin(consumed, dependency_states)
            if dependency_origin is not None:
                effective_input_fields = _merge_ordered_fields(
                    [effective_input_fields, [consumed]]
                )
                input_origins[consumed] = dependency_origin
                continue

            origin = input_origins.get(consumed)

            if (
                origin is not None
                and origin.get("kind") not in {"source", "alias"}
                and node.impl != "hep.project_fields"
            ):
                continue

            required_stream, branch = _required_source_for_consumed_field(
                consumed,
                origin=origin,
                node=node,
                stream_id=stream_id,
                primary_stream=primary_stream,
                aliases_by_stream=aliases_by_stream,
            )
            source_required_data[required_stream].add(consumed)
            source_required_branches[required_stream].add(branch)
            if consumed not in input_origins:
                effective_input_fields = _merge_ordered_fields(
                    [effective_input_fields, [consumed]]
                )
                input_origins[consumed] = {
                    "kind": "source",
                    "stream": required_stream,
                    "branch": branch,
                }

        if node.role == "sink":
            continue

        output_state = _transform_stream_state(
            node=node,
            spec=spec,
            input_state=input_state,
            input_states=input_states,
            input_fields=effective_input_fields,
            input_origins=input_origins,
            deps=deps,
            params=node.params,
            stream_id=stream_id,
            aliases_by_stream=aliases_by_stream,
        )
        stream_states[output_state.stream] = output_state

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

    result = StreamAnalysisResult(
        required_sources=required_sources,
        consumers=dict(sorted(consumers.items())),
        origins=_public_origins(stream_states),
        input_fields_by_node=input_fields_by_node,
        stream_states=stream_states,
        stream_lineage=_stream_lineage_view(stream_states),
    )
    return {
        "required_sources": result.required_sources,
        "consumers": result.consumers,
        "origins": result.origins,
        "input_fields_by_node": result.input_fields_by_node,
        "stream_states": result.stream_states,
        "stream_lineage": result.stream_lineage,
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
    required_by_dataset = (plan.data_flow or {}).get(
        "required_sources_by_dataset"
    ) or {}

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


def input_stream_fields_by_context(
    *,
    plan: ExecutionPlan,
    active_contexts: list[tuple[str | None, dict[str, Any] | None, set[str]]],
    registry: dict[str, Any],
    dep_ctx: DependencyContext,
) -> dict[str | None, dict[str, list[str]]]:
    cached = _cached_input_stream_fields_by_context(
        plan=plan,
        active_contexts=active_contexts,
    )
    if cached is not None:
        return cached

    input_fields_by_context: dict[str | None, dict[str, list[str]]] = {}
    primary_stream = _primary_stream_id(plan)
    aliases_by_stream = _aliases_by_stream(plan)
    for context_name, dataset, active_ids in active_contexts:
        analysis = _analyze_stream_data_flow(
            plan=plan,
            nodes=[node for node in plan.nodes if node.id in active_ids],
            registry=registry,
            dep_ctx=dep_ctx,
            primary_stream=primary_stream,
            aliases_by_stream=aliases_by_stream,
            dataset=dataset,
        )
        input_fields_by_context[context_name] = dict(analysis["input_fields_by_node"])
    return input_fields_by_context


def _cached_input_stream_fields_by_context(
    *,
    plan: ExecutionPlan,
    active_contexts: list[tuple[str | None, dict[str, Any] | None, set[str]]],
) -> dict[str | None, dict[str, list[str]]] | None:
    data_flow = plan.data_flow if isinstance(plan.data_flow, dict) else {}
    common = data_flow.get("input_fields_by_node")
    by_dataset = data_flow.get("input_fields_by_dataset")
    if not isinstance(common, dict):
        return None

    out: dict[str | None, dict[str, list[str]]] = {}
    for context_name, _dataset, _active_ids in active_contexts:
        if context_name is None:
            out[None] = {
                str(node_id): list(fields)
                for node_id, fields in common.items()
                if isinstance(fields, list)
            }
            continue
        if not isinstance(by_dataset, dict):
            return None
        context_fields = by_dataset.get(context_name)
        if not isinstance(context_fields, dict):
            return None
        out[context_name] = {
            str(node_id): list(fields)
            for node_id, fields in context_fields.items()
            if isinstance(fields, list)
        }
    return out


def _dataset_name(dataset: dict[str, Any] | None) -> str | None:
    if not isinstance(dataset, dict):
        return None
    name = dataset.get("name")
    return str(name) if name is not None else None


def _source_stream_state(
    *,
    plan: ExecutionPlan,
    node: ExecutionNode,
    dataset_name: str | None,
    aliases_by_stream: dict[str, dict[str, str]],
) -> StreamState:
    fields = _source_output_fields(plan, node, dataset_name=dataset_name)
    source_name = str(node.meta.get("source_name") or node.id.removeprefix("read."))
    origins = {
        field: {
            "kind": "source",
            "stream": source_name,
            "branch": _resolve_required_branch(
                field,
                stream_id=source_name,
                aliases_by_stream=aliases_by_stream,
            ),
        }
        for field in fields
    }
    for alias, branch in aliases_by_stream.get(source_name, {}).items():
        origins[alias] = {"kind": "alias", "stream": source_name, "branch": branch}
    stream = StreamRef(node.id, "stream")
    return StreamState(
        stream=stream,
        fields=fields,
        origins=origins,
        lineage=_new_lineage(stream, source=True),
    )


def _primary_input_stream_state(
    *,
    plan: ExecutionPlan,
    node: ExecutionNode,
    active_ids: set[str],
    stream_states: dict[StreamRef, StreamState],
    dataset: dict[str, Any] | None,
    omit_inactive_inputs: bool,
) -> StreamState | None:
    event_stream_input_count = _event_stream_input_count(plan, node)
    for ref in node.inputs:
        if ref.node_id in active_ids:
            active_ref = ref
        else:
            if omit_inactive_inputs:
                continue
            if event_stream_input_count > 1:
                raise ValueError(
                    f"node {node.id!r} has inactive required input "
                    f"{ref.node_id!r}; declare input.inactive_inputs: omit "
                    "to allow contextual omission"
                )
            active_ref = resolve_active_input_ref(plan, ref, dataset=dataset)
        if active_ref.node_id not in active_ids:
            continue
        if active_ref.input_name == "dependency":
            continue
        if active_ref.output_name != "stream":
            continue
        return stream_states.get(_stream_ref(active_ref))
    return None


def _event_stream_input_states(
    *,
    plan: ExecutionPlan,
    node: ExecutionNode,
    active_ids: set[str],
    stream_states: dict[StreamRef, StreamState],
    dataset: dict[str, Any] | None,
    omit_inactive_inputs: bool,
) -> list[StreamState]:
    states: list[StreamState] = []
    seen: set[StreamRef] = set()
    event_stream_input_count = _event_stream_input_count(plan, node)
    for ref in node.inputs:
        if ref.node_id in active_ids:
            active_ref = ref
        else:
            if omit_inactive_inputs:
                continue
            if event_stream_input_count > 1:
                raise ValueError(
                    f"node {node.id!r} has inactive required input "
                    f"{ref.node_id!r}; declare input.inactive_inputs: omit "
                    "to allow contextual omission"
                )
            active_ref = resolve_active_input_ref(plan, ref, dataset=dataset)
        if active_ref.node_id not in active_ids:
            continue
        if active_ref.input_name == "dependency":
            continue
        if active_ref.output_name != "stream":
            continue
        stream_ref = _stream_ref(active_ref)
        if stream_ref in seen:
            continue
        state = stream_states.get(stream_ref)
        if state is None:
            continue
        states.append(state)
        seen.add(stream_ref)
    return states


def _event_stream_dependency_states(
    *,
    plan: ExecutionPlan,
    node: ExecutionNode,
    active_ids: set[str],
    stream_states: dict[StreamRef, StreamState],
    dataset: dict[str, Any] | None,
    omit_inactive_inputs: bool,
) -> list[StreamState]:
    states: list[StreamState] = []
    seen: set[StreamRef] = set()
    for ref in node.inputs:
        if ref.input_name != "dependency":
            continue
        active_ref = ref
        if ref.node_id not in active_ids:
            if omit_inactive_inputs:
                continue
            active_ref = resolve_active_input_ref(plan, ref, dataset=dataset)
        if active_ref.node_id not in active_ids or active_ref.output_name != "stream":
            continue
        stream_ref = _stream_ref(active_ref)
        if stream_ref in seen:
            continue
        state = stream_states.get(stream_ref)
        if state is None:
            continue
        states.append(state)
        seen.add(stream_ref)
    return states


def _dependency_origin(
    field: str,
    dependency_states: list[StreamState],
) -> dict[str, Any] | None:
    origins = [
        dict(state.origins[field])
        for state in dependency_states
        if field in state.origins
        and state.origins[field].get("kind") not in {"source", "alias"}
    ]
    if not origins:
        return None
    unique: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()
    for origin in origins:
        key = origin_key(origin)
        if key in seen:
            continue
        seen.add(key)
        unique.append(origin)
    if len(unique) == 1:
        return unique[0]
    return {
        "kind": "stream_scoped",
        "origins": unique,
    }


def _event_stream_input_count(plan: ExecutionPlan, node: ExecutionNode) -> int:
    return sum(
        1
        for ref in node.inputs
        if ref.input_name != "dependency"
        and plan.get_node(ref.node_id).outputs.get(ref.output_name) == "event_stream"
    )


def _stream_ref(ref: PlanInputRef) -> StreamRef:
    return StreamRef(node_id=ref.node_id, output_name=ref.output_name)


def _transform_stream_state(
    *,
    node: ExecutionNode,
    spec: RuntimeComponentSpec | None,
    input_state: StreamState | None,
    input_states: list[StreamState],
    input_fields: list[str],
    input_origins: dict[str, dict[str, Any]],
    deps: DataDependencyResult,
    params: dict[str, Any],
    stream_id: str,
    aliases_by_stream: dict[str, dict[str, str]],
) -> StreamState:
    field_behavior = _field_propagation_behavior(spec)
    if field_behavior == "merge":
        input_fields, input_origins = _merge_stream_fields_and_origins(
            node=node,
            input_states=input_states,
        )
    output_fields = _transform_output_fields(
        node=node,
        spec=spec,
        input_fields=input_fields,
        input_states=input_states,
        deps=deps,
        params=params,
    )
    origins = {
        field: dict(input_origins[field])
        for field in output_fields
        if field in input_origins
    }
    for produced in sorted(deps.produces):
        if produced not in output_fields:
            continue
        if node.impl == "hep.project_fields" and produced in aliases_by_stream.get(
            stream_id, {}
        ):
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
    for field in output_fields:
        origins.setdefault(
            field,
            {
                "kind": "preserved",
                "node": node.id,
                "stream": {"node_id": node.id, "output_name": "stream"},
            },
        )
    stream = StreamRef(node.id, "stream")
    lineage = _output_lineage(
        stream=stream,
        input_state=input_state,
        input_states=input_states,
        behavior=_lineage_behavior(spec),
    )
    return StreamState(
        stream=stream,
        fields=output_fields,
        origins=origins,
        lineage=lineage,
    )


def _lineage_behavior(spec: RuntimeComponentSpec | None) -> str:
    raw = _result_metadata(spec).get("lineage") if spec is not None else None
    if raw is None:
        return "preserve"
    behavior = str(raw)
    if behavior in {"preserve", "require_equal", "new", "source"}:
        return behavior
    name = spec.name if spec is not None else "<unknown>"
    raise ValueError(f"Unsupported event-stream lineage behavior for {name!r}: {raw!r}")


def _field_propagation_behavior(spec: RuntimeComponentSpec | None) -> str:
    raw = _result_metadata(spec).get("field_propagation") if spec is not None else None
    if raw is None:
        return "ordinary"
    behavior = str(raw)
    if behavior in {"ordinary", "projection", "merge"}:
        return behavior
    name = spec.name if spec is not None else "<unknown>"
    raise ValueError(
        f"Unsupported event-stream field propagation behavior for {name!r}: {raw!r}"
    )


def _result_metadata(spec: RuntimeComponentSpec | None) -> dict[str, Any]:
    if spec is None:
        return {}
    result = spec.result or {}
    stream_result = result.get("stream")
    if isinstance(stream_result, dict):
        return stream_result
    if result.get("kind") == "event_stream":
        return result
    return {}


def _output_lineage(
    *,
    stream: StreamRef,
    input_state: StreamState | None,
    input_states: list[StreamState],
    behavior: str,
) -> StreamLineage:
    if behavior == "require_equal":
        lineages = [
            state.lineage for state in input_states if state.lineage is not None
        ]
        if not lineages:
            return _new_lineage(stream)
        first = lineages[0]
        if any(lineage.identity != first.identity for lineage in lineages[1:]):
            raise ValueError(
                f"Node {stream.node_id!r} cannot merge incompatible event-stream lineages"
            )
        return first
    if behavior == "preserve" and input_state is not None and input_state.lineage:
        return input_state.lineage
    return _new_lineage(stream, source=behavior == "source")


def _new_lineage(stream: StreamRef, *, source: bool = False) -> StreamLineage:
    prefix = "source" if source else "stream"
    return StreamLineage(f"{prefix}:{stream.node_id}:{stream.output_name}")


def _required_source_for_consumed_field(
    consumed: str,
    *,
    origin: dict[str, Any] | None,
    node: ExecutionNode,
    stream_id: str,
    primary_stream: str,
    aliases_by_stream: dict[str, dict[str, str]],
) -> tuple[str, str]:
    if origin is not None and origin.get("kind") in {"source", "alias"}:
        origin_stream = origin.get("stream")
        origin_branch = origin.get("branch")
        if isinstance(origin_stream, str) and isinstance(origin_branch, str):
            return origin_stream, origin_branch

    required_stream = stream_id if node.impl == "hep.project_fields" else primary_stream
    return (
        required_stream,
        _resolve_required_branch(
            consumed,
            stream_id=required_stream,
            aliases_by_stream=aliases_by_stream,
        ),
    )


def _stream_lineage_view(
    stream_states: dict[StreamRef, StreamState],
) -> dict[str, dict[str, str]]:
    return {
        _stream_key(stream): {"identity": state.lineage.identity}
        for stream, state in sorted(
            stream_states.items(),
            key=lambda item: _stream_key(item[0]),
        )
        if state.lineage is not None
    }


def _stream_key(stream: StreamRef) -> str:
    return f"{stream.node_id}:{stream.output_name}"


def _public_origins(
    stream_states: dict[StreamRef, StreamState],
) -> dict[str, dict[str, Any]]:
    by_field: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for state in stream_states.values():
        for field, origin in state.origins.items():
            by_field[field].append(dict(origin))

    public: dict[str, dict[str, Any]] = {}
    for field, origins in sorted(by_field.items()):
        unique: list[dict[str, Any]] = []
        seen: set[tuple[Any, ...]] = set()
        for origin in origins:
            key = origin_key(origin)
            if key in seen:
                continue
            seen.add(key)
            unique.append(origin)
        if not unique:
            continue
        if len(unique) == 1:
            public[field] = unique[0]
            continue
        public[field] = {
            "kind": "stream_scoped",
            "origins": unique,
        }
    return public


def origin_key(origin: dict[str, Any]) -> tuple[Any, ...]:
    return _origin_value_key(origin)


def _origin_value_key(value: Any) -> tuple[Any, ...]:
    if isinstance(value, dict):
        return (
            "dict",
            tuple(
                (str(key), _origin_value_key(item))
                for key, item in sorted(value.items(), key=lambda entry: str(entry[0]))
            ),
        )
    if isinstance(value, list):
        return ("list", tuple(_origin_value_key(item) for item in value))
    if isinstance(value, tuple):
        return ("tuple", tuple(_origin_value_key(item) for item in value))
    return ("scalar", type(value).__name__, value)


def _source_output_fields(
    plan: ExecutionPlan,
    node: ExecutionNode,
    *,
    dataset_name: str | None,
) -> list[str]:
    inferred = _inferred_source_fields(
        plan=plan,
        node=node,
        dataset_name=dataset_name,
    )
    by_dataset = node.params.get("branches_by_dataset")
    authored: list[str] = []
    if dataset_name is not None and isinstance(by_dataset, dict):
        fields = by_dataset.get(dataset_name)
        if isinstance(fields, list):
            authored = _literal_fields(_string_list(fields))
            return _merge_ordered_fields([authored, inferred])
    for key in ("branches", "fields"):
        fields = node.params.get(key)
        if isinstance(fields, list):
            authored = _literal_fields(_string_list(fields))
            break
    return _merge_ordered_fields([authored, inferred])


def _inferred_source_fields(
    *,
    plan: ExecutionPlan,
    node: ExecutionNode,
    dataset_name: str | None,
) -> list[str]:
    source_name = str(node.meta.get("source_name") or node.id.removeprefix("read."))
    data_flow = plan.data_flow if isinstance(plan.data_flow, dict) else {}
    required_by_dataset = data_flow.get("required_sources_by_dataset")
    if dataset_name is not None and isinstance(required_by_dataset, dict):
        dataset_required = required_by_dataset.get(dataset_name)
        if isinstance(dataset_required, dict):
            return _required_source_branches(dataset_required.get(source_name))
        return []

    required_sources = data_flow.get("required_sources")
    if isinstance(required_sources, dict):
        return _required_source_branches(required_sources.get(source_name))
    return []


def _required_source_branches(required: Any) -> list[str]:
    if not isinstance(required, dict):
        return []
    return _literal_fields(_string_list(required.get("branches")))


def _merge_stream_fields_and_origins(
    *,
    node: ExecutionNode,
    input_states: list[StreamState],
) -> tuple[list[str], dict[str, dict[str, Any]]]:
    on_conflict = str(node.params.get("on_conflict") or "keep_first")
    if on_conflict not in {"keep_first", "keep_last", "error"}:
        raise ValueError(
            f"Unsupported event-stream merge conflict policy for {node.id!r}: "
            f"{on_conflict!r}"
        )
    fields: list[str] = []
    origins: dict[str, dict[str, Any]] = {}
    seen: set[str] = set()
    for state in input_states:
        for field in state.fields:
            origin = state.origins.get(field)
            if field in seen:
                if on_conflict == "error":
                    raise ValueError(
                        f"Node {node.id!r} cannot merge duplicate event-stream "
                        f"field {field!r}"
                    )
                if on_conflict == "keep_first":
                    continue
                if origin is not None:
                    origins[field] = dict(origin)
                continue
            fields.append(field)
            seen.add(field)
            if origin is not None:
                origins[field] = dict(origin)
    return fields, origins


def _transform_output_fields(
    *,
    node: ExecutionNode,
    spec: RuntimeComponentSpec | None,
    input_fields: list[str],
    input_states: list[StreamState],
    deps: DataDependencyResult,
    params: dict[str, Any],
) -> list[str]:
    field_behavior = _field_propagation_behavior(spec)
    if field_behavior == "merge":
        return _merge_stream_fields_and_origins(
            node=node,
            input_states=input_states,
        )[0]
    if node.impl == "hep.project_fields":
        raw_aliases = params.get("aliases") or {}
        aliases = raw_aliases if isinstance(raw_aliases, dict) else {}
        if (
            params.get("include_existing", True) is False
            or field_behavior == "projection"
        ):
            return [str(alias) for alias in aliases if isinstance(alias, str)]
        return _merge_ordered_fields(
            [input_fields, [str(alias) for alias in aliases if isinstance(alias, str)]]
        )
    if node.impl == "hep.align_schema":
        return _align_schema_output_fields(input_fields=input_fields, params=params)
    if field_behavior == "projection":
        return sorted(deps.produces)
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


def _is_glob_pattern(value: str) -> bool:
    return any(char in value for char in "*?[")


def _literal_fields(fields: list[str]) -> list[str]:
    return [field for field in fields if not _is_glob_pattern(field)]


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


def component_spec_for_node(
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
    source_parts = source.split(".")
    root_param = source_parts[1] if len(source_parts) > 1 else None
    for depth, segment in enumerate(source_parts[1:]):
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
                schema = spec.params.get(root_param) if root_param else None
                if isinstance(schema, dict) and schema.get("hooks"):
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
        if not isinstance(suffixes_from, str) or not suffixes_from.startswith(
            "params."
        ):
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
            raise ValueError(
                f"{source} for {spec.name!r} contains an invalid expression"
            )
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
            expected = sorted(
                [*allowed, *(f"{item}<field>" for item in symbol_prefixes)]
            )
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
        if not isinstance(prefixes_from, str) or not prefixes_from.startswith(
            "params."
        ):
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
            raise ValueError(
                f"template when_true for {spec.name!r} must reference params.*"
            )
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
            for value in _values_from_param_reference(
                params, source=condition, spec=spec
            )
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
        raw_aliases = node.params.get("aliases") or {}
        if not isinstance(raw_aliases, dict):
            continue
        for alias, branch in raw_aliases.items():
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
