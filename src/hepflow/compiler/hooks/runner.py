from __future__ import annotations

from typing import Any

from hepflow.compiler.hooks.model import (
    CompileHookContext,
    CompileHookResult,
    ParamCompileHookContext,
)
from hepflow.compiler.hooks.registry import (
    phase_compile_hook_entries,
    resolve_compile_hook,
)
from hepflow.model.component_spec import RuntimeComponentSpec
from hepflow.model.plan import ExecutionNode


def run_phase_hooks(
    *,
    registry: dict[str, Any] | None,
    context: CompileHookContext,
    when: str,
) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for name, entry, _spec in phase_compile_hook_entries(registry, when=when):
        impl_ref = str(entry["impl"])
        try:
            hook = resolve_compile_hook(registry, name, kind="phase")
            params = dict(entry.get("params") or {})
            result = hook.impl(context, **params)
        except Exception as exc:
            raise RuntimeError(
                f"Compile hook {name!r} failed during {when!r} "
                f"using {impl_ref!r}: {exc}"
            ) from exc

        if result is None:
            continue
        if not isinstance(result, dict):
            raise TypeError(
                f"Compile hook {name!r} during {when!r} returned "
                f"{type(result).__name__}; expected a mapping of artifact names to data"
            )
        for artifact_name, artifact_data in result.items():
            if not isinstance(artifact_name, str) or not artifact_name.strip():
                raise ValueError(
                    f"Compile hook {name!r} returned invalid artifact name: "
                    f"{artifact_name!r}"
                )
            out[artifact_name.strip()] = artifact_data
            context.artifacts[artifact_name.strip()] = artifact_data
    return out


def run_parameter_hook_chains_for_node(
    *,
    node: ExecutionNode,
    spec: RuntimeComponentSpec,
    registry: dict[str, Any],
    input_fields_by_context: dict[str | None, dict[str, list[str]]],
) -> dict[str, list[dict[str, Any]]]:
    contexts = {
        context_name: fields_by_node.get(node.id, [])
        for context_name, fields_by_node in input_fields_by_context.items()
        if node.id in fields_by_node
    }
    if not contexts:
        return {}

    provenance_by_param: dict[str, list[dict[str, Any]]] = {}
    for param_name, schema in dict(spec.params or {}).items():
        if not isinstance(schema, dict):
            continue
        hooks = param_hook_chain(schema, param_name=param_name, spec_name=spec.name)
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
                result = run_single_parameter_hook(
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

        selected_value, selected_provenance = require_identical_param_hook_outputs(
            node=node,
            param_name=param_name,
            context_outputs=context_outputs,
        )
        node.params[param_name] = selected_value
        provenance_by_param[param_name] = selected_provenance
    return provenance_by_param


def param_hook_chain(
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


def run_single_parameter_hook(
    *,
    value: Any,
    hook_options: dict[str, Any],
    context: ParamCompileHookContext,
    registry: dict[str, Any],
    param_name: str,
    spec_name: str,
) -> CompileHookResult:
    hook_name = str(hook_options["name"])
    hook = resolve_compile_hook(registry, hook_name, kind="parameter")
    try:
        result = hook.impl(
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


def require_identical_param_hook_outputs(
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
