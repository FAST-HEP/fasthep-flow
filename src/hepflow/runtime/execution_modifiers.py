from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

from hepflow.registry.loaders import load_object, load_runtime_entry
from hepflow.runtime.handlers import (
    call_transform_impl,
    load_transform_spec_and_impl,
    validate_transform_call,
)


class ExecutionModifier(Protocol):
    def before(
        self,
        *,
        node: Any,
        stream: Any,
        inputs: dict[str, Any],
        params: dict[str, Any],
        ctx: dict[str, Any],
    ) -> tuple[Any, dict[str, Any]] | None: ...

    def wrap(
        self,
        *,
        node: Any,
        func: Callable[..., Any],
        params: dict[str, Any],
        ctx: dict[str, Any],
    ) -> Callable[..., Any]: ...

    def after(
        self,
        *,
        node: Any,
        result: Any,
        params: dict[str, Any],
        ctx: dict[str, Any],
    ) -> Any: ...


@dataclass(slots=True, frozen=True)
class ExecutionModifierHooks:
    before: Callable[..., Any] | None = None
    wrap: Callable[..., Any] | None = None
    after: Callable[..., Any] | None = None


@dataclass(slots=True, frozen=True)
class ResolvedExecutionModifier:
    name: str
    params: dict[str, Any]
    hooks: ExecutionModifierHooks


def run_transform_with_execution_modifiers(
    *,
    node: Any,
    inputs: dict[str, Any],
    params: dict[str, Any],
    registry_cfg: dict[str, Any] | None,
    ctx: dict[str, Any] | None = None,
) -> Any:
    modifiers = resolve_execution_modifiers_for_node(
        node,
        registry_cfg=registry_cfg,
    )
    if not modifiers:
        spec, impl = load_transform_spec_and_impl(registry_cfg, node.impl)
        validate_transform_call(spec=spec, inputs=inputs, params=params)
        return call_transform_impl(
            impl=impl,
            inputs=inputs,
            params=params,
            ctx=ctx,
        )

    spec, impl = load_transform_spec_and_impl(registry_cfg, node.impl)
    working_inputs = dict(inputs)
    working_params = dict(params)
    validate_transform_call(spec=spec, inputs=working_inputs, params=working_params)

    modifier_ctx = _modifier_context(node=node, ctx=ctx)
    stream_input_name = _primary_stream_input_name(working_inputs)

    for modifier in modifiers:
        before = modifier.hooks.before
        if before is None:
            continue
        try:
            result = before(
                node=node,
                stream=working_inputs.get(stream_input_name),
                inputs=working_inputs,
                params=working_params,
                ctx=modifier_ctx,
            )
        except Exception as exc:
            raise _modifier_error(node, modifier.name, "before", exc) from exc
        if result is not None:
            stream, next_params = _normalize_before_result(
                result,
                node=node,
                modifier_name=modifier.name,
            )
            working_inputs[stream_input_name] = stream
            working_params = dict(next_params)

    def operation(
        *,
        inputs: dict[str, Any],
        params: dict[str, Any],
        ctx: dict[str, Any],
    ) -> Any:
        return call_transform_impl(
            impl=impl,
            inputs=inputs,
            params=params,
            ctx=ctx,
        )

    func: Callable[..., Any] = operation
    for modifier in modifiers:
        wrap = modifier.hooks.wrap
        if wrap is None:
            continue
        try:
            wrapped = wrap(
                node=node,
                func=func,
                params=modifier.params,
                ctx=modifier_ctx,
            )
        except Exception as exc:
            raise _modifier_error(node, modifier.name, "wrap", exc) from exc
        if not callable(wrapped):
            raise TypeError(
                f"Execution modifier {modifier.name!r} wrap phase for node "
                f"{node.id} must return a callable"
            )
        func = wrapped

    try:
        result = func(inputs=working_inputs, params=working_params, ctx=modifier_ctx)
    except Exception:
        raise

    for modifier in reversed(modifiers):
        after = modifier.hooks.after
        if after is None:
            continue
        try:
            next_result = after(
                node=node,
                result=result,
                params=modifier.params,
                ctx=modifier_ctx,
            )
        except Exception as exc:
            raise _modifier_error(node, modifier.name, "after", exc) from exc
        if next_result is not None:
            result = next_result

    return result


def resolve_execution_modifiers_for_node(
    node: Any,
    *,
    registry_cfg: dict[str, Any] | None,
) -> list[ResolvedExecutionModifier]:
    node_execution = dict((node.meta or {}).get("execution") or {})
    modifier_specs = list(node_execution.get("modifiers") or [])
    resolved: list[ResolvedExecutionModifier] = []
    for raw in modifier_specs:
        if not isinstance(raw, dict):
            raise TypeError(
                f"Execution modifier metadata for node {node.id} must be mappings"
            )
        name = str(raw.get("name") or "")
        params = dict(raw.get("params") or {})
        resolved.append(
            ResolvedExecutionModifier(
                name=name,
                params=params,
                hooks=resolve_execution_modifier_hooks(
                    name,
                    node=node,
                    registry_cfg=registry_cfg,
                ),
            )
        )
    return resolved


def resolve_execution_modifier_hooks(
    name: str,
    *,
    node: Any,
    registry_cfg: dict[str, Any] | None,
) -> ExecutionModifierHooks:
    try:
        entry = load_runtime_entry(registry_cfg, "execution_modifiers", name)
    except Exception as exc:
        raise RuntimeError(
            f"Execution modifier {name!r} is not registered for node {node.id}"
        ) from exc

    impl_ref = entry.get("impl")
    if not isinstance(impl_ref, str) or ":" not in impl_ref:
        raise RuntimeError(
            f"Execution modifier {name!r} for node {node.id} must define "
            "string 'impl' as 'module:object'"
        )
    try:
        impl = load_object(impl_ref)
    except Exception as exc:
        raise RuntimeError(
            f"Execution modifier {name!r} implementation could not be loaded "
            f"for node {node.id}: {exc}"
        ) from exc
    if isinstance(impl, type):
        impl = impl()
    return _modifier_hooks_from_impl(name, node=node, impl=impl)


def _modifier_hooks_from_impl(
    name: str,
    *,
    node: Any,
    impl: Any,
) -> ExecutionModifierHooks:
    if isinstance(impl, dict):
        before = impl.get("before")
        wrap = impl.get("wrap")
        after = impl.get("after")
    else:
        before = getattr(impl, "before", None)
        wrap = getattr(impl, "wrap", None)
        after = getattr(impl, "after", None)

    hooks = ExecutionModifierHooks(
        before=_optional_callable(before, name, node, "before"),
        wrap=_optional_callable(wrap, name, node, "wrap"),
        after=_optional_callable(after, name, node, "after"),
    )
    if hooks.before is None and hooks.wrap is None and hooks.after is None:
        raise TypeError(
            f"Execution modifier {name!r} for node {node.id} must define at "
            "least one callable hook: before, wrap, or after"
        )
    return hooks


def _optional_callable(
    hook: Any,
    name: str,
    node: Any,
    phase: str,
) -> Callable[..., Any] | None:
    if hook is None:
        return None
    if not callable(hook):
        raise TypeError(
            f"Execution modifier {name!r} {phase} hook for node {node.id} "
            "must be callable"
        )
    return hook


def _normalize_before_result(
    result: Any,
    *,
    node: Any,
    modifier_name: str,
) -> tuple[Any, dict[str, Any]]:
    if (
        not isinstance(result, tuple)
        or len(result) != 2
        or not isinstance(result[1], dict)
    ):
        raise TypeError(
            f"Execution modifier {modifier_name!r} before phase for node "
            f"{node.id} must return (stream, params)"
        )
    return result[0], dict(result[1])


def _primary_stream_input_name(inputs: dict[str, Any]) -> str:
    if "stream" in inputs:
        return "stream"
    try:
        return next(iter(inputs))
    except StopIteration:
        return "stream"


def _modifier_context(
    *,
    node: Any,
    ctx: dict[str, Any] | None,
) -> dict[str, Any]:
    modifier_ctx = dict(ctx or {})
    modifier_ctx["node_id"] = node.id
    modifier_ctx["execution"] = dict((node.meta or {}).get("execution") or {})
    return modifier_ctx


def _modifier_error(
    node: Any,
    modifier_name: str,
    phase: str,
    exc: Exception,
) -> RuntimeError:
    return RuntimeError(
        f"Execution modifier {modifier_name!r} failed during {phase} phase "
        f"for node {node.id}: {exc}"
    )
