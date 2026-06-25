from __future__ import annotations

from contextlib import ExitStack, contextmanager
from typing import Any

from hepflow.model.lifecycle import WHEN_ALIASES
from hepflow.registry.defaults import (
    default_runtime_registry_config,
    merge_registry_config,
)
from hepflow.registry.loaders import load_object, load_runtime_entry
from hepflow.runtime.hooks.loaders import (
    hook_spec_events,
    load_hook_impl,
    load_hook_spec,
)


class HookDispatchError(RuntimeError):
    def __init__(
        self,
        *,
        kind: str,
        event: str,
        cause: BaseException,
        source: str = "execution_hook",
        node_id: str | None = None,
    ) -> None:
        self.kind = kind
        self.event = event
        self.cause = cause
        self.source = source
        self.node_id = node_id
        node_text = f" for node {node_id}" if node_id else ""
        super().__init__(
            f"Error {source} {kind!r} failed during {event}{node_text}: {cause}"
        )


class HookManager:
    """
    Execution lifecycle plugin dispatcher.

    Author-facing execution hooks and execution modifiers are distinct concepts,
    but internally they share the same ordered node lifecycle. Lifecycle plugins
    receive mutable runtime objects. Observer-style hooks should treat
    inputs/outputs/ctx as read-only; execution modifiers may intentionally mutate
    inputs, outputs, or ctx as part of their documented contract.
    """

    def __init__(
        self,
        hooks: list[tuple[Any, set[str]]] | None = None,
        *,
        registry_cfg: dict[str, Any] | None = None,
    ) -> None:
        self.hooks: list[dict[str, Any]] = []
        self.registry_cfg = dict(registry_cfg or {})
        self._node_modifier_bindings: dict[str, list[dict[str, Any]]] = {}
        for index, item in enumerate(list(hooks or [])):
            hook, events = item
            normalized_events = {_normalize_hook_event(event) for event in events}
            kind = f"{type(hook).__module__}.{type(hook).__name__}"
            self.hooks.append(
                {
                    "hook": hook,
                    "events": normalized_events,
                    "spec": {
                        "kind": kind,
                        "events": sorted(normalized_events),
                        "source": "execution_hook",
                    },
                    "calls": 0,
                    "index": index,
                    "order": 100,
                    "source": "execution_hook",
                    "scope": "global",
                    "mutates": False,
                }
            )

    @classmethod
    def from_plan(cls, plan) -> HookManager:
        registry_cfg = merge_registry_config(
            default_runtime_registry_config(),
            plan.registry or {},
        )
        hooks: list[tuple[Any, set[str]]] = []
        enabled_specs: list[dict[str, Any]] = []
        for hook_cfg in list(getattr(plan, "execution_hooks", []) or []):
            if not isinstance(hook_cfg, dict):
                continue
            kind = str(hook_cfg.get("kind") or "")
            if not kind:
                continue
            hook_spec = load_hook_spec(registry_cfg, kind)
            configured_events = {
                _normalize_hook_event(event)
                for event in list(hook_cfg.get("events") or [])
            }
            supported_events = {
                _normalize_hook_event(event)
                for event in hook_spec_events(hook_spec)
            }
            if not configured_events:
                configured_events = supported_events
            unsupported = configured_events - supported_events
            if unsupported:
                event = sorted(unsupported)[0]
                raise ValueError(f"Hook {kind} does not support event {event}")

            impl = load_hook_impl(registry_cfg, kind)
            params = _hook_params(hook_cfg)
            hook = impl(**params) if isinstance(impl, type) else impl
            hooks.append((hook, configured_events))
            enabled_specs.append(
                {
                    **dict(hook_cfg),
                    "events": sorted(configured_events),
                }
            )
        manager = cls(hooks, registry_cfg=registry_cfg)
        for binding, spec in zip(manager.hooks, enabled_specs, strict=False):
            binding["spec"] = {
                "source": "execution_hook",
                "scope": "global",
                **dict(spec),
            }
        return manager

    def _dispatch(
        self,
        event: str,
        *,
        reverse: bool = False,
        node: Any = None,
        **kwargs: Any,
    ) -> None:
        bindings = self._bindings_for_event(event, node=node)
        if reverse:
            bindings = list(reversed(bindings))
        call_kwargs = {"node": node, **kwargs} if node is not None else kwargs
        for binding in bindings:
            hook = binding["hook"]
            events = binding["events"]
            if events and event not in events:
                continue
            method = getattr(hook, event, None)
            if method is not None:
                binding["calls"] += 1
                try:
                    _apply_binding_params(binding, call_kwargs)
                    method(**call_kwargs)
                except Exception as exc:
                    spec = dict(binding.get("spec") or {})
                    kind = str(spec.get("kind") or type(hook).__name__)
                    raise HookDispatchError(
                        kind=kind,
                        event=event,
                        cause=exc,
                        source=str(spec.get("source") or binding.get("source")),
                        node_id=_node_id(node),
                    ) from exc

    def has_event(self, event: str, *, node: Any = None) -> bool:
        for binding in self._bindings_for_event(event, node=node):
            events = binding["events"]
            hook = binding["hook"]
            if events and event not in events:
                continue
            if getattr(hook, event, None) is not None:
                return True
        return False

    @contextmanager
    def around_node(self, *, node, inputs: dict[str, Any], ctx: dict[str, Any]):
        with ExitStack() as stack:
            for binding in self._bindings_for_event("around_node", node=node):
                hook = binding["hook"]
                events = binding["events"]
                if events and "around_node" not in events:
                    continue
                method = getattr(hook, "around_node", None)
                if method is None:
                    continue
                binding["calls"] += 1
                try:
                    _apply_binding_params(
                        binding,
                        {"node": node, "inputs": inputs, "ctx": ctx},
                    )
                    stack.enter_context(method(node=node, inputs=inputs, ctx=ctx))
                except Exception as exc:
                    spec = dict(binding.get("spec") or {})
                    kind = str(spec.get("kind") or type(hook).__name__)
                    raise HookDispatchError(
                        kind=kind,
                        event="around_node",
                        cause=exc,
                        source=str(spec.get("source") or binding.get("source")),
                        node_id=_node_id(node),
                    ) from exc
            yield

    def partition_start(self, *, partition, ctx: dict[str, Any]) -> None:
        self._dispatch("partition_start", partition=partition, ctx=ctx)

    def before_node(self, *, node, inputs: dict[str, Any], ctx: dict[str, Any]) -> None:
        self._dispatch("before_node", node=node, inputs=inputs, ctx=ctx)

    def after_node(
        self,
        *,
        node,
        inputs: dict[str, Any],
        outputs: Any,
        ctx: dict[str, Any],
    ) -> None:
        self._dispatch(
            "after_node",
            reverse=True,
            node=node,
            inputs=inputs,
            outputs=outputs,
            ctx=ctx,
        )

    def on_node_error(
        self,
        *,
        node,
        inputs: dict[str, Any],
        ctx: dict[str, Any],
        exc: BaseException,
    ) -> None:
        self._dispatch(
            "on_node_error",
            reverse=True,
            node=node,
            inputs=inputs,
            ctx=ctx,
            exc=exc,
        )

    def partition_end(self, *, partition, ctx: dict[str, Any], value_store) -> None:
        self._dispatch(
            "partition_end",
            partition=partition,
            ctx=ctx,
            value_store=value_store,
        )

    def dataset_end(
        self,
        *,
        dataset_name: str,
        ctx: dict[str, Any],
        value_store,
    ) -> None:
        self._dispatch(
            "dataset_end",
            dataset_name=dataset_name,
            ctx=ctx,
            value_store=value_store,
        )

    def run_end(self, *, plan, ctx: dict[str, Any], summary: dict[str, Any]) -> None:
        self._dispatch("run_end", plan=plan, ctx=ctx, summary=summary)
        self._merge_hook_summaries(summary)
        summary["hooks"] = self.usage_summary()
        ctx["_hook_summary"] = summary["hooks"]

    def _merge_hook_summaries(self, summary: dict[str, Any]) -> None:
        for binding in self.hooks:
            hook = binding["hook"]
            method = getattr(hook, "summary", None)
            if method is None:
                continue
            hook_summary = method()
            if not isinstance(hook_summary, dict):
                continue
            for key, value in hook_summary.items():
                if isinstance(value, list):
                    summary.setdefault(key, [])
                    if isinstance(summary[key], list):
                        for item in value:
                            if item not in summary[key]:
                                summary[key].append(item)
                    else:
                        summary[key] = value
                    continue
                if isinstance(value, dict):
                    summary.setdefault(key, {})
                    if isinstance(summary[key], dict):
                        summary[key].update(value)
                    else:
                        summary[key] = value
                    continue
                summary[key] = value

    def usage_summary(self) -> dict[str, Any]:
        enabled: list[dict[str, Any]] = []
        for binding in [*self.hooks, *self._all_node_modifier_bindings()]:
            spec = dict(binding.get("spec") or {})
            item = {
                "kind": spec.get("kind"),
                "events": list(spec.get("events") or []),
                "calls": int(binding.get("calls") or 0),
            }
            if "source" in spec:
                item["source"] = spec["source"]
            if "scope" in spec:
                item["scope"] = spec["scope"]
            if "node" in spec:
                item["node"] = spec["node"]
            if "mutates" in spec:
                item["mutates"] = spec["mutates"]
            if "params" in spec:
                item["params"] = spec["params"]
            enabled.append(item)
        return {"enabled": enabled}

    def _bindings_for_event(self, event: str, *, node: Any = None) -> list[dict[str, Any]]:
        bindings = list(self.hooks)
        if node is not None and event in NODE_EVENTS:
            bindings.extend(self._node_modifier_bindings_for(node))
        return sorted(bindings, key=lambda binding: int(binding.get("order") or 0))

    def _node_modifier_bindings_for(self, node: Any) -> list[dict[str, Any]]:
        node_id = _node_id(node)
        if node_id in self._node_modifier_bindings:
            return self._node_modifier_bindings[node_id]
        bindings: list[dict[str, Any]] = []
        execution = dict((getattr(node, "meta", {}) or {}).get("execution") or {})
        for index, raw in enumerate(list(execution.get("modifiers") or [])):
            if not isinstance(raw, dict):
                continue
            name = str(raw.get("name") or "")
            if not name:
                continue
            params = dict(raw.get("params") or {})
            hook = self._load_execution_modifier(name=name, node=node, params=params)
            events = _supported_node_events(hook)
            bindings.append(
                {
                    "hook": hook,
                    "events": events,
                    "spec": {
                        "kind": name,
                        "source": "execution_modifier",
                        "scope": "node",
                        "node": node_id,
                        "mutates": True,
                        "events": sorted(events),
                        "params": params,
                    },
                    "calls": 0,
                    "index": index,
                    "order": 300 + index,
                    "source": "execution_modifier",
                    "scope": "node",
                    "mutates": True,
                }
            )
        self._node_modifier_bindings[node_id] = bindings
        return bindings

    def _load_execution_modifier(
        self,
        *,
        name: str,
        node: Any,
        params: dict[str, Any],
    ) -> Any:
        try:
            entry = load_runtime_entry(self.registry_cfg, "execution_modifiers", name)
        except Exception as exc:
            raise HookDispatchError(
                kind=name,
                event="resolve",
                source="execution_modifier",
                node_id=_node_id(node),
                cause=RuntimeError(f"Execution modifier {name!r} is not registered"),
            ) from exc
        impl_ref = entry.get("impl")
        if not isinstance(impl_ref, str) or ":" not in impl_ref:
            raise HookDispatchError(
                kind=name,
                event="resolve",
                source="execution_modifier",
                node_id=_node_id(node),
                cause=RuntimeError(
                    f"Execution modifier {name!r} must define string 'impl' "
                    "as 'module:object'"
                ),
            )
        try:
            impl = load_object(impl_ref)
        except Exception as exc:
            raise HookDispatchError(
                kind=name,
                event="resolve",
                source="execution_modifier",
                node_id=_node_id(node),
                cause=exc,
            ) from exc
        hook = impl(**params) if isinstance(impl, type) else impl
        if not _supported_node_events(hook):
            raise HookDispatchError(
                kind=name,
                event="resolve",
                source="execution_modifier",
                node_id=_node_id(node),
                cause=TypeError(
                    "execution modifier must define at least one node lifecycle method"
                ),
            )
        return hook

    def _all_node_modifier_bindings(self) -> list[dict[str, Any]]:
        bindings: list[dict[str, Any]] = []
        for node_bindings in self._node_modifier_bindings.values():
            bindings.extend(node_bindings)
        return bindings


def _hook_params(hook_cfg: dict[str, Any]) -> dict[str, Any]:
    params = dict(hook_cfg.get("params") or {})
    for key, value in hook_cfg.items():
        if key in {"kind", "events", "source", "provenance", "params"}:
            continue
        params.setdefault(key, value)
    return params


def _normalize_hook_event(event: Any) -> str:
    value = str(event).strip()
    return WHEN_ALIASES.get(value, value)


NODE_EVENTS = {"before_node", "around_node", "after_node", "on_node_error"}


def _supported_node_events(hook: Any) -> set[str]:
    return {event for event in NODE_EVENTS if callable(getattr(hook, event, None))}


def _apply_binding_params(binding: dict[str, Any], call_kwargs: dict[str, Any]) -> None:
    if binding.get("source") != "execution_modifier":
        return
    ctx = call_kwargs.get("ctx")
    if not isinstance(ctx, dict):
        return
    params = dict((binding.get("spec") or {}).get("params") or {})
    ctx.update(params)


def _node_id(node: Any) -> str:
    return str(getattr(node, "id", "node"))
