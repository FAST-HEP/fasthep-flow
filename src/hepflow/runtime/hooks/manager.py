from __future__ import annotations

from contextlib import ExitStack, contextmanager
from typing import Any

from hepflow.model.lifecycle import WHEN_ALIASES
from hepflow.registry.defaults import (
    default_runtime_registry_config,
    merge_registry_config,
)
from hepflow.runtime.hooks.loaders import load_hook_impl, load_hook_spec


class HookDispatchError(RuntimeError):
    def __init__(self, *, kind: str, event: str, cause: BaseException) -> None:
        self.kind = kind
        self.event = event
        self.cause = cause
        super().__init__(f"Error hook {kind} failed during {event}: {cause}")


class HookManager:
    """
    Execution lifecycle hook dispatcher.

    HookManager intentionally manages execution hooks only. Sinks and observers
    remain graph components; the shared lifecycle vocabulary keeps a future
    PluginManager possible without merging those concepts before alpha.
    """

    def __init__(self, hooks: list[tuple[Any, set[str]]] | None = None) -> None:
        self.hooks: list[dict[str, Any]] = []
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
                    },
                    "calls": 0,
                    "index": index,
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
                for event in list(hook_spec.events or [])
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
        manager = cls(hooks)
        for binding, spec in zip(manager.hooks, enabled_specs, strict=False):
            binding["spec"] = dict(spec)
        return manager

    def _dispatch(self, event: str, **kwargs: Any) -> None:
        for binding in self.hooks:
            hook = binding["hook"]
            events = binding["events"]
            if events and event not in events:
                continue
            method = getattr(hook, event, None)
            if method is not None:
                binding["calls"] += 1
                try:
                    method(**kwargs)
                except Exception as exc:
                    spec = dict(binding.get("spec") or {})
                    kind = str(spec.get("kind") or type(hook).__name__)
                    raise HookDispatchError(
                        kind=kind,
                        event=event,
                        cause=exc,
                    ) from exc

    def has_event(self, event: str) -> bool:
        for binding in self.hooks:
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
            for binding in self.hooks:
                hook = binding["hook"]
                events = binding["events"]
                if events and "around_node" not in events:
                    continue
                method = getattr(hook, "around_node", None)
                if method is None:
                    continue
                binding["calls"] += 1
                try:
                    stack.enter_context(method(node=node, inputs=inputs, ctx=ctx))
                except Exception as exc:
                    spec = dict(binding.get("spec") or {})
                    kind = str(spec.get("kind") or type(hook).__name__)
                    raise HookDispatchError(
                        kind=kind,
                        event="around_node",
                        cause=exc,
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
        self._dispatch("after_node", node=node, inputs=inputs, outputs=outputs, ctx=ctx)

    def on_node_error(
        self,
        *,
        node,
        inputs: dict[str, Any],
        ctx: dict[str, Any],
        exc: BaseException,
    ) -> None:
        self._dispatch("on_node_error", node=node, inputs=inputs, ctx=ctx, exc=exc)

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
        for binding in self.hooks:
            spec = dict(binding.get("spec") or {})
            item = {
                "kind": spec.get("kind"),
                "events": list(spec.get("events") or []),
                "calls": int(binding.get("calls") or 0),
            }
            if "source" in spec:
                item["source"] = spec["source"]
            if "params" in spec:
                item["params"] = spec["params"]
            enabled.append(item)
        return {"enabled": enabled}


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
