from __future__ import annotations

from typing import Any

from hepflow.model.component_spec import RuntimeComponentSpec
from hepflow.model.hooks import HookSpec
from hepflow.registry.loaders import load_object


def load_hook_spec(
    registry_cfg: dict[str, Any],
    kind: str,
) -> RuntimeComponentSpec:
    entry = _hook_entry(registry_cfg, kind)
    spec_ref = entry.get("spec")
    if not isinstance(spec_ref, str) or ":" not in spec_ref:
        raise TypeError(
            f"Execution hook '{kind}' must define string 'spec' as 'module:object'"
        )
    spec_obj = load_object(spec_ref)
    return hook_component_spec_from_obj(spec_obj)


def hook_component_spec_from_obj(obj: Any) -> RuntimeComponentSpec:
    if isinstance(obj, HookSpec):
        return RuntimeComponentSpec(
            name=obj.name,
            kind=obj.kind,
            version=obj.version,
            lifecycle={"events": list(obj.events)},
            context_outputs=list(obj.context_outputs),
        )
    return RuntimeComponentSpec.from_obj(obj)


def hook_spec_events(spec: RuntimeComponentSpec) -> list[str]:
    lifecycle = dict(spec.lifecycle or {})
    events = lifecycle.get("events")
    if events is None and "events" in spec.params:
        events = spec.params.get("events")
    if events is None:
        events = []
    if not isinstance(events, list) or not all(
        isinstance(event, str) and event for event in events
    ):
        raise ValueError(
            f"Hook spec {spec.name!r} lifecycle.events must be a list of strings"
        )
    return list(events)


def hook_spec_context_outputs(spec: RuntimeComponentSpec) -> list[str]:
    return list(spec.context_outputs or [])


def load_hook_impl(
    registry_cfg: dict[str, Any],
    kind: str,
) -> Any:
    entry = _hook_entry(registry_cfg, kind)
    impl_ref = entry.get("impl")
    if not isinstance(impl_ref, str) or ":" not in impl_ref:
        raise TypeError(
            f"Execution hook '{kind}' must define string 'impl' as 'module:object'"
        )
    return load_object(impl_ref)


def _hook_entry(registry_cfg: dict[str, Any], kind: str) -> dict[str, Any]:
    hooks = dict((registry_cfg or {}).get("hooks") or {})
    entry = hooks.get(kind)
    if not isinstance(entry, dict):
        raise KeyError(f"Unknown execution hook kind '{kind}'")
    return entry
