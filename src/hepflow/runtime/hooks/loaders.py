from __future__ import annotations

from typing import Any

from hepflow.model.hooks import HookSpec
from hepflow.registry.loaders import load_object


def load_hook_spec(
    registry_cfg: dict[str, Any],
    kind: str,
) -> HookSpec:
    entry = _hook_entry(registry_cfg, kind)
    spec_ref = entry.get("spec")
    if not isinstance(spec_ref, str) or ":" not in spec_ref:
        raise TypeError(
            f"Execution hook '{kind}' must define string 'spec' as 'module:object'"
        )
    spec_obj = load_object(spec_ref)
    return HookSpec.from_obj(spec_obj)


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


def build_hook_manager(plan) -> Any:
    from hepflow.runtime.hooks.manager import HookManager  # noqa: PLC0415

    return HookManager.from_plan(plan)
