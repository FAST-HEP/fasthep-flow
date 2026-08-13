from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from hepflow.model.component_spec import RuntimeComponentSpec
from hepflow.registry.loaders import load_object


@dataclass(slots=True, frozen=True)
class ResolvedCompileHook:
    name: str
    kind: str
    impl_ref: str
    impl: Any
    entry: dict[str, Any]


def resolve_compile_hook(
    registry: dict[str, Any] | None,
    name: str,
    *,
    kind: str,
) -> ResolvedCompileHook:
    entry = _compile_hook_entry(registry, name)
    entry_kind = str(entry.get("kind") or "phase")
    if entry_kind != kind:
        raise TypeError(
            f"Compile hook {name!r} has kind {entry_kind!r}; expected {kind!r}"
        )
    impl_ref = entry.get("impl")
    if not isinstance(impl_ref, str) or not impl_ref.strip():
        raise ValueError(f"Compile hook registry entry {name!r} requires 'impl'")
    impl = load_object(impl_ref)
    if not callable(impl):
        raise TypeError(f"Compile hook {name!r} implementation is not callable")
    return ResolvedCompileHook(
        name=name,
        kind=entry_kind,
        impl_ref=impl_ref,
        impl=impl,
        entry=entry,
    )


def phase_compile_hook_entries(
    registry: dict[str, Any] | None,
    *,
    when: str,
) -> list[tuple[str, dict[str, Any], RuntimeComponentSpec]]:
    hooks = dict((registry or {}).get("compile_hooks") or {})
    selected: list[tuple[str, dict[str, Any], RuntimeComponentSpec]] = []
    for name, entry in hooks.items():
        if not isinstance(entry, dict):
            raise TypeError(f"Compile hook registry entry {name!r} must be a mapping")
        if entry.get("kind") == "parameter":
            continue
        resolved = resolve_compile_hook(registry, str(name), kind="phase")
        spec_ref = entry.get("spec")
        if not isinstance(spec_ref, str) or ":" not in spec_ref:
            raise TypeError(
                f"Compile hook registry entry {name!r} must define string 'spec' "
                "as 'module:object'"
            )
        spec = RuntimeComponentSpec.from_obj(load_object(spec_ref))
        if when not in _compile_hook_phases(spec):
            continue
        selected.append((str(name), dict(resolved.entry), spec))
    return selected


def _compile_hook_entry(
    registry: dict[str, Any] | None,
    name: str,
) -> dict[str, Any]:
    entry = dict((registry or {}).get("compile_hooks") or {}).get(name)
    if not isinstance(entry, dict):
        raise KeyError(f"Compile hook {name!r} is not registered")
    return dict(entry)


def _compile_hook_phases(spec: RuntimeComponentSpec) -> set[str]:
    lifecycle = dict(spec.lifecycle or {})
    raw = lifecycle.get("when")
    if raw is None:
        return set()
    if isinstance(raw, str):
        return {raw}
    if isinstance(raw, list) and all(isinstance(item, str) and item for item in raw):
        return set(raw)
    raise ValueError(
        f"Compile hook spec {spec.name!r} lifecycle.when must be a string "
        "or list of strings"
    )
