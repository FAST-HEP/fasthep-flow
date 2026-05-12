from __future__ import annotations

import importlib
from typing import Any

from hepflow.model.ops import OpEntry, OpSpec
from hepflow.model.render_types import RenderEntry, RenderTypeSpec
from hepflow.registry.defaults import (
    default_expr_registry_config,
    default_runtime_registry_config,
    merge_registry_config,
)
from hepflow.registry.expr import ExprRegistry
from hepflow.registry.runtime import RuntimeRegistry


def load_object(spec: str) -> Any:
    """
    Load 'module.submodule:object_name'.
    """
    if not isinstance(spec, str) or ":" not in spec:
        raise ValueError(
            f"Invalid object spec '{spec}'. Expected format 'module.submodule:object'"
        )
    mod_name, obj_name = spec.split(":", 1)
    mod = importlib.import_module(mod_name)
    try:
        return getattr(mod, obj_name)
    except AttributeError as e:
        raise AttributeError(
            f"Module '{mod_name}' has no attribute '{obj_name}'"
        ) from e


def expr_registry_from_config(cfg: dict[str, Any] | None) -> ExprRegistry:
    """
    Build an ExprRegistry from symbolic config, e.g.

    {
      "functions": {"nth": "fasthep_carpenter.expr_helpers:nth"},
      "constants": {"PI": "math:pi"}
    }
    """
    if not cfg:
        return ExprRegistry()

    functions_cfg = dict(cfg.get("functions") or {})
    constants_cfg = dict(cfg.get("constants") or {})

    functions = {name: load_object(spec) for name, spec in functions_cfg.items()}
    constants = {name: load_object(spec) for name, spec in constants_cfg.items()}

    return ExprRegistry(functions=functions, constants=constants)


def runtime_registry_from_config(cfg: dict[str, Any] | None) -> RuntimeRegistry:
    if not cfg:
        return RuntimeRegistry()

    ops_cfg = dict(cfg.get("ops") or {})
    renderers_cfg = dict(cfg.get("renderers") or {})

    ops = {}
    for name, entry_cfg in ops_cfg.items():
        if not isinstance(entry_cfg, dict):
            raise TypeError(
                f"Op registry entry '{name}' must be a mapping with 'spec' and 'impl'"
            )

        spec_obj = load_object(entry_cfg["spec"])
        impl_obj = load_object(entry_cfg["impl"])

        if not isinstance(spec_obj, OpSpec):
            raise TypeError(f"Op spec '{name}' did not resolve to OpSpec")
        if not callable(impl_obj):
            raise TypeError(f"Op impl '{name}' did not resolve to a callable")

        ops[name] = OpEntry(spec=spec_obj, handler=impl_obj)

    renderers = {}
    for name, entry_cfg in renderers_cfg.items():
        if not isinstance(entry_cfg, dict):
            raise TypeError(
                f"Renderer registry entry '{name}' must be a mapping with 'spec' and 'impl'"
            )

        spec_obj = load_object(entry_cfg["spec"])
        impl_obj = load_object(entry_cfg["impl"])

        if not isinstance(spec_obj, RenderTypeSpec):
            raise TypeError(f"Renderer spec '{name}' did not resolve to RenderTypeSpec")
        if not callable(impl_obj):
            raise TypeError(f"Renderer impl '{name}' did not resolve to a callable")

        renderers[name] = RenderEntry(spec=spec_obj, handler=impl_obj)

    return RuntimeRegistry(ops=ops, renderers=renderers)


def resolve_runtime_registry(
    registry_cfg: dict[str, Any] | None = None,
) -> RuntimeRegistry:
    merged = merge_registry_config(
        {
            **default_expr_registry_config(),
            **default_runtime_registry_config(),
        },
        registry_cfg or {},
    )
    return runtime_registry_from_config(merged)


def compile_op_registry_from_config(cfg: dict[str, Any] | None) -> dict[str, OpSpec]:
    """
    Load only op specs from symbolic registry config.
    Used during compile for IR/deps validation.
    """
    if not cfg:
        return {}

    ops_cfg = dict(cfg.get("ops") or {})
    out: dict[str, OpSpec] = {}

    for name, entry_cfg in ops_cfg.items():
        if not isinstance(entry_cfg, dict):
            raise TypeError(
                f"Op registry entry '{name}' must be a mapping with 'spec' and 'impl'"
            )
        spec_obj = load_object(entry_cfg["spec"])
        if not isinstance(spec_obj, OpSpec):
            raise TypeError(f"Op spec '{name}' did not resolve to OpSpec")
        out[name] = spec_obj

    return out


def load_runtime_entry(
    registry_cfg: dict[str, Any] | None,
    category: str,
    name: str,
) -> dict[str, Any]:
    """
    Resolve a symbolic runtime registry entry from config, e.g. sinks/root_tree.
    """
    if registry_cfg:
        merged = registry_cfg
    else:
        merged = merge_registry_config(
            {
                **default_expr_registry_config(),
                **default_runtime_registry_config(),
            },
            {},
        )
    category_cfg = merged.get(category)
    try:
        entry = category_cfg[name]
    except KeyError as exc:
        raise KeyError(
            f"Unknown runtime registry entry '{name}' in category '{category}'"
        ) from exc

    if not isinstance(entry, dict):
        raise TypeError(f"Runtime registry entry '{category}.{name}' must be a mapping")

    return entry


def load_runtime_spec_and_impl(
    registry_cfg: dict[str, Any] | None,
    category: str,
    name: str,
) -> tuple[Any, Any]:
    """
    Resolve and load the spec and impl objects for a runtime registry entry.
    """
    entry = load_runtime_entry(registry_cfg, category, name)

    try:
        spec_ref = entry["spec"]
        impl_ref = entry["impl"]
    except KeyError as exc:
        raise KeyError(
            f"Runtime registry entry '{category}.{name}' must define 'spec' and 'impl'"
        ) from exc

    return load_object(spec_ref), load_object(impl_ref)
