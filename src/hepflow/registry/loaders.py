from __future__ import annotations

import importlib
from typing import Any, cast

from hepflow.model.products import (
    BoundaryRepresentation,
    ProductBoundaryPolicy,
    ProductHandlerEntry,
)
from hepflow.progress import ProgressSink
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
      "functions": {"nth": "some_package.expr_helpers:nth"},
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

    product_handlers_cfg = dict(cfg.get("product_handlers") or {})

    product_handlers = {}
    for name, entry_cfg in product_handlers_cfg.items():
        if not isinstance(entry_cfg, dict):
            raise TypeError(
                f"Product handler registry entry '{name}' must be a mapping"
            )

        combine_obj = (
            load_object(entry_cfg["combine"])
            if isinstance(entry_cfg.get("combine"), str)
            else None
        )
        merge_obj = (
            load_object(entry_cfg["merge"])
            if isinstance(entry_cfg.get("merge"), str)
            else None
        )
        materialize_obj = (
            load_object(entry_cfg["materialize"])
            if isinstance(entry_cfg.get("materialize"), str)
            else None
        )
        boundary_materialize_obj = (
            load_object(entry_cfg["boundary_materialize"])
            if isinstance(entry_cfg.get("boundary_materialize"), str)
            else None
        )
        if combine_obj is not None and not callable(combine_obj):
            raise TypeError(f"Product handler combine '{name}' is not callable")
        if merge_obj is not None and not callable(merge_obj):
            raise TypeError(f"Product handler merge '{name}' is not callable")
        if materialize_obj is not None and not callable(materialize_obj):
            raise TypeError(f"Product handler materialize '{name}' is not callable")
        if boundary_materialize_obj is not None and not callable(
            boundary_materialize_obj
        ):
            raise TypeError(
                f"Product handler boundary_materialize '{name}' is not callable"
            )

        product_handlers[name] = ProductHandlerEntry(
            combine=combine_obj,
            merge=merge_obj,
            materialize=materialize_obj,
            boundary=_product_boundary_policy(name, entry_cfg.get("boundary")),
            boundary_materialize=boundary_materialize_obj,
        )

    return RuntimeRegistry(
        product_handlers=product_handlers,
    )


def _product_boundary_policy(
    name: str,
    cfg: Any,
) -> ProductBoundaryPolicy:
    if cfg is None:
        return ProductBoundaryPolicy()
    if not isinstance(cfg, dict):
        raise TypeError(f"Product handler boundary '{name}' must be a mapping")

    retain = cfg.get("retain", False)
    if not isinstance(retain, bool):
        raise TypeError(f"Product handler boundary '{name}'.retain must be boolean")

    representation = cfg.get("representation", "value")
    if representation not in {"value", "reference", "materialize"}:
        raise ValueError(
            f"Product handler boundary '{name}'.representation must be one of "
            "'value', 'reference', or 'materialize'; got {representation!r}"
        )

    return ProductBoundaryPolicy(
        retain=retain,
        representation=cast(BoundaryRepresentation, representation),
    )


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
    if not isinstance(category_cfg, dict):
        raise KeyError(f"Unknown runtime registry category '{category}'")
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


def load_progress_sink(
    registry_cfg: dict[str, Any] | None,
    name: str,
) -> ProgressSink:
    entry = load_runtime_entry(registry_cfg, "progress_sinks", name)
    impl_ref = entry.get("impl")
    if not isinstance(impl_ref, str):
        raise KeyError(f"Progress sink registry entry {name!r} must define 'impl'")
    impl = load_object(impl_ref)
    params = dict(entry.get("params") or {})
    sink = impl(**params) if isinstance(impl, type) else impl
    if not hasattr(sink, "handle"):
        raise TypeError(f"Progress sink {name!r} must define handle(update)")
    return sink


def load_progress_sinks(
    registry_cfg: dict[str, Any] | None,
    names: list[str] | tuple[str, ...],
) -> list[ProgressSink]:
    return [load_progress_sink(registry_cfg, name) for name in names]
