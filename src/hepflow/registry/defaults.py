from __future__ import annotations

from typing import Any

import numpy as np

from hepflow.registry.expr import ExprRegistry

REGISTRY_SECTIONS: tuple[str, ...] = (
    "functions",
    "constants",
    "ops",
    "renderers",
    "sinks",
    "sources",
    "observers",
    "transforms",
    "backends",
    "hooks",
)


def default_expr_registry() -> ExprRegistry:
    return ExprRegistry(
        functions={
            "sqrt": np.sqrt,
            "abs": np.abs,
            "log": np.log,
            "log10": np.log10,
            "exp": np.exp,
            "where": np.where,
        },
        constants={},
    )


def default_expr_registry_config() -> dict[str, Any]:
    return {
        "functions": {
            "sqrt": "numpy:sqrt",
            "abs": "numpy:abs",
            "log": "numpy:log",
            "log10": "numpy:log10",
            "exp": "numpy:exp",
            "where": "numpy:where",
        },
        "constants": {},
    }


def merge_registry_config(
    base: dict[str, Any] | None,
    override: dict[str, Any] | None,
) -> dict[str, Any]:
    base = dict(base or {})
    override = dict(override or {})

    merged: dict[str, Any] = {}
    for section in REGISTRY_SECTIONS:
        merged[section] = {
            **dict(base.get(section) or {}),
            **dict(override.get(section) or {}),
        }
    return merged


def default_runtime_registry_config() -> dict[str, Any]:
    return {
        "ops": {},
        "renderers": {},
        "sinks": {},
        "sources": {},
        "observers": {},
        "transforms": {},
        "backends": {
            "local.default": {
                "impl": "hepflow.backends.local:LocalBackend",
            },
            "dask.local": {
                "impl": "hepflow.backends.dask_local:DaskLocalBackend",
            },
        },
        "hooks": {},
    }
