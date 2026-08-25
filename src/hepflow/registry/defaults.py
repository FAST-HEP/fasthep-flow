from __future__ import annotations

from typing import Any

import numpy as np

from hepflow.registry.expr import ExprRegistry

REGISTRY_SECTIONS: tuple[str, ...] = (
    "functions",
    "constants",
    "sinks",
    "sources",
    "observers",
    "transforms",
    "backends",
    "hooks",
    "compile_hooks",
    "render",
    "progress_sinks",
    "report_templates",
    "execution_modifiers",
    "product_handlers",
)


def default_expr_registry() -> ExprRegistry:
    return ExprRegistry(
        functions={
            "sqrt": np.sqrt,
            "abs": np.abs,
            "log": np.log,
            "log10": np.log10,
            "exp": np.exp,
            "cosh": np.cosh,
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
            "cosh": "numpy:cosh",
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
        "sinks": {},
        "sources": {},
        "observers": {},
        "transforms": {},
        "backends": {
            "local.default": {
                "impl": "hepflow.backends:Local",
            },
            "dask": {
                "impl": "hepflow.backends:Dask",
            },
        },
        "hooks": {},
        "compile_hooks": {
            "flow.expand_field_glob": {
                "kind": "parameter",
                "impl": "hepflow.compiler.hooks.expand_field_glob:expand_field_glob",
            },
            "flow.load_mapping": {
                "kind": "parameter",
                "impl": "hepflow.compiler.hooks.load_mapping:load_mapping",
            },
            "flow.expand_mapping_matrix": {
                "kind": "parameter",
                "impl": (
                    "hepflow.compiler.hooks.expand_mapping_matrix:expand_mapping_matrix"
                ),
            },
        },
        "render": {},
        "progress_sinks": {},
        "report_templates": {},
        "execution_modifiers": {},
        "product_handlers": {
            "artifact": {
                "boundary": {
                    "retain": True,
                    "representation": "reference",
                },
            },
        },
    }
