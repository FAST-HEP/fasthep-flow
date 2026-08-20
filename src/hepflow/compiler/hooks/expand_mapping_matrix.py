from __future__ import annotations

from itertools import product
from typing import Any

from hepflow.compiler.hooks.model import CompileHookResult, ParamCompileHookContext


def expand_mapping_matrix(
    *,
    value: Any,
    options: dict[str, Any],
    context: ParamCompileHookContext,
) -> CompileHookResult:
    del options, context
    if not isinstance(value, dict):
        raise TypeError(
            f"flow.expand_mapping_matrix expected mapping, got {type(value).__name__}"
        )

    if "matrix" not in value:
        return CompileHookResult(
            value=dict(value),
            provenance={
                "hook": "flow.expand_mapping_matrix",
                "input_kind": "mapping",
                "output_kind": "mapping",
                "generated": 0,
            },
        )

    explicit = {key: item for key, item in value.items() if key != "matrix"}
    matrix = value["matrix"]
    if not isinstance(matrix, dict):
        raise TypeError("flow.expand_mapping_matrix matrix must be a mapping")

    axes = matrix.get("axes")
    if not isinstance(axes, dict) or not axes:
        raise ValueError(
            "flow.expand_mapping_matrix matrix.axes must be a non-empty mapping"
        )

    axis_names = list(axes)
    axis_values = [_axis_values(name, axes[name]) for name in axis_names]
    templates = matrix.get("mappings", matrix.get("templates"))
    if not isinstance(templates, list) or not templates:
        raise ValueError(
            "flow.expand_mapping_matrix matrix.mappings must be a non-empty list"
        )

    out = dict(explicit)
    generated = 0
    for values in product(*axis_values):
        context_values = dict(zip(axis_names, values, strict=True))
        for index, raw_template in enumerate(templates):
            if not isinstance(raw_template, dict):
                raise TypeError(
                    "flow.expand_mapping_matrix matrix.mappings"
                    f"[{index}] must be a mapping"
                )
            try:
                target = _format_template(raw_template.get("target"), context_values)
                source = _format_template(raw_template.get("source"), context_values)
            except KeyError as exc:
                missing = exc.args[0]
                raise ValueError(
                    "flow.expand_mapping_matrix template references unknown axis "
                    f"{missing!r}"
                ) from exc
            if target in out:
                raise ValueError(
                    f"flow.expand_mapping_matrix generated duplicate target {target!r}"
                )
            out[target] = source
            generated += 1

    return CompileHookResult(
        value=out,
        provenance={
            "hook": "flow.expand_mapping_matrix",
            "axis_names": axis_names,
            "template_count": len(templates),
            "generated": generated,
            "output_kind": "mapping",
        },
    )


def _axis_values(name: str, raw: Any) -> list[str]:
    if not isinstance(name, str) or not name:
        raise ValueError(
            "flow.expand_mapping_matrix axis names must be non-empty strings"
        )
    if not isinstance(raw, list) or not raw:
        raise ValueError(
            f"flow.expand_mapping_matrix matrix.axes.{name} must be a non-empty list"
        )
    values = [str(item) for item in raw]
    if any(not item for item in values):
        raise ValueError(
            f"flow.expand_mapping_matrix matrix.axes.{name} contains an empty value"
        )
    return values


def _format_template(raw: Any, context_values: dict[str, str]) -> str:
    if not isinstance(raw, str) or not raw:
        raise ValueError(
            "flow.expand_mapping_matrix templates require non-empty target/source strings"
        )
    return raw.format(**context_values)
