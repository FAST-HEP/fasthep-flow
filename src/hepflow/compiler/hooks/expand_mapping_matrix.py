from __future__ import annotations

from copy import deepcopy
from itertools import product
from string import Formatter
from typing import Any

from hepflow.compiler.hooks.model import CompileHookResult, ParamCompileHookContext


def expand_mapping_matrix(
    *,
    value: Any,
    options: dict[str, Any],
    context: ParamCompileHookContext,
) -> CompileHookResult:
    del options, context
    if isinstance(value, list):
        return _expand_list_matrix(value)
    if not isinstance(value, dict):
        raise TypeError(
            "flow.expand_mapping_matrix expected mapping or list[mapping], "
            f"got {type(value).__name__}"
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
            "input_kind": "mapping",
            "axis_names": axis_names,
            "template_count": len(templates),
            "generated": generated,
            "output_kind": "mapping",
        },
    )


def _expand_list_matrix(value: list[Any]) -> CompileHookResult:
    out: list[Any] = []
    generated = 0
    expanded_entries = 0
    axis_names: list[str] = []

    for index, item in enumerate(value):
        if not isinstance(item, dict):
            out.append(deepcopy(item))
            continue
        matrix_raw = item.get("matrix")
        if matrix_raw is None:
            out.append(deepcopy(item))
            continue
        if not isinstance(matrix_raw, dict) or not matrix_raw:
            raise ValueError(
                "flow.expand_mapping_matrix list entry "
                f"{index} matrix must be a non-empty mapping"
            )
        axes = _matrix_axes(matrix_raw, f"list entry {index}")
        entry_axis_names = [name for name, _values in axes]
        axis_names.extend(name for name in entry_axis_names if name not in axis_names)
        template = {key: item_value for key, item_value in item.items() if key != "matrix"}
        for values in product(*(values for _name, values in axes)):
            context_values = dict(zip(entry_axis_names, values, strict=True))
            out.append(_substitute_matrix_templates(template, context_values, index))
            generated += 1
        expanded_entries += 1

    return CompileHookResult(
        value=out,
        provenance={
            "hook": "flow.expand_mapping_matrix",
            "input_kind": "list[mapping]",
            "output_kind": "list[mapping]",
            "axis_names": axis_names,
            "expanded_entries": expanded_entries,
            "generated": generated,
        },
    )


def _matrix_axes(matrix: dict[str, Any], where: str) -> list[tuple[str, list[str]]]:
    axes: list[tuple[str, list[str]]] = []
    for name, raw in matrix.items():
        axes.append((name, _axis_values(name, raw, where=where)))
    return axes


def _axis_values(name: str, raw: Any, *, where: str = "matrix.axes") -> list[str]:
    if not isinstance(name, str) or not name:
        raise ValueError(
            "flow.expand_mapping_matrix axis names must be non-empty strings"
        )
    if isinstance(raw, dict) and "range" in raw:
        return _range_values(name, raw["range"], where=where)
    if not isinstance(raw, list) or not raw:
        raise ValueError(
            "flow.expand_mapping_matrix "
            f"{where}.{name} must be a non-empty list or range"
        )
    values = [str(item) for item in raw]
    if any(not item for item in values):
        raise ValueError(
            f"flow.expand_mapping_matrix {where}.{name} contains an empty value"
        )
    return values


def _range_values(name: str, raw: Any, *, where: str) -> list[str]:
    if not isinstance(raw, dict):
        raise ValueError(
            f"flow.expand_mapping_matrix {where}.{name}.range must be a mapping"
        )
    start = raw.get("start", 0)
    stop = raw.get("stop")
    step = raw.get("step", 1)
    if stop is None:
        raise ValueError(
            f"flow.expand_mapping_matrix {where}.{name}.range.stop is required"
        )
    if not all(isinstance(item, int) for item in (start, stop, step)):
        raise ValueError(
            f"flow.expand_mapping_matrix {where}.{name}.range values must be integers"
        )
    if step == 0:
        raise ValueError(
            f"flow.expand_mapping_matrix {where}.{name}.range.step must not be zero"
        )
    return [str(item) for item in range(start, stop, step)]


def _substitute_matrix_templates(
    value: Any,
    context_values: dict[str, str],
    entry_index: int,
) -> Any:
    if isinstance(value, str):
        return _format_value_template(value, context_values, entry_index)
    if isinstance(value, list):
        return [
            _substitute_matrix_templates(item, context_values, entry_index)
            for item in value
        ]
    if isinstance(value, dict):
        return {
            _substitute_matrix_templates(key, context_values, entry_index): (
                _substitute_matrix_templates(item, context_values, entry_index)
            )
            for key, item in value.items()
        }
    return deepcopy(value)


def _format_value_template(
    raw: str,
    context_values: dict[str, str],
    entry_index: int,
) -> str:
    formatter = Formatter()
    for _literal, field_name, format_spec, conversion in formatter.parse(raw):
        if field_name is None:
            continue
        if not field_name.isidentifier():
            raise ValueError(
                "flow.expand_mapping_matrix list entry "
                f"{entry_index} template reference {field_name!r} "
                "must be a matrix axis name"
            )
        if field_name not in context_values:
            raise ValueError(
                "flow.expand_mapping_matrix list entry "
                f"{entry_index} template references unknown axis {field_name!r}"
            )
        if format_spec or conversion is not None:
            raise ValueError(
                "flow.expand_mapping_matrix list entry "
                f"{entry_index} template reference {field_name!r} "
                "must not use format specs"
            )
    return raw.format(**context_values)


def _format_template(raw: Any, context_values: dict[str, str]) -> str:
    if not isinstance(raw, str) or not raw:
        raise ValueError(
            "flow.expand_mapping_matrix templates require non-empty target/source strings"
        )
    return raw.format(**context_values)
