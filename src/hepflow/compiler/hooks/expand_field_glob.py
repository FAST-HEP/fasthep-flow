from __future__ import annotations

import fnmatch
from typing import Any

from hepflow.compiler.hooks.model import CompileHookResult, ParamCompileHookContext


def expand_field_glob(
    *,
    value: Any,
    options: dict[str, Any],
    context: ParamCompileHookContext,
) -> CompileHookResult:
    against = str(options.get("against") or "")
    if against != "input.stream":
        raise ValueError("flow.expand_field_glob currently supports against='input.stream'")
    patterns = _string_list_param(
        value,
        param_name=str(options.get("_param_name") or "value"),
        spec_name=str(options.get("_spec_name") or "component"),
    )
    expanded, unmatched = _expand_field_glob_patterns(
        patterns,
        available_fields=list(context.input_stream_fields),
    )
    return CompileHookResult(
        value=expanded,
        provenance={
            "hook": "flow.expand_field_glob",
            "against": against,
            "input": patterns,
            "output": expanded,
            "unmatched": unmatched,
        },
    )


def _expand_field_glob_patterns(
    patterns: list[str],
    *,
    available_fields: list[str],
) -> tuple[list[str], list[str]]:
    expanded: list[str] = []
    seen: set[str] = set()
    unmatched: list[str] = []
    for pattern in patterns:
        matches = [field for field in available_fields if fnmatch.fnmatchcase(field, pattern)]
        if not matches:
            unmatched.append(pattern)
            continue
        for field in matches:
            if field in seen:
                continue
            expanded.append(field)
            seen.add(field)
    return expanded, unmatched


def _string_list_param(value: Any, *, param_name: str, spec_name: str) -> list[str]:
    if isinstance(value, str):
        values = [value]
    elif isinstance(value, list):
        values = value
    else:
        raise TypeError(f"{spec_name} parameter {param_name!r} must be a list of strings")
    if not all(isinstance(item, str) and item.strip() for item in values):
        raise ValueError(f"{spec_name} parameter {param_name!r} contains an invalid field")
    return [str(item).strip() for item in values]
