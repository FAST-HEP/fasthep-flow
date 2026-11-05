"""Non-mutating structural validation of a loaded plan before it is compared."""

from __future__ import annotations

import math
from typing import Any

from hepflow.compiler.plan_diff.errors import InvalidPlan
from hepflow.compiler.plan_diff.identity import (
    STRICT_UNIQUE_PATHS,
    identity_spec,
    registered_paths,
)

_SCALARS = (str, int, float, bool, type(None))

#: Expected container type at known key paths (checked only when present).
_CONTAINERS: dict[tuple[str, ...], type] = {
    ("context",): dict,
    ("nodes",): list,
    ("partitions",): list,
    ("registry",): dict,
    ("provenance",): dict,
    ("execution",): dict,
    ("execution_hooks",): list,
    ("reports",): list,
    ("data_flow",): dict,
}


def _join(path: str, key: str) -> str:
    return f"{path}.{key}" if path else key


def _walk(value: Any, path: str, side: str) -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            if not isinstance(key, str):
                raise InvalidPlan(
                    f"{side} plan: mapping key {key!r} is not a string", path
                )
            _walk(child, _join(path, key), side)
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _walk(child, f"{path}[{index}]", side)
    elif type(value) in _SCALARS:
        if isinstance(value, float) and not math.isfinite(value):
            raise InvalidPlan(f"{side} plan: non-finite number {value!r}", path)
    else:
        raise InvalidPlan(
            f"{side} plan: {type(value).__name__} value is not representable in JSON",
            path,
        )


def _get(plan: dict[str, Any], key_path: tuple[str, ...]) -> Any:
    value: Any = plan
    for key in key_path:
        if not isinstance(value, dict) or key not in value:
            return None
        value = value[key]
    return value


def _display(key_path: tuple[str, ...]) -> str:
    return ".".join(key_path)


def _check_lists(plan: dict[str, Any], side: str) -> None:
    for key_path in registered_paths():
        spec = identity_spec(key_path)
        assert spec is not None
        if key_path == ("nodes", "inputs"):
            continue  # nested per node, checked below
        _check_identified(_get(plan, key_path), key_path, key_path, spec, side)
    for index, node in enumerate(plan.get("nodes") or []):
        spec = identity_spec(("nodes", "inputs"))
        assert spec is not None
        if isinstance(node, dict) and "inputs" in node:
            _check_identified(
                node["inputs"],
                ("nodes", "inputs"),
                (f"nodes[{index}]", "inputs"),
                spec,
                side,
            )


def _check_identified(
    items: Any, key_path: tuple[str, ...], shown: tuple[str, ...], spec: Any, side: str
) -> None:
    if items is None:
        return
    base = ".".join(shown)
    if not isinstance(items, list):
        raise InvalidPlan(f"{side} plan: expected a list", base)
    seen: set[tuple[Any, ...]] = set()
    for index, item in enumerate(items):
        where = f"{base}[{index}]"
        if not isinstance(item, dict):
            raise InvalidPlan(f"{side} plan: expected a mapping", where)
        values = []
        for field in spec.fields:
            if field not in item:
                raise InvalidPlan(
                    f"{side} plan: missing identity field {field!r}", f"{where}.{field}"
                )
            value = item[field]
            if type(value) not in _SCALARS:
                raise InvalidPlan(
                    f"{side} plan: identity field {field!r} is not a scalar",
                    f"{where}.{field}",
                )
            if (
                key_path in (("nodes",), ("reports",))
                and field == "id"
                and not isinstance(value, str)
            ):
                raise InvalidPlan(
                    f"{side} plan: {field!r} must be a string", f"{where}.{field}"
                )
            values.append((type(value), value))
        identity = tuple(values)
        if key_path in STRICT_UNIQUE_PATHS:
            if identity in seen:
                raise InvalidPlan(
                    f"{side} plan: duplicate identity in {_display(key_path)}", where
                )
            seen.add(identity)


def validate_plan(plan: object, *, side: str) -> None:
    """Raise :class:`InvalidPlan` when ``plan`` cannot be compared; never mutates it."""
    if not isinstance(plan, dict):
        raise InvalidPlan(f"{side} plan: the root must be a mapping")
    _walk(plan, "", side)
    for key_path, expected in _CONTAINERS.items():
        value = _get(plan, key_path)
        if value is not None and not isinstance(value, expected):
            raise InvalidPlan(
                f"{side} plan: expected {expected.__name__}, found {type(value).__name__}",
                _display(key_path),
            )
    provenance_inputs = _get(plan, ("provenance", "inputs"))
    if provenance_inputs is not None and not isinstance(provenance_inputs, list):
        raise InvalidPlan(f"{side} plan: expected list", "provenance.inputs")
    _check_lists(plan, side)
