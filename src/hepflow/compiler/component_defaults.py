from __future__ import annotations

from copy import deepcopy
from typing import Any

from hepflow.model.component_spec import RuntimeComponentSpec
from hepflow.registry.loaders import load_runtime_spec_and_impl


def apply_component_param_defaults(
    normalized: dict[str, Any],
) -> dict[str, Any]:
    """Materialize opt-in component parameter defaults in analysis stages."""
    out = deepcopy(normalized)
    registry = dict(out.get("registry") or {})
    analysis = out.get("analysis")
    if not isinstance(analysis, dict):
        return out
    stages = analysis.get("stages")
    if not isinstance(stages, list):
        return out

    normalized_stages: list[Any] = []
    for stage in stages:
        if not isinstance(stage, dict):
            normalized_stages.append(stage)
            continue
        item = dict(stage)
        op = str(item.get("op") or "")
        if not op:
            normalized_stages.append(item)
            continue
        spec = _component_spec(registry, op)
        if spec is not None and _should_materialize_defaults(spec):
            item["params"] = _with_param_defaults(
                dict(item.get("params") or {}),
                spec.params,
            )
        normalized_stages.append(item)

    analysis["stages"] = normalized_stages
    return out


def _component_spec(
    registry: dict[str, Any],
    op: str,
) -> RuntimeComponentSpec | None:
    transforms = registry.get("transforms")
    if not isinstance(transforms, dict) or op not in transforms:
        return None
    try:
        spec, _impl = load_runtime_spec_and_impl(registry, "transforms", op)
        return RuntimeComponentSpec.from_obj(spec)
    except Exception:
        return None


def _should_materialize_defaults(spec: RuntimeComponentSpec | None) -> bool:
    if spec is None:
        return False
    marker = getattr(spec, "normalize_params", None)
    return isinstance(marker, dict) and marker.get("defaults") is True


def _with_param_defaults(
    params: dict[str, Any],
    schema: dict[str, Any],
) -> dict[str, Any]:
    out = deepcopy(params)
    for name, param_schema in schema.items():
        if not isinstance(param_schema, dict) or "default" not in param_schema:
            continue
        if name not in out:
            out[name] = deepcopy(param_schema["default"])
    return out
