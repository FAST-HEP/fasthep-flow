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
        stage_id = item.get("id")
        op = str(item.get("op") or "")
        if not op:
            normalized_stages.append(item)
            continue
        spec = _component_spec(registry, op)
        if spec is not None and _should_normalize_params(spec):
            params = dict(item.get("params") or {})
            if _should_materialize_defaults(spec):
                params = _with_param_defaults(params, spec.params)
            params = _with_param_templates(
                params,
                marker=spec.normalize_params,
            )
            params = _with_stage_id_defaults(
                params,
                stage_id=stage_id,
                marker=spec.normalize_params,
            )
            item["params"] = params
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


def _should_normalize_params(spec: RuntimeComponentSpec | None) -> bool:
    if spec is None:
        return False
    marker = getattr(spec, "normalize_params", None)
    return isinstance(marker, dict) and bool(marker)


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


def _with_param_templates(
    params: dict[str, Any],
    *,
    marker: dict[str, Any],
) -> dict[str, Any]:
    templates = marker.get("param_templates")
    if not isinstance(templates, dict) or not templates:
        return params
    out = deepcopy(params)
    for param_name, template in templates.items():
        if not isinstance(param_name, str) or not param_name:
            continue
        if param_name in out:
            continue
        if not isinstance(template, str) or not template.strip():
            continue
        out[param_name] = _format_template(template, out)
    return out


def _format_template(template: str, params: dict[str, Any]) -> str:
    values: dict[str, str] = {}
    for raw in template.split("{")[1:]:
        token = raw.split("}", 1)[0]
        value: Any = params
        for segment in token.split("."):
            if not isinstance(value, dict) or segment not in value:
                raise ValueError(
                    f"Cannot materialize component parameter template {template!r}: "
                    f"{token!r} is missing"
                )
            value = value[segment]
        if not isinstance(value, str) or not value.strip():
            raise ValueError(
                f"Cannot materialize component parameter template {template!r}: "
                f"{token!r} must resolve to one string"
            )
        values[token] = value.strip()
    out = template
    for token, value in values.items():
        out = out.replace("{" + token + "}", value)
    return out


def _with_stage_id_defaults(
    params: dict[str, Any],
    *,
    stage_id: Any,
    marker: dict[str, Any],
) -> dict[str, Any]:
    defaults = marker.get("stage_id_defaults")
    if not isinstance(defaults, dict) or not defaults:
        return params
    if not isinstance(stage_id, str) or not stage_id:
        return params

    out = deepcopy(params)
    for param_name, source in defaults.items():
        if source != "id":
            continue
        if not isinstance(param_name, str) or not param_name:
            continue
        if param_name not in out:
            out[param_name] = stage_id
    return out
