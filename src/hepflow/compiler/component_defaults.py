from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Any

import yaml

from hepflow.model.component_spec import RuntimeComponentSpec
from hepflow.registry.loaders import load_runtime_spec_and_impl


def apply_component_param_defaults(
    normalized: dict[str, Any],
) -> dict[str, Any]:
    """Materialize declarative component parameter normalization in stages."""
    out = deepcopy(normalized)
    registry = dict(out.get("registry") or {})
    analysis = out.get("analysis")
    if not isinstance(analysis, dict):
        return out
    stages = analysis.get("stages")
    if not isinstance(stages, list):
        return out
    workflow_path = out.get("workflow_path")
    workflow_dir = (
        Path(workflow_path).parent
        if isinstance(workflow_path, str) and workflow_path
        else Path.cwd()
    )

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
        if spec is not None:
            params = dict(item.get("params") or {})
            params = _with_loaded_file_params(
                params,
                param_schema=spec.params,
                op=op,
                stage_id=stage_id,
                base_dir=workflow_dir,
            )
            if _should_normalize_params(spec):
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


def _with_loaded_file_params(
    params: dict[str, Any],
    *,
    param_schema: dict[str, Any],
    op: str,
    stage_id: Any,
    base_dir: Path,
) -> dict[str, Any]:
    if not param_schema:
        return params

    out = deepcopy(params)
    for param_name, spec in param_schema.items():
        if not isinstance(param_name, str) or param_name not in out:
            continue
        if not isinstance(spec, dict):
            continue
        load_spec = spec.get("load")
        param_path = f"params.{param_name}"
        value = out[param_name]
        if not isinstance(load_spec, dict):
            continue

        if isinstance(value, str):
            out[param_name] = _load_param_file(
                value,
                formats=_load_param_formats(load_spec),
                base_dir=base_dir,
                op=op,
                stage_id=stage_id,
                param_path=param_path,
                expected_type=str(spec.get("type") or ""),
            )
            continue

        _validate_loaded_param_value(
            value,
            spec=spec,
            op=op,
            stage_id=stage_id,
            param_path=param_path,
        )
    return out


def _load_param_formats(config: Any) -> set[str]:
    if not isinstance(config, dict):
        return {"yaml", "yml", "json"}
    raw = config.get("formats")
    if not isinstance(raw, list):
        return {"yaml", "yml", "json"}
    formats = {str(item).lower().lstrip(".") for item in raw if str(item).strip()}
    if "yaml" in formats:
        formats.add("yml")
    if "yml" in formats:
        formats.add("yaml")
    return formats


def _load_param_file(
    value: str,
    *,
    formats: set[str],
    base_dir: Path,
    op: str,
    stage_id: Any,
    param_path: str,
    expected_type: str,
) -> Any:
    if not value.strip():
        raise ValueError(_param_error(op, stage_id, param_path, "file path is empty"))
    path = Path(value)
    if not path.is_absolute():
        path = base_dir / path
    suffix = path.suffix.lower().lstrip(".")
    if suffix not in formats:
        raise ValueError(
            _param_error(
                op,
                stage_id,
                param_path,
                f"unsupported file format for {value!r}; expected one of "
                f"{sorted(formats)}",
            )
        )
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise FileNotFoundError(
            _param_error(op, stage_id, param_path, f"file not found: {path}")
        ) from exc
    try:
        loaded = json.loads(text) if suffix == "json" else yaml.safe_load(text)
    except (json.JSONDecodeError, yaml.YAMLError) as exc:
        raise ValueError(
            _param_error(op, stage_id, param_path, f"cannot parse {path}: {exc}")
        ) from exc

    _validate_loaded_value(
        loaded,
        expected_type=expected_type,
        op=op,
        stage_id=stage_id,
        param_path=param_path,
        source_path=path,
    )
    return loaded


def _validate_loaded_param_value(
    value: Any,
    *,
    spec: dict[str, Any],
    op: str,
    stage_id: Any,
    param_path: str,
) -> None:
    _validate_loaded_value(
        value,
        expected_type=str(spec.get("type") or ""),
        op=op,
        stage_id=stage_id,
        param_path=param_path,
        source_path=None,
    )


def _validate_loaded_value(
    value: Any,
    *,
    expected_type: str,
    op: str,
    stage_id: Any,
    param_path: str,
    source_path: Path | None,
) -> None:
    if expected_type != "mapping":
        return
    if isinstance(value, dict):
        return
    suffix = f" loaded from {source_path}" if source_path is not None else ""
    raise ValueError(
        _param_error(
            op,
            stage_id,
            param_path,
            f"expected mapping{suffix}, got {type(value).__name__}",
        )
    )


def _param_error(op: str, stage_id: Any, param_path: str, message: str) -> str:
    stage = f" stage {stage_id!r}" if isinstance(stage_id, str) and stage_id else ""
    return f"Component {op!r}{stage} {param_path}: {message}"
