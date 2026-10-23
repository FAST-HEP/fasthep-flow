from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict
from pathlib import Path
from typing import Any

from hepflow.compiler.input_paths import ResolvedInput, resolve_path_value
from hepflow.model.component_spec import RuntimeComponentSpec
from hepflow.registry.loaders import load_runtime_spec_and_impl


def apply_component_param_defaults(
    normalized: dict[str, Any],
    *,
    root: Path | None = None,
    entry_dir: Path | None = None,
) -> dict[str, Any]:
    """Materialize declarative component parameter normalization in stages.

    With a ``root``, params whose spec entry has ``kind: path`` are input paths: they are
    resolved against the root (see :mod:`hepflow.compiler.input_paths`) and recorded in
    ``provenance.inputs``. Params without that marker (for example output paths) are not
    touched.
    """
    out = deepcopy(normalized)
    entries: list[ResolvedInput] = []
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
        role = str(item.get("role") or "transform")
        spec = _component_spec(registry, role, op)
        if spec is not None:
            params = dict(item.get("params") or {})
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
            if root is not None:
                item["params"] = _with_resolved_paths(
                    params,
                    spec.params,
                    stage_id=str(stage_id),
                    root=root,
                    entry_dir=entry_dir,
                    entries=entries,
                )
        normalized_stages.append(item)

    analysis["stages"] = normalized_stages
    if entries:
        provenance = out.setdefault("provenance", {})
        provenance.setdefault("inputs", []).extend(asdict(entry) for entry in entries)
    return out


def _with_resolved_paths(
    params: dict[str, Any],
    schema: dict[str, Any],
    *,
    stage_id: str,
    root: Path,
    entry_dir: Path | None,
    entries: list[ResolvedInput],
) -> dict[str, Any]:
    out = dict(params)
    for name, cfg in schema.items():
        if not isinstance(cfg, dict) or cfg.get("kind") != "path":
            continue
        if out.get(name) is None:
            continue
        out[name], resolved = resolve_path_value(
            out[name],
            role=f"param:{stage_id}.{name}",
            root=root,
            entry_dir=entry_dir,
            label=f"stage {stage_id!r} param {name!r}",
        )
        entries.extend(resolved)
    return out


def _component_spec(
    registry: dict[str, Any],
    role: str,
    op: str,
) -> RuntimeComponentSpec | None:
    registry_section = "sinks" if role == "sink" else "transforms"
    entries = registry.get(registry_section)
    if not isinstance(entries, dict) or op not in entries:
        return None
    try:
        spec, _impl = load_runtime_spec_and_impl(registry, registry_section, op)
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
