from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import yaml

from hepflow.compiler.hooks.model import CompileHookResult, ParamCompileHookContext


def load_mapping(
    *,
    value: Any,
    options: dict[str, Any],
    context: ParamCompileHookContext,
) -> CompileHookResult:
    if isinstance(value, dict):
        return CompileHookResult(
            value=dict(value),
            provenance={
                "hook": "flow.load_mapping",
                "input_kind": "mapping",
                "output_kind": "mapping",
            },
        )
    if not isinstance(value, str):
        raise TypeError(
            f"flow.load_mapping expected mapping or file path string, "
            f"got {type(value).__name__}"
        )

    source = value.strip()
    if not source:
        raise ValueError("flow.load_mapping file path is empty")

    path = Path(source)
    if not path.is_absolute():
        if context.workflow_dir is None:
            raise ValueError(
                "flow.load_mapping cannot resolve relative path without workflow_dir"
            )
        path = Path(context.workflow_dir) / path

    suffix = path.suffix.lower().lstrip(".")
    formats = _declared_formats(options)
    if suffix not in formats:
        raise ValueError(
            f"flow.load_mapping unsupported file format for {source!r}; "
            f"expected one of {sorted(formats)}"
        )

    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"flow.load_mapping file not found: {path}") from exc

    try:
        loaded = json.loads(text) if suffix == "json" else yaml.safe_load(text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"flow.load_mapping cannot parse JSON {path}: {exc}") from exc
    except yaml.YAMLError as exc:
        raise ValueError(f"flow.load_mapping cannot parse YAML {path}: {exc}") from exc

    return CompileHookResult(
        value=loaded,
        provenance={
            "hook": "flow.load_mapping",
            "input": source,
            "format": suffix,
            "resolved_path": str(path),
            "sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
            "output_kind": "mapping" if isinstance(loaded, dict) else type(loaded).__name__,
        },
    )


def _declared_formats(options: dict[str, Any]) -> set[str]:
    raw = options.get("formats")
    if not isinstance(raw, list) or not raw:
        raise ValueError("flow.load_mapping requires non-empty formats list")
    formats = {str(item).lower().lstrip(".") for item in raw if str(item).strip()}
    if not formats:
        raise ValueError("flow.load_mapping requires non-empty formats list")
    return formats
