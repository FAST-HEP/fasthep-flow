from __future__ import annotations

from pathlib import Path
from typing import Any

from hepflow.compiler.profiles import (
    load_profile_config_with_provenance,
    normalize_profile_names,
)
from hepflow.model.lifecycle import WHEN_ALIASES


def resolve_author_execution(
    author: dict[str, Any],
    *,
    author_path: Path,
) -> dict[str, Any]:
    project_root = author_path.parent
    use_block = author.get("use") or {}
    if not isinstance(use_block, dict):
        raise ValueError("use must be a mapping")

    profile_names = normalize_profile_names(use_block.get("profiles"))
    layers: list[dict[str, Any]] = [
        {
            "name": "builtin",
            "kind": "builtin",
            "execution": {
                "backend": "local",
                "strategy": "default",
                "config": {},
            },
        }
    ]
    for name in profile_names:
        config, provenance = load_profile_config_with_provenance(
            name,
            project_root=project_root,
        )
        layers.append(
            {
                "name": name,
                "kind": "profile",
                "path": provenance["path"],
                "execution": dict(config.get("execution") or {}),
            }
        )
    layers.append(
        {
            "name": "author",
            "kind": "author",
            "path": str(author_path),
            "execution": dict(author.get("execution") or {}),
        }
    )

    merged = _merge_execution_layers(layers)
    return {
        "execution": merged,
        "provenance": {
            "execution_layers": _provenance_layers(layers),
        },
    }


def resolve_author_execution_hooks(
    author: dict[str, Any],
    *,
    author_path: Path,
) -> dict[str, Any]:
    project_root = author_path.parent
    use_block = author.get("use") or {}
    if not isinstance(use_block, dict):
        raise ValueError("use must be a mapping")

    profile_names = normalize_profile_names(use_block.get("profiles"))
    layers: list[dict[str, Any]] = [
        {
            "name": "builtin",
            "kind": "builtin",
            "execution_hooks": [],
        }
    ]
    for name in profile_names:
        config, provenance = load_profile_config_with_provenance(
            name,
            project_root=project_root,
        )
        layers.append(
            {
                "name": name,
                "kind": "profile",
                "path": provenance["path"],
                "execution_hooks": list(config.get("execution_hooks") or []),
            }
        )
    layers.append(
        {
            "name": "author",
            "kind": "author",
            "path": str(author_path),
            "execution_hooks": list(author.get("execution_hooks") or []),
        }
    )

    return {
        "execution_hooks": _merge_execution_hook_layers(layers),
        "provenance": {
            "execution_hook_layers": _provenance_layers(layers),
        },
    }


def _merge_execution_layers(layers: list[dict[str, Any]]) -> dict[str, Any]:
    merged: dict[str, Any] = {
        "backend": "local",
        "strategy": "default",
        "config": {},
    }
    for layer in layers:
        execution = dict(layer.get("execution") or {})
        config = dict(execution.pop("config", {}) or {})
        merged.update(
            {key: value for key, value in execution.items() if value is not None}
        )
        merged["config"] = {
            **dict(merged.get("config") or {}),
            **config,
        }
    return {
        "backend": str(merged.get("backend") or "local"),
        "strategy": str(merged.get("strategy") or "default"),
        "config": dict(merged.get("config") or {}),
    }


def _merge_execution_hook_layers(layers: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[tuple[Any, ...], dict[str, Any]] = {}
    order: list[tuple[Any, ...]] = []

    for layer in layers:
        source = _hook_source(layer)
        for raw_hook in list(layer.get("execution_hooks") or []):
            if not isinstance(raw_hook, dict):
                raise ValueError("execution_hooks entries must be mappings")
            hook = dict(raw_hook)
            kind = str(hook.get("kind") or "")
            if not kind:
                raise ValueError("execution_hooks entries must define non-empty 'kind'")
            events = [
                WHEN_ALIASES.get(str(event).strip(), str(event).strip())
                for event in list(hook.get("events") or [])
            ]
            params = dict(hook.get("params") or {})
            hook["kind"] = kind
            hook["events"] = events
            if params:
                hook["params"] = params
            else:
                hook.pop("params", None)
            hook["source"] = source
            match = hook.get("match")
            key = (
                kind,
                tuple(events),
                _freeze_for_key(params),
                _freeze_for_key(match),
            )
            if key not in merged:
                order.append(key)
            merged[key] = hook

    return [merged[key] for key in order]


def _provenance_layers(layers: list[dict[str, Any]]) -> list[dict[str, str]]:
    provenance_layers: list[dict[str, str]] = []
    for layer in layers:
        item = {
            "name": str(layer["name"]),
            "kind": str(layer["kind"]),
        }
        if layer.get("path") is not None:
            item["path"] = str(layer["path"])
        provenance_layers.append(item)
    return provenance_layers


def _hook_source(layer: dict[str, Any]) -> str:
    kind = str(layer.get("kind") or "")
    name = str(layer.get("name") or "")
    if kind == "profile":
        return f"profile:{name}"
    return kind or name


def _freeze_for_key(value: Any) -> Any:
    if isinstance(value, dict):
        return tuple((key, _freeze_for_key(value[key])) for key in sorted(value))
    if isinstance(value, list):
        return tuple(_freeze_for_key(item) for item in value)
    return value
