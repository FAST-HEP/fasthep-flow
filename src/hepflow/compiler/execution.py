from __future__ import annotations

from pathlib import Path
from typing import Any

from hepflow.compiler.profiles import (
    load_profile_config_with_provenance,
    normalize_profile_names,
)
from hepflow.model.execution import ExecutionConfig, StageExecutionConfig
from hepflow.model.lifecycle import WHEN_ALIASES


def normalize_global_execution(raw: Any) -> dict[str, Any]:
    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        raise ValueError("execution must be a mapping")

    backend = raw.get("backend", "local")
    if not isinstance(backend, str):
        raise ValueError("execution.backend must be a string")
    strategy = raw.get("strategy", "default")
    if not isinstance(strategy, str):
        raise ValueError("execution.strategy must be a string")

    profiles_raw = raw.get("profiles", [])
    if not isinstance(profiles_raw, list):
        raise ValueError("execution.profiles must be a list of strings")
    profiles = _list_of_strings(profiles_raw, "execution.profiles")

    resources_raw = raw.get("resources", {})
    if not isinstance(resources_raw, dict):
        raise ValueError("execution.resources must be a mapping")
    resources: dict[str, dict[str, Any]] = {}
    for resource_name, resource_raw in resources_raw.items():
        if not isinstance(resource_name, str) or not resource_name.strip():
            raise ValueError("execution.resources keys must be non-empty strings")
        if not isinstance(resource_raw, dict):
            raise ValueError(
                f"execution.resources[{resource_name!r}] must be a mapping"
            )
        resource: dict[str, Any] = {}
        for key, value in resource_raw.items():
            if not isinstance(key, str) or not key.strip():
                raise ValueError(
                    f"execution.resources[{resource_name!r}] keys must be non-empty strings"
                )
            if key == "gpus" and not _is_resource_value(value):
                raise ValueError(
                    f"execution.resources[{resource_name!r}].gpus must be an integer or string"
                )
            resource[key] = value
        resources[resource_name] = resource

    config_raw = raw.get("config", {})
    if not isinstance(config_raw, dict):
        raise ValueError("execution.config must be a mapping")
    config = dict(config_raw)

    environment_raw = raw.get("environment", {})
    if not isinstance(environment_raw, dict):
        raise ValueError("execution.environment must be a mapping")
    environment = dict(environment_raw)

    pools = _normalize_execution_pools(
        raw.get("pools"),
        resources=resources,
        config=config,
    )

    return ExecutionConfig(
        backend=backend.strip() or "local",
        strategy=strategy.strip() or "default",
        profiles=profiles,
        resources=resources,
        pools=pools,
        environment=environment,
        config=config,
    ).to_dict()


def normalize_stage_execution(raw: Any) -> dict[str, Any] | None:
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise ValueError("stage execution must be a mapping")

    require = _optional_string(raw.get("require"), "stage execution.require")
    prefer = _optional_string(raw.get("prefer"), "stage execution.prefer")
    if require is not None and prefer is not None:
        raise ValueError("stage execution cannot define both require and prefer")
    fallback = _optional_string(raw.get("fallback"), "stage execution.fallback")

    timeout = raw.get("timeout")
    if timeout is not None and not isinstance(timeout, str | int):
        raise ValueError("stage execution.timeout must be a string or integer")

    modifiers_raw = raw.get("modifiers", [])
    if not isinstance(modifiers_raw, list):
        raise ValueError("stage execution.modifiers must be a list of strings")

    return StageExecutionConfig(
        require=require,
        prefer=prefer,
        fallback=fallback,
        timeout=timeout,
        modifiers=_list_of_strings(modifiers_raw, "stage execution.modifiers"),
    ).to_dict()


def validate_stage_execution_resource_references(
    stages: list[Any],
    resources: dict[str, dict[str, Any]],
    pools: dict[str, dict[str, Any]] | None = None,
) -> None:
    resource_classes_with_pools = {
        str(pool.get("resources"))
        for pool in dict(pools or {}).values()
        if isinstance(pool, dict) and pool.get("resources") is not None
    }
    for idx, stage_raw in enumerate(stages):
        if not isinstance(stage_raw, dict):
            continue
        stage_id = str(stage_raw.get("id") or idx)
        execution = stage_raw.get("execution")
        if execution is None:
            continue
        if not isinstance(execution, dict):
            raise ValueError(f"analysis.stages[{idx}].execution must be a mapping")
        for field_name in ("require", "prefer", "fallback"):
            resource_name = execution.get(field_name)
            if resource_name is None:
                continue
            if str(resource_name) not in resources:
                raise ValueError(
                    f"analysis.stages[{idx}] ({stage_id!r}) execution.{field_name} "
                    f"references unknown resource class {resource_name!r}"
                )
            if pools and str(resource_name) not in resource_classes_with_pools:
                raise ValueError(
                    f"analysis.stages[{idx}] ({stage_id!r}) execution.{field_name} "
                    f"references resource class {resource_name!r}, but no execution pool "
                    "provides it"
                )


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
                "profiles": [],
                "resources": {},
                "pools": {},
                "environment": {},
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
            "execution": normalize_global_execution(author.get("execution")),
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
        "profiles": [],
        "resources": {},
        "pools": {},
        "environment": {},
        "config": {},
    }
    for layer in layers:
        execution = normalize_global_execution(layer.get("execution"))
        config = dict(execution.pop("config", {}) or {})
        resources = dict(execution.pop("resources", {}) or {})
        pools = dict(execution.pop("pools", {}) or {})
        environment = dict(execution.pop("environment", {}) or {})
        profiles = list(execution.pop("profiles", []) or [])
        merged.update(
            {key: value for key, value in execution.items() if value is not None}
        )
        merged["profiles"] = profiles
        merged["resources"] = {
            **dict(merged.get("resources") or {}),
            **resources,
        }
        merged["pools"] = {
            **dict(merged.get("pools") or {}),
            **pools,
        }
        merged["environment"] = {
            **dict(merged.get("environment") or {}),
            **environment,
        }
        merged["config"] = {
            **dict(merged.get("config") or {}),
            **config,
        }
    return normalize_global_execution(merged)


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


def _list_of_strings(raw: list[Any], where: str) -> list[str]:
    out: list[str] = []
    for idx, item in enumerate(raw):
        if not isinstance(item, str) or not item.strip():
            raise ValueError(f"{where}[{idx}] must be a non-empty string")
        out.append(item.strip())
    return out


def _optional_string(raw: Any, where: str) -> str | None:
    if raw is None:
        return None
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError(f"{where} must be a non-empty string")
    return raw.strip()


def _is_resource_value(value: Any) -> bool:
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return True
    return isinstance(value, str) and bool(value.strip())


def _normalize_execution_pools(
    raw: Any,
    *,
    resources: dict[str, dict[str, Any]],
    config: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    if raw is None:
        workers = config.get("workers", config.get("n_workers"))
        if workers is None:
            return {}
        if "default" not in resources:
            resources["default"] = {}
        return {
            "default": {
                "resources": "default",
                "workers": _normalize_pool_workers(
                    workers,
                    "execution.pools.default.workers",
                ),
                "config": {},
            }
        }

    if not isinstance(raw, dict):
        raise ValueError("execution.pools must be a mapping")

    pools: dict[str, dict[str, Any]] = {}
    for pool_name, pool_raw in raw.items():
        if not isinstance(pool_name, str) or not pool_name.strip():
            raise ValueError("execution.pools keys must be non-empty strings")
        if not isinstance(pool_raw, dict):
            raise ValueError(f"execution.pools[{pool_name!r}] must be a mapping")

        resource_name = pool_raw.get("resources")
        if not isinstance(resource_name, str) or not resource_name.strip():
            raise ValueError(
                f"execution.pools[{pool_name!r}].resources must be a string"
            )
        resource_name = resource_name.strip()
        if resource_name not in resources:
            if resource_name == "default" and not resources:
                resources["default"] = {}
            else:
                raise ValueError(
                    f"execution.pools[{pool_name!r}] references missing resource "
                    f"class {resource_name!r}"
                )

        workers = pool_raw.get("workers")
        config_raw = pool_raw.get("config", {})
        if not isinstance(config_raw, dict):
            raise ValueError(f"execution.pools[{pool_name!r}].config must be a mapping")

        pool: dict[str, Any] = {
            "resources": resource_name,
            "workers": None,
            "config": dict(config_raw),
        }
        if workers is not None:
            pool["workers"] = _normalize_pool_workers(
                workers,
                f"execution.pools[{pool_name!r}].workers",
            )
        pools[pool_name.strip()] = pool
    return pools


def _normalize_pool_workers(raw: Any, where: str) -> int:
    if isinstance(raw, bool):
        raise ValueError(f"{where} must be a positive integer")
    try:
        workers = int(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{where} must be a positive integer") from exc
    if workers <= 0:
        raise ValueError(f"{where} must be a positive integer")
    return workers
