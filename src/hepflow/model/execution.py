from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(frozen=True)
class ExecutionConfig:
    backend: str = "local"
    strategy: str = "default"
    profiles: list[str] = field(default_factory=list)
    resources: dict[str, dict[str, Any]] = field(default_factory=dict)
    pools: dict[str, dict[str, Any]] = field(default_factory=dict)
    environment: dict[str, Any] = field(default_factory=dict)
    config: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ExecutionModifier:
    name: str
    params: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class StageExecutionConfig:
    require: str | None = None
    prefer: str | None = None
    fallback: str | None = None
    timeout: str | int | None = None
    modifiers: list[ExecutionModifier] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class NodeResourceIntent:
    require: str | None = None
    prefer: str | None = None
    fallback: str | None = None
    required_resource: dict[str, Any] | None = None
    preferred_resource: dict[str, Any] | None = None
    fallback_resource: dict[str, Any] | None = None
    candidate_pools: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def resolve_node_resource_intent(plan: Any, node: Any) -> NodeResourceIntent:
    if isinstance(node, str):
        node = plan.get_node(node)

    execution = dict(getattr(plan, "execution", {}) or {})
    resources = dict(execution.get("resources") or {})
    node_execution = dict((getattr(node, "meta", {}) or {}).get("execution") or {})

    require = _optional_resource_name(node_execution.get("require"))
    prefer = _optional_resource_name(node_execution.get("prefer"))
    fallback = _optional_resource_name(node_execution.get("fallback"))

    return NodeResourceIntent(
        require=require,
        prefer=prefer,
        fallback=fallback,
        required_resource=_resource_dict(resources, require),
        preferred_resource=_resource_dict(resources, prefer),
        fallback_resource=_resource_dict(resources, fallback),
        candidate_pools=_candidate_pools(execution, require, prefer, fallback),
    )


def _optional_resource_name(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _resource_dict(
    resources: dict[str, Any],
    resource_name: str | None,
) -> dict[str, Any] | None:
    if resource_name is None:
        return None
    resource = resources.get(resource_name)
    if resource is None:
        return None
    return dict(resource)


def _candidate_pools(
    execution: dict[str, Any],
    require: str | None,
    prefer: str | None,
    fallback: str | None,
) -> list[dict[str, Any]]:
    pools = dict(execution.get("pools") or {})
    resource_order = [item for item in (require, prefer, fallback) if item is not None]
    candidates: list[dict[str, Any]] = []
    seen: set[str] = set()
    for pool_name, pool_raw in pools.items():
        if not isinstance(pool_raw, dict):
            continue
        pool_resources = pool_raw.get("resources")
        if pool_resources not in resource_order or str(pool_name) in seen:
            continue
        seen.add(str(pool_name))
        candidates.append({"name": str(pool_name), **dict(pool_raw)})
    return candidates
