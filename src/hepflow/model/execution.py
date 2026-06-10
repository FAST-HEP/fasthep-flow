from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(frozen=True)
class ExecutionConfig:
    backend: str = "local"
    strategy: str = "default"
    profiles: list[str] = field(default_factory=list)
    resources: dict[str, dict[str, Any]] = field(default_factory=dict)
    config: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class StageExecutionConfig:
    require: str | None = None
    prefer: str | None = None
    fallback: str | None = None
    timeout: str | int | None = None
    modifiers: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class NodeResourceIntent:
    require: str | None = None
    prefer: str | None = None
    fallback: str | None = None
    required_resources: dict[str, Any] | None = None
    preferred_resources: dict[str, Any] | None = None
    fallback_resources: dict[str, Any] | None = None

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
        required_resources=_resource_dict(resources, require),
        preferred_resources=_resource_dict(resources, prefer),
        fallback_resources=_resource_dict(resources, fallback),
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
