from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class RuntimeComponentSpec:
    name: str
    kind: str
    version: str | None = None
    input: dict[str, Any] = field(default_factory=dict)
    params: dict[str, Any] = field(default_factory=dict)
    result: dict[str, Any] = field(default_factory=dict)
    dependencies: dict[str, Any] = field(default_factory=dict)
    requires: dict[str, Any] = field(default_factory=dict)
    provides: dict[str, Any] = field(default_factory=dict)
    lifecycle: dict[str, Any] = field(default_factory=dict)
    context_outputs: list[str] = field(default_factory=list)

    @classmethod
    def from_obj(cls, obj: Any) -> RuntimeComponentSpec:
        if isinstance(obj, cls):
            return obj
        if not isinstance(obj, dict):
            raise TypeError(
                "Runtime component spec must be a mapping or RuntimeComponentSpec, "
                f"got {type(obj).__name__}"
            )

        name = obj.get("name")
        if not isinstance(name, str) or not name:
            raise ValueError("Runtime component spec requires non-empty string 'name'")
        kind = obj.get("kind")
        if not isinstance(kind, str) or not kind:
            raise ValueError("Runtime component spec requires non-empty string 'kind'")

        version = obj.get("version")
        if version is not None:
            version = str(version)
        context_outputs = obj.get("context_outputs") or []
        if not isinstance(context_outputs, list) or not all(
            isinstance(item, str) and item for item in context_outputs
        ):
            raise ValueError(
                "Runtime component spec 'context_outputs' must be a list of strings"
            )

        return cls(
            name=name,
            kind=kind,
            version=version,
            input=dict(obj.get("input") or {}),
            params=dict(obj.get("params") or {}),
            result=dict(obj.get("result") or {}),
            dependencies=dict(obj.get("dependencies") or {}),
            requires=dict(obj.get("requires") or {}),
            provides=dict(obj.get("provides") or {}),
            lifecycle=dict(obj.get("lifecycle") or {}),
            context_outputs=list(context_outputs),
        )
