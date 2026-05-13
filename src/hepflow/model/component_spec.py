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

        return cls(
            name=name,
            kind=kind,
            version=version,
            input=dict(obj.get("input") or {}),
            params=dict(obj.get("params") or {}),
            result=dict(obj.get("result") or {}),
            dependencies=dict(obj.get("dependencies") or {}),
        )
