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
