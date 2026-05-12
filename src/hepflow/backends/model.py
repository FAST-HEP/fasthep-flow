from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

from hepflow.model.plan import ExecutionPlan


@dataclass(slots=True)
class BackendResult:
    backend: str
    strategy: str
    success: bool
    outputs: dict[str, Any] = field(default_factory=dict)
    summary: dict[str, Any] = field(default_factory=dict)


class Backend(Protocol):
    name: str

    def run(
        self,
        plan: ExecutionPlan,
        *,
        ctx: dict[str, Any] | None = None,
    ) -> BackendResult:
        ...
