from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable


@dataclass(frozen=True)
class ExprRegistry:
    functions: dict[str, Callable[..., Any]] = field(default_factory=dict)
    constants: dict[str, Any] = field(default_factory=dict)

    def merged(self, other: "ExprRegistry") -> "ExprRegistry":
        return ExprRegistry(
            functions={**self.functions, **other.functions},
            constants={**self.constants, **other.constants},
        )

    def to_plan_dict(self) -> dict[str, Any]:
        # symbolic/serialisable form only; callables handled elsewhere
        raise NotImplementedError("Use resolved symbolic config for plan output")


