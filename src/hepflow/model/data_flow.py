from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class DataDependencyResult:
    consumes: set[str] = field(default_factory=set)
    produces: set[str] = field(default_factory=set)

    def to_dict(self) -> dict[str, Any]:
        return {
            "consumes": sorted(self.consumes),
            "produces": sorted(self.produces),
        }
