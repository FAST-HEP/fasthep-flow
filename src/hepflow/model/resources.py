from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class ResolvedResource:
    """Runtime-only resource value plus serialisable resolution metadata."""

    id: str
    kind: str
    value: Any
    requested_era: str | None = None
    selected_era: str | None = None
    path: str | None = None
    correction: str | None = None
    fallback: bool = False
    reason: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


__all__ = ["ResolvedResource"]
