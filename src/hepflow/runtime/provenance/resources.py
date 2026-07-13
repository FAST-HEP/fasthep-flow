from __future__ import annotations

import warnings
from dataclasses import dataclass, field
from typing import Any

from hepflow.runtime.provenance.model import ResolvedResourceRecord


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

    def to_record(self) -> ResolvedResourceRecord:
        return ResolvedResourceRecord(
            id=self.id,
            kind=self.kind,
            requested_era=self.requested_era,
            selected_era=self.selected_era,
            path=self.path,
            correction=self.correction,
            fallback=self.fallback,
            reason=self.reason,
            metadata=dict(self.metadata),
        )


def warn_resource_fallback(resource: ResolvedResource) -> None:
    reason = resource.reason or "requested resource is unavailable"
    warnings.warn(
        "Runtime resource fallback: "
        f"resource={resource.id}, "
        f"requested_era={resource.requested_era}, "
        f"selected_era={resource.selected_era}, "
        f"path={resource.path}, "
        f"correction={resource.correction}, "
        f"reason={reason}",
        RuntimeWarning,
        stacklevel=2,
    )


__all__ = [
    "ResolvedResource",
    "warn_resource_fallback",
]
