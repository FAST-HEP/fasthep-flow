from __future__ import annotations

import warnings

from hepflow.model import ResolvedResource
from hepflow.runtime.provenance.model import ResolvedResourceRecord


def resolved_resource_record(resource: ResolvedResource) -> ResolvedResourceRecord:
    return ResolvedResourceRecord(
        id=resource.id,
        kind=resource.kind,
        requested_era=resource.requested_era,
        selected_era=resource.selected_era,
        path=resource.path,
        correction=resource.correction,
        fallback=resource.fallback,
        reason=resource.reason,
        metadata=dict(resource.metadata),
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
    "resolved_resource_record",
    "warn_resource_fallback",
]
