from __future__ import annotations

from collections.abc import Mapping
from typing import Any

SUPPORTED_EVENTTYPES = frozenset({"data", "mc"})


def normalize_node_applicability(raw: Any, *, where: str) -> dict[str, str] | None:
    """Normalize first-pass dataset applicability for workflow nodes."""
    if raw is None:
        return None
    if not isinstance(raw, Mapping):
        raise ValueError(f"{where}.applies_to must be a mapping")

    allowed = {"eventtype"}
    unknown = sorted(str(key) for key in raw if key not in allowed)
    if unknown:
        raise ValueError(
            f"{where}.applies_to only supports eventtype in this release; "
            f"unsupported keys: {unknown}"
        )

    eventtype = str(raw.get("eventtype") or "").lower()
    if eventtype not in SUPPORTED_EVENTTYPES:
        raise ValueError(
            f"{where}.applies_to.eventtype only supports "
            f"{sorted(SUPPORTED_EVENTTYPES)!r} in this release"
        )
    return {"eventtype": eventtype}


def node_applies_to_dataset(
    applicability: Any,
    *,
    dataset: dict[str, Any] | None,
) -> bool:
    if applicability is None:
        return True
    if not isinstance(applicability, Mapping):
        raise ValueError("node applicability metadata must be a mapping")
    eventtype = applicability.get("eventtype")
    if eventtype is None:
        return True
    eventtype = str(eventtype or "").lower()
    if eventtype not in SUPPORTED_EVENTTYPES:
        raise ValueError(
            "node applicability metadata only supports "
            f"eventtype in {sorted(SUPPORTED_EVENTTYPES)!r} in this release"
        )
    return str((dataset or {}).get("eventtype") or "").lower() == eventtype


def node_applies_to_context(applicability: Any, *, ctx: dict[str, Any]) -> bool:
    dataset = ctx.get("dataset")
    return node_applies_to_dataset(
        applicability,
        dataset=dataset if isinstance(dataset, dict) else None,
    )


__all__ = [
    "node_applies_to_context",
    "node_applies_to_dataset",
    "normalize_node_applicability",
]
