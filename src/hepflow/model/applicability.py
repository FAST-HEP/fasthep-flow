from __future__ import annotations

from collections.abc import Mapping
from typing import Any

SUPPORTED_EVENTTYPES = frozenset({"data", "mc"})
EMPTY_APPLICABILITY = {"empty": True}


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
    constraints = _applicability_constraints(applicability)
    if constraints["empty"]:
        return False
    eventtypes = constraints["eventtypes"]
    datasets = constraints["datasets"]
    if eventtypes is None and datasets is None:
        return True
    dataset = dataset or {}
    if eventtypes is not None:
        eventtype = str(dataset.get("eventtype") or "").lower()
        if eventtype not in eventtypes:
            return False
    if datasets is not None:
        dataset_name = str(dataset.get("name") or "")
        if dataset_name not in datasets:
            return False
    return True


def node_applies_to_context(applicability: Any, *, ctx: dict[str, Any]) -> bool:
    dataset = ctx.get("dataset")
    return node_applies_to_dataset(
        applicability,
        dataset=dataset if isinstance(dataset, dict) else None,
    )


def intersect_applicability(original: Any, variation: Any) -> dict[str, Any] | None:
    """Return the canonical intersection of two applicability constraints."""
    left = _applicability_constraints(original)
    right = _applicability_constraints(variation)
    if left["empty"] or right["empty"]:
        return dict(EMPTY_APPLICABILITY)

    eventtypes = _intersect_dimension(left["eventtypes"], right["eventtypes"])
    if eventtypes == frozenset():
        return dict(EMPTY_APPLICABILITY)
    datasets = _intersect_dimension(left["datasets"], right["datasets"])
    if datasets == frozenset():
        return dict(EMPTY_APPLICABILITY)

    result: dict[str, Any] = {}
    if eventtypes is not None:
        result["eventtypes"] = sorted(eventtypes)
    if datasets is not None:
        result["datasets"] = sorted(datasets)
    return result or None


def applicability_is_empty(applicability: Any) -> bool:
    return _applicability_constraints(applicability)["empty"]


def _intersect_dimension(
    left: frozenset[str] | None,
    right: frozenset[str] | None,
) -> frozenset[str] | None:
    if left is None:
        return right
    if right is None:
        return left
    return left & right


def _applicability_constraints(applicability: Any) -> dict[str, Any]:
    if applicability is None:
        return {"eventtypes": None, "datasets": None, "empty": False}
    if not isinstance(applicability, Mapping):
        raise ValueError("node applicability metadata must be a mapping")
    if applicability.get("empty") is True:
        return {"eventtypes": frozenset(), "datasets": frozenset(), "empty": True}

    eventtypes = _eventtype_constraint(applicability)
    datasets = _string_set_constraint(applicability.get("datasets"), key="datasets")
    return {
        "eventtypes": eventtypes,
        "datasets": datasets,
        "empty": False,
    }


def _eventtype_constraint(applicability: Mapping[str, Any]) -> frozenset[str] | None:
    single = applicability.get("eventtype")
    multiple = applicability.get("eventtypes")
    if single is not None and multiple is not None:
        raise ValueError(
            "node applicability metadata must not mix eventtype and eventtypes"
        )
    if single is not None:
        eventtype = str(single or "").lower()
        if eventtype not in SUPPORTED_EVENTTYPES:
            raise ValueError(
                "node applicability metadata only supports "
                f"eventtype in {sorted(SUPPORTED_EVENTTYPES)!r} in this release"
            )
        return frozenset({eventtype})
    values = _string_set_constraint(multiple, key="eventtypes", lower=True)
    if values is None:
        return None
    unsupported = sorted(value for value in values if value not in SUPPORTED_EVENTTYPES)
    if unsupported:
        raise ValueError(
            "node applicability metadata only supports "
            f"eventtypes in {sorted(SUPPORTED_EVENTTYPES)!r} in this release"
        )
    return values


def _string_set_constraint(
    raw: Any,
    *,
    key: str,
    lower: bool = False,
) -> frozenset[str] | None:
    if raw is None:
        return None
    if not isinstance(raw, list):
        raise ValueError(f"node applicability metadata {key} must be a list")
    if not raw:
        return None
    values = [str(item).strip() for item in raw]
    if any(not value for value in values):
        raise ValueError(f"node applicability metadata {key} entries must be non-empty")
    if lower:
        values = [value.lower() for value in values]
    return frozenset(values)


__all__ = [
    "EMPTY_APPLICABILITY",
    "applicability_is_empty",
    "intersect_applicability",
    "node_applies_to_context",
    "node_applies_to_dataset",
    "normalize_node_applicability",
]
