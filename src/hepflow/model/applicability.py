from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

SUPPORTED_EVENTTYPES = frozenset({"data", "mc"})
EMPTY_APPLICABILITY = {"empty": True}
STAGE_APPLIES_TO_KEYS = ("eventtype", "dataset_groups")


@dataclass(frozen=True)
class _Dimension:
    """One thing a node can be restricted by: a list-valued key of the applicability metadata and the dataset-record field it is matched against."""

    key: str
    record_field: str
    lower: bool = False


# A new dataset attribute is one more row here (and, for stage ``applies_to``, an entry in STAGE_APPLIES_TO_KEYS).
_DIMENSIONS = (
    _Dimension("eventtypes", "eventtype", lower=True),
    _Dimension("datasets", "name"),
    _Dimension("dataset_groups", "group"),
)


def normalize_node_applicability(raw: Any, *, where: str) -> dict[str, Any] | None:
    """Normalize dataset applicability for workflow nodes: ``eventtype`` and ``dataset_groups``."""
    if raw is None:
        return None
    if not isinstance(raw, Mapping):
        raise ValueError(f"{where}.applies_to must be a mapping")

    unknown = sorted(str(key) for key in raw if key not in STAGE_APPLIES_TO_KEYS)
    if unknown:
        raise ValueError(
            f"{where}.applies_to only supports {', '.join(STAGE_APPLIES_TO_KEYS)} "
            f"in this release; unsupported keys: {unknown}"
        )
    if not raw:
        raise ValueError(
            f"{where}.applies_to needs at least one of {list(STAGE_APPLIES_TO_KEYS)}"
        )

    result: dict[str, Any] = {}
    if "eventtype" in raw:
        eventtype = str(raw.get("eventtype") or "").lower()
        if eventtype not in SUPPORTED_EVENTTYPES:
            raise ValueError(
                f"{where}.applies_to.eventtype only supports "
                f"{sorted(SUPPORTED_EVENTTYPES)!r} in this release"
            )
        result["eventtype"] = eventtype
    if "dataset_groups" in raw:
        result["dataset_groups"] = _normalize_dataset_groups(
            raw["dataset_groups"], where=f"{where}.applies_to.dataset_groups"
        )
    return result


def _normalize_dataset_groups(raw: Any, *, where: str) -> list[str]:
    values = [raw] if isinstance(raw, str) else raw
    if not isinstance(values, list) or not values:
        raise ValueError(f"{where} must be a non-empty list of group names")
    if not all(isinstance(item, str) for item in values):
        raise ValueError(f"{where} entries must be strings")
    groups = [item.strip() for item in values]
    if any(not group for group in groups):
        raise ValueError(f"{where} entries must be non-empty")
    return sorted(set(groups))


def node_applies_to_dataset(
    applicability: Any,
    *,
    dataset: dict[str, Any] | None,
) -> bool:
    constraints = _applicability_constraints(applicability)
    if constraints["empty"]:
        return False
    dataset = dataset or {}
    for dimension in _DIMENSIONS:
        allowed = constraints[dimension.key]
        if allowed is None:
            continue
        value = str(dataset.get(dimension.record_field) or "")
        if dimension.lower:
            value = value.lower()
        if value not in allowed:
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

    result: dict[str, Any] = {}
    for dimension in _DIMENSIONS:
        values = _intersect_dimension(left[dimension.key], right[dimension.key])
        if values == frozenset():
            return dict(EMPTY_APPLICABILITY)
        if values is not None:
            result[dimension.key] = sorted(values)
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
    none = {dimension.key: None for dimension in _DIMENSIONS}
    if applicability is None:
        return {**none, "empty": False}
    if not isinstance(applicability, Mapping):
        raise ValueError("node applicability metadata must be a mapping")
    if applicability.get("empty") is True:
        return {
            **{dimension.key: frozenset() for dimension in _DIMENSIONS},
            "empty": True,
        }

    constraints: dict[str, Any] = {"empty": False}
    for dimension in _DIMENSIONS:
        if dimension.key == "eventtypes":
            constraints[dimension.key] = _eventtype_constraint(applicability)
        else:
            constraints[dimension.key] = _string_set_constraint(
                applicability.get(dimension.key), key=dimension.key
            )
    return constraints


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
