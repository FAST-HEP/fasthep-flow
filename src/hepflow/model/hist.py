from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from .defaults import DEFAULT_HIST_STORAGE

_ALLOWED_AXIS_TYPES = {"regular", "category", "int", "bool"}
_ALLOWED_STORAGE = {"count", "weighted"}


@dataclass(frozen=True)
class HistAxis:
    name: str
    type: str
    source: str
    bins: dict[str, Any] | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name.strip():
            raise ValueError("HistAxis.name must be a non-empty string")
        if not isinstance(self.type, str) or not self.type.strip():
            raise ValueError("HistAxis.type must be a non-empty string")
        if not isinstance(self.source, str) or not self.source.strip():
            raise ValueError("HistAxis.source must be a non-empty string")

        t = self.type.strip().lower()
        if t == "boolean":
            t = "bool"
        if t not in _ALLOWED_AXIS_TYPES:
            raise ValueError(
                f"HistAxis.type must be one of {sorted(_ALLOWED_AXIS_TYPES)}, got {self.type!r}"
            )
        # normalize spelling via object identity (frozen dataclass)
        object.__setattr__(self, "type", t)

        if t == "regular":
            if self.bins is None or not isinstance(self.bins, dict):
                raise ValueError(
                    "HistAxis.bins must be a mapping for regular axes")
            low = self.bins.get("low")
            high = self.bins.get("high")
            if low is None or high is None:
                raise ValueError(
                    "HistAxis.bins for regular axes must include low and high")
            if "nbins" not in self.bins and "step" not in self.bins:
                raise ValueError(
                    "HistAxis.bins for regular axes must include nbins or step")
        else:
            # bins must not be required; allow None; if present, keep (future-proof) but you may forbid it
            pass

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class HistParams:
    axes: list[HistAxis]
    storage: str = DEFAULT_HIST_STORAGE
    weight_expr: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.axes, list) or not self.axes:
            raise ValueError("HistParams.axes must be a non-empty list")
        for a in self.axes:
            if not isinstance(a, HistAxis):
                raise TypeError(
                    "HistParams.axes entries must be HistAxis instances")

        if not isinstance(self.storage, str) or not self.storage.strip():
            raise ValueError("HistParams.storage must be a non-empty string")
        s = self.storage.strip().lower()
        if s not in _ALLOWED_STORAGE:
            raise ValueError(
                f"HistParams.storage must be one of {sorted(_ALLOWED_STORAGE)}, got {self.storage!r}")
        object.__setattr__(self, "storage", s)

        if self.weight_expr is not None:
            if not isinstance(self.weight_expr, str) or not self.weight_expr.strip():
                raise ValueError(
                    "HistParams.weight_expr must be a non-empty string or None")

            # If weights exist, storage should be weighted (compiler may set this; model enforces consistency)
            if s != "weighted":
                raise ValueError(
                    "HistParams.weight_expr requires storage='weighted'")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
