from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Literal

BoundaryRepresentation = Literal["value", "reference", "materialize"]


@dataclass(frozen=True, slots=True)
class ProductBoundaryPolicy:
    retain: bool = False
    representation: BoundaryRepresentation = "value"

    def to_dict(self) -> dict[str, Any]:
        return {
            "retain": self.retain,
            "representation": self.representation,
        }


@dataclass(frozen=True, slots=True)
class BoundaryOutputSpec:
    node_id: str
    output_name: str
    kind: str
    policy: ProductBoundaryPolicy = field(default_factory=ProductBoundaryPolicy)
    reasons: tuple[str, ...] = ()

    def key(self) -> tuple[str, str]:
        return (self.node_id, self.output_name)

    def to_dict(self) -> dict[str, Any]:
        return {
            "node_id": self.node_id,
            "output_name": self.output_name,
            "kind": self.kind,
            "boundary": self.policy.to_dict(),
            "reasons": list(self.reasons),
        }


@dataclass(frozen=True, slots=True)
class BoundaryProduct:
    node_id: str
    output_name: str
    kind: str
    dataset: str | None
    partition_id: str | None
    representation: BoundaryRepresentation
    value: Any
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))

    def key(self) -> tuple[str, str]:
        return (self.node_id, self.output_name)


@dataclass(slots=True)
class ProductRef:
    name: str
    kind: str
    scope: str
    format: str
    path: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class OperationResult:
    products: dict[str, Any]
    product_refs: list[ProductRef]
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ProductHandlerEntry:
    merge: Callable[..., Any] | None = None
    materialize: Callable[..., Any] | None = None
    boundary: ProductBoundaryPolicy = field(default_factory=ProductBoundaryPolicy)
    boundary_materialize: Callable[..., Any] | None = None
