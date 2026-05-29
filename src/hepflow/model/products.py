from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any


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
