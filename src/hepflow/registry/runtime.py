from __future__ import annotations

from dataclasses import dataclass, field

from hepflow.model.products import ProductHandlerEntry


@dataclass(frozen=True)
class RuntimeRegistry:
    """
    Loaded runtime registry.

    Contains concrete, executable objects resolved from the symbolic registry
    configuration stored in author/plan files.
    """

    product_handlers: dict[str, ProductHandlerEntry] = field(default_factory=dict)
