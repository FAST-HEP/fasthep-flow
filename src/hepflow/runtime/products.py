"""Product bindings: where a materialized final product lives, as seen by the nodes that read it.

A binding ties an output port of a plan node (``node_id``, ``port``) to the product Flow wrote for
it. ``ctx["product_bindings"]`` maps ``(node_id, port)`` to a frozen :class:`ProductBinding`; the
engine registers the bindings after final products are materialized and gives each node the
bindings of its inputs as ``ctx["input_products"]``, a plain mapping per input name (see
:meth:`ProductBinding.to_input_product`), which is what sinks read.

Bindings describe a product and never carry its value. They hold no persistence-format fields: a
persisted schema (the Snakemake export, flow#114) extends this class later.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any

from hepflow.model.plan import ExecutionPlan, Scope

PRODUCT_BINDINGS_KEY = "product_bindings"

_ITEM_FIELDS = {"node_id", "output_name", "product_kind", "path", "id", "producer", "reference"}


@dataclass(frozen=True, slots=True)
class ProductBinding:
    """One materialized product of one output port.

    ``path`` is the outdir-relative, absolute or URI location Flow stored; ``reference`` is set
    instead when the product is held by reference. ``scope`` is the output scope of the producing
    node; ``dataset`` and ``partition_id`` are set for a product of one partition.
    ``metadata`` is whatever else the product handler reported, apart from the values.
    """

    node_id: str
    port: str
    kind: str
    producer: str
    id: str
    path: str = ""
    reference: str | None = None
    scope: Scope = "global"
    dataset: str | None = None
    partition_id: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))

    def key(self) -> tuple[str, str]:
        return (self.node_id, self.port)

    def to_input_product(self) -> dict[str, str]:
        """The mapping a sink reads in ``ctx["input_products"]``."""
        return {
            "node_id": self.node_id,
            "port": self.port,
            "kind": self.kind,
            "path": self.path,
            "id": self.id,
            "producer": self.producer,
        }

    @classmethod
    def from_item(cls, item: Mapping[str, Any], *, scope: Scope) -> ProductBinding:
        """Build a binding from a materialized manifest item (see ``materialize_final_products``)."""
        node_id = str(item["node_id"])
        reference = item.get("reference")
        return cls(
            node_id=node_id,
            port=str(item["output_name"]),
            kind=str(item.get("product_kind") or ""),
            producer=str(item.get("producer") or node_id),
            id=str(item.get("id") or ""),
            path=str(item.get("path") or ""),
            reference=str(reference) if reference else None,
            scope=scope,
            metadata={k: v for k, v in item.items() if k not in _ITEM_FIELDS},
        )


def product_bindings(ctx: dict[str, Any]) -> dict[tuple[str, str], ProductBinding]:
    """The bindings registered in ``ctx`` (empty when none), checked to be typed."""
    bindings = ctx.get(PRODUCT_BINDINGS_KEY)
    if bindings is None:
        return {}
    if not isinstance(bindings, dict):
        raise TypeError("Runtime context product_bindings must be a mapping")
    for key, binding in bindings.items():
        if not isinstance(binding, ProductBinding):
            raise TypeError(
                f"Runtime context product_bindings[{key!r}] must be a ProductBinding, "
                f"got {type(binding).__name__}"
            )
    return bindings


def register_product_bindings(
    ctx: dict[str, Any],
    items: list[dict[str, Any]],
    *,
    plan: ExecutionPlan,
) -> None:
    """Bind every materialized item to its ``(node_id, port)``; items without both are skipped."""
    product_bindings(ctx)
    bindings: dict[tuple[str, str], ProductBinding] = ctx.setdefault(
        PRODUCT_BINDINGS_KEY, {}
    )
    for item in items:
        node_id = item.get("node_id")
        output_name = item.get("output_name")
        if not isinstance(node_id, str) or not isinstance(output_name, str):
            continue
        node = plan.node_index.get(node_id)
        scope: Scope = node.output_scope if node is not None else "global"
        binding = ProductBinding.from_item(item, scope=scope)
        bindings[binding.key()] = binding
