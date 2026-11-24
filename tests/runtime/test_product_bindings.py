"""Typed product bindings: built from materialized items, read as sink input mappings."""

from __future__ import annotations

import dataclasses
from typing import Any

import pytest

from hepflow.model.plan import ExecutionNode, ExecutionPlan
from hepflow.runtime.products import (
    ProductBinding,
    product_bindings,
    register_product_bindings,
)

ITEM = {
    "node_id": "stage.Hist",
    "output_name": "hist",
    "product_kind": "histogram",
    "id": "Hist",
    "path": "artifacts/histograms/Hist.pkl",
    "producer": "stage.Hist",
    "entries": 3,
}


def _plan(scope: str = "partition") -> ExecutionPlan:
    plan = ExecutionPlan()
    plan.add_node(
        ExecutionNode(
            id="stage.Hist",
            graph_node_id="stage.Hist",
            role="transform",
            impl="toy.hist",
            outputs={"hist": "histogram"},
            output_scope=scope,  # type: ignore[arg-type]
        )
    )
    return plan


def test_a_binding_is_built_from_a_materialized_item() -> None:
    binding = ProductBinding.from_item(ITEM, scope="partition")

    assert (binding.node_id, binding.port, binding.kind) == ("stage.Hist", "hist", "histogram")
    assert (binding.id, binding.path, binding.producer) == (
        "Hist",
        "artifacts/histograms/Hist.pkl",
        "stage.Hist",
    )
    assert binding.scope == "partition"
    assert binding.reference is None
    assert dict(binding.metadata) == {"entries": 3}
    assert binding.key() == ("stage.Hist", "hist")


def test_a_binding_is_frozen_and_its_metadata_is_read_only() -> None:
    binding = ProductBinding.from_item(ITEM, scope="global")

    with pytest.raises(dataclasses.FrozenInstanceError):
        binding.path = "x"  # type: ignore[misc]
    with pytest.raises(TypeError):
        binding.metadata["entries"] = 4  # type: ignore[index]


def test_the_input_product_mapping_has_the_keys_sinks_read() -> None:
    assert ProductBinding.from_item(ITEM, scope="global").to_input_product() == {
        "node_id": "stage.Hist",
        "port": "hist",
        "kind": "histogram",
        "path": "artifacts/histograms/Hist.pkl",
        "id": "Hist",
        "producer": "stage.Hist",
    }


def test_registering_binds_items_with_the_producing_nodes_scope() -> None:
    ctx: dict[str, Any] = {}
    skipped = {"output_name": "x", "id": "x", "path": "p", "producer": "q"}

    register_product_bindings(ctx, [ITEM, skipped], plan=_plan("partition"))

    assert list(product_bindings(ctx)) == [("stage.Hist", "hist")]
    assert ctx["product_bindings"][("stage.Hist", "hist")].scope == "partition"


def test_an_untyped_binding_in_the_context_is_an_error() -> None:
    ctx: dict[str, Any] = {"product_bindings": {("a", "b"): {"node_id": "a"}}}

    with pytest.raises(TypeError, match="must be a ProductBinding"):
        product_bindings(ctx)
    with pytest.raises(TypeError, match="must be a mapping"):
        product_bindings({"product_bindings": []})
