from __future__ import annotations

from typing import Any

import pytest

from hepflow.registry.loaders import runtime_registry_from_config

TOY = "tests.toy_components.persistence"


def _cfg(**overrides: Any) -> dict[str, Any]:
    fmt: dict[str, Any] = {
        "version": "1",
        "container": "json",
        "payload_roles": ["data"],
        "fidelity": "exact counts",
        "save": f"{TOY}:save_toy_counts",
        "load": f"{TOY}:load_toy_counts",
    }
    fmt.update(overrides)
    fmt = {k: v for k, v in fmt.items() if v is not None}
    return {
        "product_handlers": {
            "toy_counts": {"persistence": {"formats": {"toy.counts": fmt}}}
        }
    }


def test_capability_loaded_from_registry() -> None:
    registry = runtime_registry_from_config(_cfg(variants=["plain"]))

    capability = registry.product_handlers["toy_counts"].persistence["toy.counts"]
    assert (capability.name, capability.version) == ("toy.counts", "1")
    assert capability.kind == "toy_counts"
    assert capability.container == "json"
    assert capability.payload_roles == ("data",)
    assert capability.variants == ("plain",)
    assert callable(capability.save)
    assert callable(capability.load)


@pytest.mark.parametrize(
    "missing", ["version", "container", "payload_roles", "fidelity"]
)
def test_descriptor_missing_required_field_fails(missing: str) -> None:
    with pytest.raises(ValueError, match=rf"toy_counts.*toy\.counts.*{missing}"):
        runtime_registry_from_config(_cfg(**{missing: None}))


def test_kind_without_persistence_still_loads() -> None:
    registry = runtime_registry_from_config(
        {"product_handlers": {"plain": {"merge": f"{TOY}:save_toy_counts"}}}
    )
    assert registry.product_handlers["plain"].persistence == {}


def test_save_must_be_callable() -> None:
    with pytest.raises(TypeError, match=r"toy\.counts.*save"):
        runtime_registry_from_config(_cfg(save="os:sep"))


def test_capability_without_load_registers() -> None:
    registry = runtime_registry_from_config(_cfg(load=None))
    assert (
        registry.product_handlers["toy_counts"].persistence["toy.counts"].load is None
    )


def test_declared_kind_must_match_the_handler() -> None:
    with pytest.raises(ValueError, match="kind"):
        runtime_registry_from_config(_cfg(kind="other"))
