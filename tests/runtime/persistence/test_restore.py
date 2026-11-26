from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from tests.toy_components import persistence as toy

from hepflow.api import restore_products
from hepflow.model.persistence import PersistenceCapability, PersistenceSettings
from hepflow.model.plan import ExecutionPlan
from hepflow.model.product_manifest import ProductEntry
from hepflow.model.products import BoundaryProduct, ProductHandlerEntry
from hepflow.registry.runtime import RuntimeRegistry
from hepflow.runtime.persistence import errors
from hepflow.runtime.persistence.save import persist_boundary_products
from hepflow.runtime.persistence.select import PersistenceSelection


def _capability(**overrides: Any) -> PersistenceCapability:
    fields: dict[str, Any] = {
        "name": "toy.counts",
        "version": "1",
        "kind": "histogram",
        "container": "json",
        "payload_roles": ("data",),
        "fidelity": "exact",
        "save": toy.save_toy_counts,
        "load": toy.load_echo_params,
        "variants": (),
        "params": {"save": {"indent": 2}, "load": {"scale": 1}},
    }
    fields.update(overrides)
    return PersistenceCapability(**fields)


def _registry(*capabilities: PersistenceCapability) -> RuntimeRegistry:
    handlers: dict[str, ProductHandlerEntry] = {}
    for capability in capabilities:
        entry = handlers.get(capability.kind, ProductHandlerEntry())
        handlers[capability.kind] = ProductHandlerEntry(
            persistence={**entry.persistence, capability.name: capability}
        )
    return RuntimeRegistry(product_handlers=handlers)


def _publish(tmp_path: Path, capability: PersistenceCapability | None = None) -> Path:
    capability = capability or _capability()
    product = BoundaryProduct(
        node_id="stage.Fill",
        output_name="hist",
        kind="histogram",
        dataset="Zmumu",
        partition_id="p0",
        representation="persist",
        value={"entries": 3},
    )
    selection = PersistenceSelection(persisted={product.key(): capability})
    [saved] = persist_boundary_products(
        [product],
        selection,
        plan=ExecutionPlan(),
        settings=PersistenceSettings(mode="all"),
        outdir=tmp_path,
    )
    return Path(saved.metadata["persisted"])


def _edit_binding(manifest: Path, **changes: Any) -> None:
    data = json.loads(manifest.read_text(encoding="utf-8"))
    for key, value in changes.items():
        if value is None:
            data["products"][0]["storage"].pop(key)
        else:
            data["products"][0]["storage"][key] = value
    manifest.write_text(json.dumps(data), encoding="utf-8")


def _assert_identity(caught: errors.PersistenceError) -> None:
    assert (caught.producer_node, caught.output_name, caught.kind) == (
        "stage.Fill",
        "hist",
        "histogram",
    )


def test_restore_returns_the_value_and_the_evidence(tmp_path: Path) -> None:
    manifest = _publish(tmp_path)

    restored = restore_products(manifest, registry=_registry(_capability()))

    [(key, value)] = restored.products.items()
    assert key == ("stage.Fill", "hist", "Zmumu", "partition")
    assert value["value"] == {"entries": 3}


def test_unknown_capability(tmp_path: Path) -> None:
    manifest = _publish(tmp_path)

    with pytest.raises(errors.UnknownCapability) as caught:
        restore_products(manifest, registry=_registry())

    assert caught.value.code == "unknown_capability"
    _assert_identity(caught.value)


def test_unsupported_version_is_an_exact_match(tmp_path: Path) -> None:
    manifest = _publish(tmp_path)

    with pytest.raises(errors.UnsupportedVersion) as caught:
        restore_products(manifest, registry=_registry(_capability(version="2")))

    _assert_identity(caught.value)


def test_missing_reader(tmp_path: Path) -> None:
    manifest = _publish(tmp_path)

    with pytest.raises(errors.MissingReader) as caught:
        restore_products(manifest, registry=_registry(_capability(load=None)))

    _assert_identity(caught.value)


def test_invalid_binding(tmp_path: Path) -> None:
    manifest = _publish(tmp_path)
    _edit_binding(manifest, capability_version=None)

    with pytest.raises(errors.InvalidBinding) as caught:
        restore_products(manifest, registry=_registry(_capability()))

    _assert_identity(caught.value)


def test_kind_mismatch(tmp_path: Path) -> None:
    manifest = _publish(tmp_path)

    with pytest.raises(errors.KindMismatch) as caught:
        restore_products(manifest, registry=_registry(_capability(kind="cutflow")))

    _assert_identity(caught.value)


def test_variant_mismatch(tmp_path: Path) -> None:
    manifest = _publish(tmp_path)
    _edit_binding(manifest, variant="rntuple")

    with pytest.raises(errors.VariantMismatch) as caught:
        restore_products(manifest, registry=_registry(_capability(variants=("ttree",))))

    _assert_identity(caught.value)


def test_missing_payload_and_hash_errors_surface_through_restore(
    tmp_path: Path,
) -> None:
    manifest = _publish(tmp_path)
    (manifest.parent / "counts.json").write_text('{"entries": 4}', encoding="utf-8")

    with pytest.raises(errors.SizeOrHashMismatch):
        restore_products(manifest, registry=_registry(_capability()))


def test_missing_evidence(tmp_path: Path) -> None:
    manifest = _publish(tmp_path)
    (manifest.parent / "evidence.json").unlink()

    with pytest.raises(errors.MissingEvidence) as caught:
        restore_products(manifest, registry=_registry(_capability()))

    _assert_identity(caught.value)


def test_changed_registry_default_does_not_change_restore(tmp_path: Path) -> None:
    manifest = _publish(tmp_path)
    changed = _capability(params={"save": {"indent": 8}, "load": {"scale": 99}})

    restored = restore_products(manifest, registry=_registry(changed))

    [value] = restored.products.values()
    assert value["params"] == {"scale": 1}


def test_unpublished_set_is_not_restorable(tmp_path: Path) -> None:
    manifest = _publish(tmp_path)
    manifest.unlink()

    with pytest.raises(errors.ManifestNotPublished):
        restore_products(manifest, registry=_registry(_capability()))


def test_error_codes_match_the_schema() -> None:
    names = {
        cls.code
        for cls in vars(errors).values()
        if isinstance(cls, type)
        and issubclass(cls, errors.PersistenceError)
        and cls is not errors.PersistenceError
    }
    assert names == set(errors.ERROR_CODES)


def test_manifest_entry_is_a_product_entry(tmp_path: Path) -> None:
    from hepflow.runtime.persistence.manifest import read_manifest  # noqa: PLC0415

    manifest = read_manifest(_publish(tmp_path))
    assert isinstance(manifest.products[0], ProductEntry)
