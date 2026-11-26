"""Restore published products by their recorded storage binding."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from hepflow.model.persistence import PersistenceCapability
from hepflow.model.product_manifest import ProductEntry, ProductManifest
from hepflow.registry.runtime import RuntimeRegistry
from hepflow.runtime.persistence.errors import (
    KindMismatch,
    MissingReader,
    UnknownCapability,
    UnsupportedVersion,
    VariantMismatch,
)
from hepflow.runtime.persistence.evidence import (
    EvidenceLedger,
    import_evidence,
    record_consumption,
)
from hepflow.runtime.persistence.manifest import read_manifest
from hepflow.runtime.provenance.store import ProvenanceStore

ProductKey = tuple[str, str, str | None, str]


@dataclass(slots=True)
class RestoredProducts:
    """Restored values keyed by (node, output, dataset, scope), plus the evidence."""

    manifest: ProductManifest
    products: dict[ProductKey, Any] = field(default_factory=dict)
    evidence: dict[str, Any] = field(default_factory=dict)
    ledger: EvidenceLedger = field(default_factory=EvidenceLedger)


def restore_manifest(
    manifest_path: Path,
    registry: RuntimeRegistry,
    *,
    ledger: EvidenceLedger | None = None,
    store: ProvenanceStore | None = None,
) -> RestoredProducts:
    """Validate the manifest, resolve each reader exactly, verify payloads, load.

    The reader is found by the recorded capability name and version in ``registry``;
    registry defaults never replace the recorded binding. Hooks do not run.
    """
    manifest_path = Path(manifest_path)
    manifest = read_manifest(manifest_path)
    folder = manifest_path.parent
    restored = RestoredProducts(
        manifest=manifest,
        evidence=json.loads((folder / manifest.evidence).read_text(encoding="utf-8")),
    )
    for entry in manifest.products:
        capability = resolve_reader(entry, registry)
        payloads = [folder / payload.uri for payload in entry.payloads]
        value = capability.load(  # type: ignore[misc]
            folder,
            payloads,
            params=dict(entry.storage.load.params),
            variant=entry.storage.variant,
            locator=entry.storage.locator,
        )
        key = (entry.producer_node, entry.output_name, entry.dataset_name, entry.scope)
        restored.products[key] = value
    ledger = ledger if ledger is not None else EvidenceLedger()
    import_evidence(ledger, restored.evidence, store=store)
    record_consumption(
        ledger,
        manifest=str(manifest_path),
        identities=[
            {"producer_node": e.producer_node, "output_name": e.output_name}
            for e in manifest.products
        ],
    )
    restored.ledger = ledger
    return restored


def resolve_reader(
    entry: ProductEntry, registry: RuntimeRegistry
) -> PersistenceCapability:
    binding = entry.storage
    identity = {
        "producer_node": entry.producer_node,
        "output_name": entry.output_name,
        "kind": entry.product_kind,
    }
    capability = _find(registry, binding.capability)
    if capability is None:
        raise UnknownCapability(
            f"capability {binding.capability!r} is not registered", **identity
        )
    if capability.version != binding.capability_version:
        raise UnsupportedVersion(
            f"capability {binding.capability!r} is registered at version "
            f"{capability.version!r}, the product was written with "
            f"{binding.capability_version!r}",
            **identity,
        )
    if capability.kind != entry.product_kind:
        raise KindMismatch(
            f"capability {binding.capability!r} stores kind {capability.kind!r}",
            **identity,
        )
    if capability.load is None:
        raise MissingReader(
            f"capability {binding.capability!r} has no load implementation", **identity
        )
    if binding.variant is not None and binding.variant not in capability.variants:
        raise VariantMismatch(
            f"variant {binding.variant!r} is not one of {list(capability.variants)}",
            **identity,
        )
    return capability


def _find(registry: RuntimeRegistry, name: str) -> PersistenceCapability | None:
    for handler in registry.product_handlers.values():
        if name in handler.persistence:
            return handler.persistence[name]
    return None
