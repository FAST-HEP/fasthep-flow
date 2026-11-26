"""Restart: take a partition's boundary products from published sets.

Only for the same plan (the recorded ``plan_hash`` must match) and only when every
boundary product of the partition is persisted and the plan has no sink or observer
whose side effects a skipped partition would lose. Anything else runs the producers.
A set that fails validation (missing manifest, payload or evidence, changed size or
sha256) is never reused.
"""

from __future__ import annotations

from pathlib import Path

from hepflow.model.persistence import PersistenceSettings
from hepflow.model.plan import ExecutionPartition, ExecutionPlan
from hepflow.model.products import BoundaryOutputSpec, BoundaryProduct
from hepflow.registry.runtime import RuntimeRegistry
from hepflow.runtime.persistence.errors import ManifestNotPublished, PersistenceError
from hepflow.runtime.persistence.evidence import EvidenceLedger
from hepflow.runtime.persistence.restore import restore_manifest
from hepflow.runtime.persistence.save import (
    persistence_root,
    product_folder,
    provisional_plan_hash,
)
from hepflow.runtime.persistence.select import PersistenceSelection
from hepflow.runtime.provenance.store import ProvenanceStore

_SIDE_EFFECT_ROLES = {"sink", "observer"}


def reuse_partition_products(
    plan: ExecutionPlan,
    partition: ExecutionPartition,
    *,
    boundary: list[BoundaryOutputSpec],
    selection: PersistenceSelection,
    settings: PersistenceSettings,
    registry: RuntimeRegistry,
    outdir: str | Path,
    ledger: EvidenceLedger,
    store: ProvenanceStore | None,
) -> list[BoundaryProduct] | None:
    """The partition's restored boundary products, or ``None`` to run it."""
    if not settings.reuse or not boundary:
        return None
    if any(node.role in _SIDE_EFFECT_ROLES for node in plan.nodes):
        return None
    if {spec.key() for spec in boundary} != set(selection.persisted):
        return None
    root = persistence_root(settings, outdir)
    plan_hash = provisional_plan_hash(plan)
    products: list[BoundaryProduct] = []
    for spec in boundary:
        folder = product_folder(
            root, spec.node_id, spec.output_name, partition.dataset, partition.id
        )
        manifest_path = folder / "manifest.json"
        try:
            restored = restore_manifest(
                manifest_path, registry, ledger=ledger, store=store
            )
        except (ManifestNotPublished, PersistenceError):
            return None
        if restored.manifest.plan_hash != plan_hash:
            return None
        [value] = restored.products.values()
        products.append(
            BoundaryProduct(
                node_id=spec.node_id,
                output_name=spec.output_name,
                kind=spec.kind,
                dataset=partition.dataset,
                partition_id=partition.id,
                representation=spec.policy.representation,
                value=value,
                metadata={
                    "reasons": list(spec.reasons),
                    "persisted": str(manifest_path),
                    "restored": True,
                },
            )
        )
    return products
