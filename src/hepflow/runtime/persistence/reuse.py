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
from hepflow.runtime.persistence.evidence import (
    EvidenceLedger,
    import_evidence,
    record_consumption,
)
from hepflow.runtime.persistence.manifest import read_manifest
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
    """The partition's restored boundary products, or ``None`` to run it.

    Two phases: every set is read and checked first (manifest, plan hash, identity), then
    loaded into scratch state; evidence reaches ``ledger`` and ``store`` only when every
    product of the partition can be reused.
    """
    if not settings.reuse or not boundary:
        return None
    if plan.execution_hooks:
        return None  # a skipped partition would skip its hooks
    if any(node.role in _SIDE_EFFECT_ROLES for node in plan.nodes):
        return None
    if {spec.key() for spec in boundary} != set(selection.persisted):
        return None
    root = persistence_root(settings, outdir)
    plan_hash = provisional_plan_hash(plan)
    paths: list[tuple[BoundaryOutputSpec, Path]] = []
    try:
        for spec in boundary:
            folder = product_folder(
                root, spec.node_id, spec.output_name, partition.dataset, partition.id
            )
            manifest = read_manifest(folder / "manifest.json")
            if manifest.plan_hash != plan_hash or len(manifest.products) != 1:
                return None
            entry = manifest.products[0]
            if (
                entry.producer_node,
                entry.output_name,
                entry.dataset_name,
                entry.product_kind,
            ) != (spec.node_id, spec.output_name, partition.dataset, spec.kind):
                return None
            paths.append((spec, folder / "manifest.json"))
        restored = [
            restore_manifest(
                path, registry, ledger=EvidenceLedger(), store=ProvenanceStore()
            )
            for _, path in paths
        ]
    except (ManifestNotPublished, PersistenceError, OSError, ValueError, KeyError):
        return None
    products: list[BoundaryProduct] = []
    for (spec, path), result in zip(paths, restored, strict=True):
        import_evidence(ledger, result.evidence, store=store)
        record_consumption(
            ledger,
            manifest=str(path),
            identities=[
                {"producer_node": spec.node_id, "output_name": spec.output_name}
            ],
        )
        [value] = result.products.values()
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
                    "persisted": str(path),
                    "restored": True,
                },
            )
        )
    return products
