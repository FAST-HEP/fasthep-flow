"""Restart: take a partition's boundary products from published sets.

Only for the same plan (the recorded ``plan_hash`` must match) and only when every
boundary product of the partition is persisted and the plan has no sink or observer
whose side effects a skipped partition would lose. Anything else runs the producers.
A set that fails validation (missing manifest, payload or evidence, changed size or
sha256) is never reused.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

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


@dataclass
class ReuseReport:
    """Per partition: reused, or skipped with a reason code and detail.

    Reasons: ``reuse_off``, ``no_boundary_products``, ``execution_hooks``,
    ``sink_or_observer``, ``products_not_all_persisted``, ``not_published``,
    ``plan_hash_differs``, ``identity_differs``, ``validation_failed``.
    """

    reused: list[str] = field(default_factory=list)
    skipped: list[dict[str, str]] = field(default_factory=list)

    def add_reused(self, partition_id: str) -> None:
        self.reused.append(partition_id)

    def add_skipped(self, partition_id: str, reason: str, detail: str = "") -> None:
        self.skipped.append(
            {"partition": partition_id, "reason": reason, "detail": detail}
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "attempted": len(self.reused) + len(self.skipped),
            "reused": len(self.reused),
            "skipped": list(self.skipped),
        }


def _hook_label(hook: Any) -> str:
    if isinstance(hook, dict):
        return str(hook.get("kind") or hook.get("name") or hook)
    return str(hook)


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
    plan_hash: str | None = None,
    report: ReuseReport | None = None,
) -> list[BoundaryProduct] | None:
    """The partition's restored boundary products, or ``None`` to run it.

    Two phases: every set is read and checked first (manifest, plan hash, identity), then
    loaded into scratch state; evidence reaches ``ledger`` and ``store`` only when every
    product of the partition can be reused.
    """
    if report is None:
        report = ReuseReport()
    reason: list[str] = []

    def skip(code: str, detail: str = "") -> None:
        reason[:] = [code, detail]

    products = _reuse(
        plan,
        partition,
        boundary=boundary,
        selection=selection,
        settings=settings,
        registry=registry,
        outdir=outdir,
        ledger=ledger,
        store=store,
        plan_hash=plan_hash,
        skip=skip,
    )
    if products is None:
        report.add_skipped(partition.id, *(reason or ["validation_failed"]))
    else:
        report.add_reused(partition.id)
    return products


def _blocked(
    plan: ExecutionPlan,
    boundary: list[BoundaryOutputSpec],
    selection: PersistenceSelection,
    settings: PersistenceSettings,
) -> tuple[str, str] | None:
    """``(reason, detail)`` when the plan or settings rule reuse out before any manifest is read."""
    if not settings.reuse:
        return ("reuse_off", "")
    if not boundary:
        return ("no_boundary_products", "")
    if plan.execution_hooks:  # a skipped partition would skip its hooks
        return (
            "execution_hooks",
            ", ".join(_hook_label(h) for h in plan.execution_hooks),
        )
    effects = [n.id for n in plan.nodes if n.role in _SIDE_EFFECT_ROLES]
    if effects:  # a skipped partition would skip their side effects
        return ("sink_or_observer", ", ".join(effects))
    if {spec.key() for spec in boundary} != set(selection.persisted):
        return ("products_not_all_persisted", "")
    return None


def _reuse(
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
    plan_hash: str | None,
    skip: Any,
) -> list[BoundaryProduct] | None:
    blocked = _blocked(plan, boundary, selection, settings)
    if blocked is not None:
        return skip(*blocked)
    root = persistence_root(settings, outdir)
    plan_hash = plan_hash or provisional_plan_hash(plan)
    paths: list[tuple[BoundaryOutputSpec, Path]] = []
    try:
        for spec in boundary:
            folder = product_folder(
                root, spec.node_id, spec.output_name, partition.dataset, partition.id
            )
            manifest = read_manifest(folder / "manifest.json")
            if manifest.plan_hash != plan_hash:
                return skip("plan_hash_differs")
            if len(manifest.products) != 1:
                return skip("identity_differs", f"{len(manifest.products)} products")
            entry = manifest.products[0]
            if (
                entry.producer_node,
                entry.output_name,
                entry.dataset_name,
                entry.product_kind,
            ) != (spec.node_id, spec.output_name, partition.dataset, spec.kind):
                return skip("identity_differs", f"{spec.node_id}.{spec.output_name}")
            paths.append((spec, folder / "manifest.json"))
        restored = [
            restore_manifest(
                path, registry, ledger=EvidenceLedger(), store=ProvenanceStore()
            )
            for _, path in paths
        ]
    except ManifestNotPublished as exc:
        return skip("not_published", str(exc))
    except (PersistenceError, OSError, ValueError, KeyError) as exc:
        return skip("validation_failed", f"{type(exc).__name__}: {exc}")
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
