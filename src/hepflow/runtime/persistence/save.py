"""Save boundary products under their persistence capability and publish them."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import replace
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from hepflow.compiler.input_paths import is_uri
from hepflow.model.persistence import (
    PersistenceCapability,
    PersistenceSettings,
    StorageBinding,
    StorageCall,
)
from hepflow.model.plan import ExecutionPlan
from hepflow.model.product_manifest import ProductEntry
from hepflow.model.products import BoundaryProduct
from hepflow.runtime.persistence.evidence import build_evidence, forbid_credentials
from hepflow.runtime.persistence.manifest import describe_payload, write_product_set
from hepflow.runtime.persistence.select import PersistenceSelection
from hepflow.runtime.provenance.store import ProvenanceStore

DEFAULT_ROOT = Path("artifacts") / "products"


def persistence_root(settings: PersistenceSettings, outdir: str | Path) -> Path:
    """The folder product sets are published below (a local path or ``file://``)."""
    root = settings.root
    if root is None:
        return Path(outdir) / DEFAULT_ROOT
    if is_uri(root):
        parsed = urlparse(root)
        if parsed.scheme != "file":
            raise NotImplementedError(
                f"persistence root {root!r}: writing to {parsed.scheme}:// is a "
                "backend's job; use a local path or file:// root"
            )
        return Path(parsed.path)
    path = Path(root)
    return path if path.is_absolute() else Path(outdir) / path


def provisional_plan_hash(plan: ExecutionPlan) -> str:
    """Hash of the plan's nodes, until the semantic plan hash of flow#105 exists."""
    payload = json.dumps(
        [node.to_dict() for node in plan.nodes], sort_keys=True, default=str
    )
    return "sha256:" + hashlib.sha256(payload.encode("utf-8")).hexdigest()


def product_folder(root: Path, product: BoundaryProduct) -> Path:
    """``<root>/<node>/<output>/<dataset or _>/<partition>``, path-safe."""
    parts = (
        product.node_id,
        product.output_name,
        product.dataset or "_",
        product.partition_id or "_",
    )
    return root.joinpath(*(_safe(part) for part in parts))


def persist_boundary_products(
    products: list[BoundaryProduct],
    selection: PersistenceSelection,
    *,
    plan: ExecutionPlan,
    settings: PersistenceSettings,
    outdir: str | Path,
    store: ProvenanceStore | None = None,
    attempt_id: str = "attempt-1",
) -> list[BoundaryProduct]:
    """Save the selected products; return the products with their manifest recorded."""
    if not selection.persisted:
        return products
    root = persistence_root(settings, outdir)
    plan_hash = provisional_plan_hash(plan)
    saved: list[BoundaryProduct] = []
    for product in products:
        capability = selection.persisted.get(product.key())
        if capability is None or capability.save is None:
            saved.append(product)
            continue
        folder = product_folder(root, product)
        manifest = _save_one(
            product,
            capability,
            folder,
            plan_hash,
            store or ProvenanceStore(),
            attempt_id,
            outdir=outdir,
        )
        saved.append(
            replace(
                product,
                metadata={**product.metadata, "persisted": str(manifest)},
            )
        )
    return saved


def _save_one(
    product: BoundaryProduct,
    capability: PersistenceCapability,
    folder: Path,
    plan_hash: str,
    store: ProvenanceStore,
    attempt_id: str,
    *,
    outdir: str | Path,
) -> Path:
    assert capability.save is not None
    identity = {
        "producer_node": product.node_id,
        "output_name": product.output_name,
        "product_kind": product.kind,
        "dataset_name": product.dataset,
        "partition_id": product.partition_id,
    }
    save_params = dict(dict(capability.params).get("save") or {})
    load_params = dict(dict(capability.params).get("load") or {})
    forbid_credentials(
        {"save": save_params, "load": load_params},
        where="persistence params",
        producer_node=product.node_id,
        output_name=product.output_name,
        kind=product.kind,
    )
    outcome = capability.save(
        product.value, root=folder, params=save_params, identity=identity
    )
    entry = ProductEntry(
        producer_node=product.node_id,
        output_name=product.output_name,
        product_kind=product.kind,
        scope="partition" if product.partition_id else "dataset",
        dataset_name=product.dataset,
        storage=StorageBinding(
            capability=capability.name,
            capability_version=capability.version,
            container=capability.container,
            variant=outcome.variant,
            locator=outcome.locator,
            save=StorageCall(
                _qualname(capability.save), capability.version, save_params
            ),
            load=StorageCall(
                _qualname(capability.load), capability.version, load_params
            ),
        ),
        payloads=tuple(
            describe_payload(folder, payload.path, role=payload.role)
            for payload in outcome.payloads
        ),
    )
    try:
        shown = str(folder.relative_to(Path(outdir)))
    except ValueError:
        shown = str(folder)
    evidence = build_evidence(
        store, identity=identity, attempt_id=attempt_id, status="succeeded"
    )
    return write_product_set(folder, [entry], evidence, plan_hash=plan_hash, root=shown)


def _qualname(func: Any) -> str:
    if func is None:
        return "unavailable"
    return f"{getattr(func, '__module__', '?')}:{getattr(func, '__qualname__', '?')}"


def _safe(part: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", part).strip("_") or "_"
