"""Save boundary products under their persistence capability and publish them."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import replace
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse
from urllib.request import url2pathname

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
        if parsed.netloc not in {"", "localhost"}:
            raise ValueError(
                f"persistence root {root!r} names a host {parsed.netloc!r}; use "
                "file:///absolute/path"
            )
        return Path(url2pathname(unquote(parsed.path)))
    path = Path(root)
    return path if path.is_absolute() else Path(outdir) / path


def provisional_plan_hash(plan: ExecutionPlan) -> str:
    """Hash of what decides a product, until the semantic plan hash of flow#105 exists.

    Nodes, partitions (file and event range), dataset metadata, the registry's product
    handlers, execution settings (not ``persistence``) and execution hooks. Code behind an
    unchanged ``impl`` name is not covered.
    """
    payload = json.dumps(
        {
            "nodes": [node.to_dict() for node in plan.nodes],
            "partitions": [partition.to_dict() for partition in plan.partitions],
            "datasets": plan.context.get("datasets"),
            "product_handlers": plan.registry.get("product_handlers"),
            "execution": {
                key: value
                for key, value in plan.execution.items()
                if key != "persistence"
            },
            "execution_hooks": plan.execution_hooks,
        },
        sort_keys=True,
        default=str,
    )
    return "sha256:" + hashlib.sha256(payload.encode("utf-8")).hexdigest()


def product_folder(
    root: Path,
    node_id: str,
    output_name: str,
    dataset: str | None,
    partition_id: str | None,
) -> Path:
    """``<root>/<node>/<output>/<dataset or _>/<partition>``, path-safe."""
    parts = (node_id, output_name, dataset or "_", partition_id or "_")
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
        folder = product_folder(
            root,
            product.node_id,
            product.output_name,
            product.dataset,
            product.partition_id,
        )
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
    (folder / "manifest.json").unlink(
        missing_ok=True
    )  # republishing starts unpublished
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
    who = {
        "producer_node": product.node_id,
        "output_name": product.output_name,
        "kind": product.kind,
    }
    forbid_credentials(evidence, where="evidence", **who)
    forbid_credentials(entry.to_dict(), where="manifest", **who)
    return write_product_set(folder, [entry], evidence, plan_hash=plan_hash, root=shown)


def _qualname(func: Any) -> str:
    if func is None:
        return "unavailable"
    return f"{getattr(func, '__module__', '?')}:{getattr(func, '__qualname__', '?')}"


_PLAIN = re.compile(r"[A-Za-z0-9._-]+")


def _safe(part: str) -> str:
    """A folder name for ``part``; names that needed changing carry a short hash.

    Different names never share a folder, and ``.``, ``..`` and the empty name are
    never used as they are.
    """
    if _PLAIN.fullmatch(part) and part not in {".", ".."}:
        return part
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", part).strip("._") or "_"
    return f"{cleaned}-{hashlib.sha256(part.encode('utf-8')).hexdigest()[:8]}"
