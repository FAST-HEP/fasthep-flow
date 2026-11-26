"""Write and read a published product set.

Payloads are written first, then ``evidence.json``, then ``manifest.json`` through a
temporary file in the same folder and a rename. The manifest is the commit marker:
without it nothing was published.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from fasthep_toolbench.misc import file_sha256

from hepflow.model.product_manifest import (
    PRODUCT_MANIFEST_SCHEMA_VERSION,
    PayloadRef,
    ProductEntry,
    ProductManifest,
)
from hepflow.runtime.persistence.errors import (
    InvalidBinding,
    ManifestNotPublished,
    MissingEvidence,
    MissingPayload,
    SizeOrHashMismatch,
)

MANIFEST_NAME = "manifest.json"
EVIDENCE_NAME = "evidence.json"


def describe_payload(
    folder: Path, relative: str, *, role: str | None = None
) -> PayloadRef:
    """Size and checksum of ``folder / relative`` as a payload reference."""
    path = folder / relative
    return PayloadRef(
        uri=Path(relative).as_posix(),
        size_bytes=path.stat().st_size,
        sha256=file_sha256(path),
        role=role,
    )


def write_product_set(
    folder: Path,
    products: list[ProductEntry],
    evidence: dict[str, Any],
    *,
    plan_hash: str,
    root: str | None = None,
) -> Path:
    """Publish ``products`` (payloads already in ``folder``); return the manifest path."""
    folder.mkdir(parents=True, exist_ok=True)
    _write_json(folder / EVIDENCE_NAME, evidence)
    manifest = ProductManifest(
        plan_hash=plan_hash,
        root=root if root is not None else str(folder),
        products=tuple(products),
        evidence=EVIDENCE_NAME,
    )
    final = folder / MANIFEST_NAME
    temporary = folder / f".{MANIFEST_NAME}.tmp-{os.getpid()}"
    try:
        temporary.write_text(
            json.dumps(manifest.to_dict(), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temporary.replace(final)
    finally:
        if temporary.exists():
            temporary.unlink()
    return final


def read_manifest(path: Path) -> ProductManifest:
    """Read a manifest and check its payloads and evidence file.

    Relative payload and evidence locations are read against the manifest's folder.
    """
    path = Path(path)
    if not path.is_file():
        raise ManifestNotPublished(f"no manifest at {path}: nothing was published")
    manifest = ProductManifest.from_dict(json.loads(path.read_text(encoding="utf-8")))
    folder = path.parent
    if manifest.schema_version != PRODUCT_MANIFEST_SCHEMA_VERSION:
        first = manifest.products[0] if manifest.products else None
        raise InvalidBinding(
            f"manifest schema_version {manifest.schema_version!r} is not "
            f"{PRODUCT_MANIFEST_SCHEMA_VERSION!r}",
            producer_node=first.producer_node if first else "",
            output_name=first.output_name if first else "",
            kind=first.product_kind if first else "",
        )
    for product in manifest.products:
        _check_payloads(folder, product)
    if manifest.products and not (folder / manifest.evidence).is_file():
        first = manifest.products[0]
        raise MissingEvidence(
            f"evidence file {manifest.evidence!r} is missing",
            producer_node=first.producer_node,
            output_name=first.output_name,
            kind=first.product_kind,
        )
    return manifest


def _check_payloads(folder: Path, product: ProductEntry) -> None:
    identity = {
        "producer_node": product.producer_node,
        "output_name": product.output_name,
        "kind": product.product_kind,
    }
    for payload in product.payloads:
        path = folder / payload.uri
        if not path.is_file():
            raise MissingPayload(f"payload {payload.uri!r} is missing", **identity)
        size = path.stat().st_size
        if size != payload.size_bytes:
            raise SizeOrHashMismatch(
                f"payload {payload.uri!r} has size {size}, manifest says "
                f"{payload.size_bytes}",
                **identity,
            )
        if file_sha256(path) != payload.sha256:
            raise SizeOrHashMismatch(
                f"payload {payload.uri!r} differs from the recorded sha256",
                **identity,
            )


def _write_json(path: Path, data: dict[str, Any]) -> None:
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
