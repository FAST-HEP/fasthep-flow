"""The result manifest of a published product set (``docs/execution/persistence.md``)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from hepflow.model.persistence import StorageBinding

PRODUCT_MANIFEST_SCHEMA_VERSION = "1"


@dataclass(frozen=True, slots=True)
class PayloadRef:
    """One payload file: a location relative to the manifest folder, size, checksum."""

    uri: str
    size_bytes: int
    sha256: str
    role: str | None = None

    def to_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {
            "uri": self.uri,
            "size_bytes": self.size_bytes,
            "sha256": self.sha256,
        }
        if self.role is not None:
            data["role"] = self.role
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> PayloadRef:
        return cls(
            uri=str(data["uri"]),
            size_bytes=int(data["size_bytes"]),
            sha256=str(data["sha256"]),
            role=data.get("role"),
        )


@dataclass(frozen=True, slots=True)
class ProductEntry:
    """One product instance in a manifest."""

    producer_node: str
    output_name: str
    product_kind: str
    scope: str
    storage: StorageBinding
    payloads: tuple[PayloadRef, ...]
    dataset_name: str | None = None

    def to_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {
            "producer_node": self.producer_node,
            "output_name": self.output_name,
            "product_kind": self.product_kind,
            "scope": self.scope,
            "storage": self.storage.to_dict(),
            "payloads": [payload.to_dict() for payload in self.payloads],
        }
        if self.dataset_name is not None:
            data["dataset_name"] = self.dataset_name
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ProductEntry:
        return cls(
            producer_node=str(data["producer_node"]),
            output_name=str(data["output_name"]),
            product_kind=str(data["product_kind"]),
            scope=str(data["scope"]),
            dataset_name=data.get("dataset_name"),
            storage=StorageBinding.from_dict(dict(data["storage"])),
            payloads=tuple(PayloadRef.from_dict(dict(p)) for p in data["payloads"]),
        )


@dataclass(frozen=True, slots=True)
class ProductManifest:
    """``manifest.json`` of a product set; its presence is publication."""

    plan_hash: str
    root: str
    products: tuple[ProductEntry, ...]
    evidence: str
    evidence_sha256: str = ""
    schema_version: str = PRODUCT_MANIFEST_SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "plan_hash": self.plan_hash,
            "root": self.root,
            "products": [product.to_dict() for product in self.products],
            "evidence": self.evidence,
            "evidence_sha256": self.evidence_sha256,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ProductManifest:
        return cls(
            schema_version=str(data.get("schema_version") or ""),
            plan_hash=str(data["plan_hash"]),
            root=str(data["root"]),
            products=tuple(ProductEntry.from_dict(dict(p)) for p in data["products"]),
            evidence=str(data["evidence"]),
            evidence_sha256=str(data.get("evidence_sha256", "")),
        )
