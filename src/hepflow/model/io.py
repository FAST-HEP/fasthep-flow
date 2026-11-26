from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Any, Literal

from hepflow.compiler.input_paths import is_uri
from hepflow.model.persistence import StorageBinding

PathType = Literal["relative_to_outdir", "absolute", "uri"]

DELIVERY_STATUSES = ("ok", "missing", "size_mismatch", "checksum_mismatch", "error")


@dataclass(slots=True)
class ArtifactReference:
    """
    A produced artifact.

    ``uri`` (also ``path``) is where the artifact is. A sink returns a path usable
    from its working directory, an absolute path or a URI; once Flow has stored the
    result it is a path relative to the outdir for files inside it, an absolute path
    for files outside it, or a URI (``scheme://``) unchanged. ``path_type`` is derived from
    that string. ``size_bytes`` is set for every local file Flow could stat, ``adler32``
    only when a delivery destination is configured, ``delivery`` is the per-file outcome
    the backend reports (one of ``DELIVERY_STATUSES``).
    """

    uri: str
    product_kind: str
    metadata: dict[str, Any] = field(default_factory=dict)
    provenance: dict[str, Any] = field(default_factory=dict)
    producer_node: str | None = None
    output_name: str | None = None
    dataset_name: str | None = None
    partition_id: str | None = None
    partition_index: int | None = None
    size_bytes: int | None = None
    adler32: str | None = None
    delivery: str | None = None
    storage: StorageBinding | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.uri, str) or not self.uri.strip():
            raise ValueError("ArtifactReference.uri is required")
        if self.delivery is not None and self.delivery not in DELIVERY_STATUSES:
            raise ValueError(
                f"ArtifactReference.delivery must be one of {DELIVERY_STATUSES}; "
                f"got {self.delivery!r}"
            )
        if not isinstance(self.product_kind, str) or not self.product_kind.strip():
            raise ValueError("ArtifactReference.product_kind is required")
        self.uri = self.uri.strip()
        self.product_kind = self.product_kind.strip()
        self.metadata = dict(self.metadata or {})
        self.provenance = dict(self.provenance or {})
        if self.producer_node is not None:
            self.producer_node = str(self.producer_node)
        if self.output_name is not None:
            self.output_name = str(self.output_name)
        if self.dataset_name is not None:
            self.dataset_name = str(self.dataset_name)
        if self.partition_id is not None:
            self.partition_id = str(self.partition_id)

    @property
    def kind(self) -> str:
        return self.product_kind

    @kind.setter
    def kind(self, value: str) -> None:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("ArtifactReference.kind is required")
        self.product_kind = value.strip()

    @property
    def path(self) -> str:
        return self.uri

    @path.setter
    def path(self, value: str) -> None:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("ArtifactReference.path is required")
        self.uri = value.strip()

    @property
    def name(self) -> str:
        return Path(self.uri).name

    @property
    def path_type(self) -> PathType:
        """Classify ``uri``: a URI, an absolute path, or a path relative to the outdir."""
        if is_uri(self.uri):
            return "uri"
        if self.uri.startswith("/"):
            return "absolute"
        return "relative_to_outdir"

    def to_dict(self) -> dict[str, Any]:
        return _drop_none(
            {
                "type": "artifact_reference",
                "uri": self.uri,
                "product_kind": self.product_kind,
                "producer_node": self.producer_node,
                "output_name": self.output_name,
                "dataset_name": self.dataset_name,
                "partition_id": self.partition_id,
                "partition_index": self.partition_index,
                "path_type": self.path_type,
                "size_bytes": self.size_bytes,
                "adler32": self.adler32,
                "delivery": self.delivery,
                "storage": self.storage.to_dict() if self.storage else None,
                "metadata": dict(self.metadata),
                "provenance": dict(self.provenance),
            }
        )

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ArtifactReference:
        return cls(
            uri=str(data.get("uri") or data.get("path") or ""),
            product_kind=str(data.get("product_kind") or data.get("kind") or ""),
            metadata=dict(data.get("metadata") or {}),
            provenance=dict(data.get("provenance") or {}),
            producer_node=data.get("producer_node") or data.get("node_id"),
            output_name=data.get("output_name"),
            dataset_name=data.get("dataset_name") or data.get("dataset"),
            partition_id=data.get("partition_id"),
            partition_index=data.get("partition_index") or data.get("partition"),
            size_bytes=data.get("size_bytes"),
            adler32=data.get("adler32"),
            delivery=data.get("delivery"),
            storage=(
                StorageBinding.from_dict(dict(data["storage"]))
                if data.get("storage")
                else None
            ),
        )


class OutputResult(ArtifactReference):
    """Compatibility artifact reference using the historical kind/path names."""

    def __init__(
        self,
        *,
        kind: str,
        path: str,
        metadata: dict[str, Any] | None = None,
        provenance: dict[str, Any] | None = None,
        producer_node: str | None = None,
        output_name: str | None = None,
        dataset_name: str | None = None,
        partition_id: str | None = None,
        partition_index: int | None = None,
        size_bytes: int | None = None,
        adler32: str | None = None,
        delivery: str | None = None,
        storage: StorageBinding | None = None,
    ) -> None:
        super().__init__(
            uri=path,
            product_kind=kind,
            metadata=dict(metadata or {}),
            provenance=dict(provenance or {}),
            producer_node=producer_node,
            output_name=output_name,
            dataset_name=dataset_name,
            partition_id=partition_id,
            partition_index=partition_index,
            size_bytes=size_bytes,
            adler32=adler32,
            delivery=delivery,
            storage=storage,
        )


@dataclass(frozen=True, slots=True)
class ArtifactManifest:
    product_kind: str
    producer_node: str
    output_name: str
    dataset_name: str | None
    parts: tuple[ArtifactReference, ...]
    metadata: dict[str, Any] = field(default_factory=dict)
    root: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.product_kind, str) or not self.product_kind.strip():
            raise ValueError("ArtifactManifest.product_kind is required")
        if not isinstance(self.producer_node, str) or not self.producer_node.strip():
            raise ValueError("ArtifactManifest.producer_node is required")
        if not isinstance(self.output_name, str) or not self.output_name.strip():
            raise ValueError("ArtifactManifest.output_name is required")
        object.__setattr__(self, "product_kind", self.product_kind.strip())
        object.__setattr__(self, "producer_node", self.producer_node.strip())
        object.__setattr__(self, "output_name", self.output_name.strip())
        object.__setattr__(self, "parts", tuple(self.parts))
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))

    def to_dict(self) -> dict[str, Any]:
        return _drop_none(
            {
                "type": "artifact_manifest",
                "product_kind": self.product_kind,
                "producer_node": self.producer_node,
                "output_name": self.output_name,
                "dataset_name": self.dataset_name,
                "root": self.root,
                "metadata": dict(self.metadata),
                "parts": [part.to_dict() for part in self.parts],
            }
        )

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ArtifactManifest:
        return cls(
            product_kind=str(data.get("product_kind") or data.get("kind") or ""),
            producer_node=str(data.get("producer_node") or data.get("node_id") or ""),
            output_name=str(data.get("output_name") or ""),
            dataset_name=data.get("dataset_name") or data.get("dataset"),
            parts=tuple(
                item
                if isinstance(item, ArtifactReference)
                else ArtifactReference.from_dict(dict(item))
                for item in list(data.get("parts") or [])
            ),
            metadata=dict(data.get("metadata") or {}),
            root=data.get("root"),
        )


def _drop_none(data: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in data.items() if value is not None}
