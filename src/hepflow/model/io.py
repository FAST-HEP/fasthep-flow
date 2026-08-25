from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Any


@dataclass(slots=True)
class ArtifactReference:
    uri: str
    product_kind: str
    format: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    provenance: dict[str, Any] = field(default_factory=dict)
    producer_node: str | None = None
    output_name: str | None = None
    dataset_name: str | None = None
    partition_id: str | None = None
    partition_index: int | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.uri, str) or not self.uri.strip():
            raise ValueError("ArtifactReference.uri is required")
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

    def to_dict(self) -> dict[str, Any]:
        return _drop_none(
            {
                "type": "artifact_reference",
                "uri": self.uri,
                "product_kind": self.product_kind,
                "format": self.format,
                "producer_node": self.producer_node,
                "output_name": self.output_name,
                "dataset_name": self.dataset_name,
                "partition_id": self.partition_id,
                "partition_index": self.partition_index,
                "metadata": dict(self.metadata),
                "provenance": dict(self.provenance),
            }
        )

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ArtifactReference:
        return cls(
            uri=str(data.get("uri") or data.get("path") or ""),
            product_kind=str(data.get("product_kind") or data.get("kind") or ""),
            format=data.get("format"),
            metadata=dict(data.get("metadata") or {}),
            provenance=dict(data.get("provenance") or {}),
            producer_node=data.get("producer_node") or data.get("node_id"),
            output_name=data.get("output_name"),
            dataset_name=data.get("dataset_name") or data.get("dataset"),
            partition_id=data.get("partition_id"),
            partition_index=data.get("partition_index") or data.get("partition"),
        )


class OutputResult(ArtifactReference):
    """Compatibility artifact reference using the historical kind/path names."""

    def __init__(
        self,
        *,
        kind: str,
        path: str,
        format: str | None = None,
        metadata: dict[str, Any] | None = None,
        provenance: dict[str, Any] | None = None,
        producer_node: str | None = None,
        output_name: str | None = None,
        dataset_name: str | None = None,
        partition_id: str | None = None,
        partition_index: int | None = None,
    ) -> None:
        super().__init__(
            uri=path,
            product_kind=kind,
            format=format,
            metadata=dict(metadata or {}),
            provenance=dict(provenance or {}),
            producer_node=producer_node,
            output_name=output_name,
            dataset_name=dataset_name,
            partition_id=partition_id,
            partition_index=partition_index,
        )


@dataclass(frozen=True, slots=True)
class ArtifactManifest:
    product_kind: str
    format: str | None
    producer_node: str
    output_name: str
    dataset_name: str | None
    parts: tuple[ArtifactReference, ...]
    metadata: dict[str, Any] = field(default_factory=dict)

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
                "format": self.format,
                "producer_node": self.producer_node,
                "output_name": self.output_name,
                "dataset_name": self.dataset_name,
                "metadata": dict(self.metadata),
                "parts": [part.to_dict() for part in self.parts],
            }
        )

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ArtifactManifest:
        return cls(
            product_kind=str(data.get("product_kind") or data.get("kind") or ""),
            format=data.get("format"),
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
        )


def _drop_none(data: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in data.items() if value is not None}
