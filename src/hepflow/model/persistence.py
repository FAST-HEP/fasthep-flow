"""Typed persistence records: how a product was stored and how to read it back.

The schema is ``docs/execution/persistence.md``. This module holds the leaf types
that ``ArtifactReference`` carries; it imports nothing from ``hepflow.model.io``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class StorageCall:
    """The save or load implementation recorded with a storage binding."""

    implementation: str
    version: str
    params: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "implementation": self.implementation,
            "version": self.version,
            "params": dict(self.params),
        }

    @classmethod
    def from_dict(
        cls, data: dict[str, Any], *, field_name: str = "call"
    ) -> StorageCall:
        for key in ("implementation", "version"):
            if not isinstance(data.get(key), str) or not data[key].strip():
                raise ValueError(f"StorageBinding.{field_name}.{key} is required")
        return cls(
            implementation=str(data["implementation"]),
            version=str(data["version"]),
            params=dict(data.get("params") or {}),
        )


@dataclass(frozen=True, slots=True)
class StorageBinding:
    """What was written for a product and how it is read back (published binding)."""

    capability: str
    capability_version: str
    container: str
    save: StorageCall
    load: StorageCall
    variant: str | None = None
    locator: str | None = None

    def __post_init__(self) -> None:
        for name in ("capability", "capability_version", "container"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"StorageBinding.{name} is required")

    def to_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {
            "capability": self.capability,
            "capability_version": self.capability_version,
            "container": self.container,
            "save": self.save.to_dict(),
            "load": self.load.to_dict(),
        }
        if self.variant is not None:
            data["variant"] = self.variant
        if self.locator is not None:
            data["locator"] = self.locator
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> StorageBinding:
        return cls(
            capability=str(data.get("capability") or ""),
            capability_version=str(data.get("capability_version") or ""),
            container=str(data.get("container") or ""),
            save=StorageCall.from_dict(dict(data.get("save") or {}), field_name="save"),
            load=StorageCall.from_dict(dict(data.get("load") or {}), field_name="load"),
            variant=data.get("variant"),
            locator=data.get("locator"),
        )
