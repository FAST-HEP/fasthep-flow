"""Typed persistence records: how a product was stored and how to read it back.

The schema is ``docs/execution/persistence.md``. This module holds the leaf types
that ``ArtifactReference`` carries; it imports nothing from ``hepflow.model.io``.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
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


@dataclass(frozen=True, slots=True)
class SavedPayload:
    """One file a ``save`` callable wrote, named relative to the product folder."""

    path: str
    role: str | None = None


@dataclass(frozen=True, slots=True)
class SaveOutcome:
    """What a ``save`` callable reports. Flow stats and checksums the payloads."""

    payloads: tuple[SavedPayload, ...]
    variant: str | None = None
    locator: str | None = None


@dataclass(frozen=True, slots=True)
class PersistenceCapability:
    """A registered way to save and load one product kind.

    ``save(value, *, root, params, identity) -> SaveOutcome`` writes below ``root``;
    ``load(root, payloads, *, params, variant, locator) -> value`` reads back the
    payload files (absolute paths). A capability without ``load`` registers, and a
    restore of its products fails with ``missing_reader``.
    """

    name: str
    version: str
    kind: str
    container: str
    payload_roles: tuple[str, ...]
    fidelity: str
    save: Callable[..., SaveOutcome] | None = None
    load: Callable[..., Any] | None = None
    variants: tuple[str, ...] = ()
    params: Mapping[str, Any] = field(default_factory=dict)
    bindings: Mapping[str, Any] = field(default_factory=dict)


PERSISTENCE_MODES = ("off", "all", "selected")


@dataclass(frozen=True, slots=True)
class PersistenceSettings:
    """``execution.persistence``: the run-level persistence switch.

    ``off`` persists nothing (the default). ``all`` persists every boundary product whose
    kind has a capability. ``selected`` persists products chosen in ``products`` and kinds
    whose registry boundary representation is ``persist``. An entry of ``products`` (key
    ``<node id>:<output name>``) overrides the mode: ``false`` never, ``true`` with the
    kind's only capability, a string with that capability. ``reuse`` lets a restarted run
    take a partition's products from published sets of the same plan instead of running the
    producers again (restart, not a cache: no lookup across plans).
    """

    mode: str = "off"
    root: str | None = None
    products: Mapping[str, bool | str] = field(default_factory=dict)
    reuse: bool = False

    @classmethod
    def from_execution(cls, execution: Mapping[str, Any] | None) -> PersistenceSettings:
        raw = (execution or {}).get("persistence")
        return cls(**normalize_persistence(raw)) if raw else cls()


def normalize_persistence(raw: Any) -> dict[str, Any]:
    """Validate ``execution.persistence``; ``{}`` when nothing is persisted."""
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ValueError("execution.persistence must be a mapping")
    unknown = set(raw) - {"mode", "root", "products", "reuse"}
    if unknown:
        raise ValueError(
            f"execution.persistence has unknown keys {sorted(unknown)}; "
            "allowed: mode, root, products, reuse"
        )
    mode = raw.get("mode", "off")
    if mode not in PERSISTENCE_MODES:
        raise ValueError(
            f"execution.persistence.mode must be one of {PERSISTENCE_MODES}; got {mode!r}"
        )
    root = raw.get("root")
    if root is not None and (not isinstance(root, str) or not root.strip()):
        raise ValueError("execution.persistence.root must be a non-empty string")
    products = raw.get("products") or {}
    if not isinstance(products, dict):
        raise ValueError("execution.persistence.products must be a mapping")
    for key, choice in products.items():
        if ":" not in str(key):
            raise ValueError(
                f"execution.persistence.products key {key!r} must be "
                "'<node id>:<output name>'"
            )
        if not isinstance(choice, bool | str):
            raise ValueError(
                f"execution.persistence.products[{key!r}] must be true, false or a "
                "capability name"
            )
    reuse = raw.get("reuse", False)
    if not isinstance(reuse, bool):
        raise ValueError("execution.persistence.reuse must be a boolean")
    if mode == "off":
        return {}
    result: dict[str, Any] = {"mode": mode}
    if reuse:
        result["reuse"] = True
    if root is not None:
        result["root"] = root.strip()
    if products:
        result["products"] = dict(products)
    return result
