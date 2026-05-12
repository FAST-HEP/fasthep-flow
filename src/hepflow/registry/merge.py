from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Any

from hepflow.registry.defaults import REGISTRY_SECTIONS


@dataclass(slots=True)
class RegistryLayer:
    name: str
    kind: str
    registry: dict[str, Any]
    path: str | None = None


@dataclass(slots=True)
class RegistryMergeResult:
    registry: dict[str, Any]
    provenance: dict[str, Any]


def merge_registry_layers(
    layers: list[RegistryLayer],
    *,
    sections: tuple[str, ...] = REGISTRY_SECTIONS,
) -> RegistryMergeResult:
    """Merge registry layers and report key-level additions/overwrites.

    Merge order is controlled by caller and should be:
    builtin defaults < selected profile configs < author.yaml < CLI overrides.
    """
    merged: dict[str, Any] = {section: {} for section in sections}
    owners: dict[tuple[str, str], str] = {}
    added: list[dict[str, str]] = []
    overwritten: list[dict[str, str]] = []

    for layer in layers:
        for section in sections:
            section_cfg = dict((layer.registry or {}).get(section) or {})
            for key, value in section_cfg.items():
                owner_key = (section, str(key))
                previous_layer = owners.get(owner_key)
                if previous_layer is None:
                    added.append(
                        {
                            "section": section,
                            "key": str(key),
                            "layer": layer.name,
                        }
                    )
                elif merged[section].get(key) != value:
                    overwritten.append(
                        {
                            "section": section,
                            "key": str(key),
                            "previous_layer": previous_layer,
                            "new_layer": layer.name,
                        }
                    )
                merged[section][key] = deepcopy(value)
                owners[owner_key] = layer.name

    provenance_layers = []
    for layer in layers:
        item = {
            "name": layer.name,
            "kind": layer.kind,
        }
        if layer.path is not None:
            item["path"] = layer.path
        provenance_layers.append(item)

    return RegistryMergeResult(
        registry=merged,
        provenance={
            "registry_layers": provenance_layers,
            "registry_changes": {
                "added": added,
                "overwritten": overwritten,
            },
        },
    )
