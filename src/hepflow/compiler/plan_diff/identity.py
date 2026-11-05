"""Registered list identities: the only way list items are matched by identity.

There is deliberately no generic "object with ``id`` or ``name``" matcher.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from hepflow.compiler.plan_diff.model import DiffSection


@dataclass(frozen=True)
class MoveSpec:
    """How unmatched rows of an identified list are paired into a move."""

    pair_key: tuple[str, ...]
    hash_field: str
    #: Fields that may legitimately differ in a move.
    location_fields: tuple[str, ...]
    #: Fields that must be equal for rows to count as the same thing at a new place.
    metadata_fields: tuple[str, ...]


@dataclass(frozen=True)
class IdentitySpec:
    fields: tuple[str, ...]
    #: Section of a whole added or removed object of this list.
    section: DiffSection
    #: Whether rows that are equal in every field collapse to one before matching.
    collapse_exact_duplicates: bool = False
    move: MoveSpec | None = None


_G, _D, _R, _P = (
    DiffSection.GRAPH,
    DiffSection.DATA,
    DiffSection.REPORTS,
    DiffSection.PROVENANCE,
)

_LAYER = IdentitySpec(("kind", "name"), _P, True)

_SPECS: dict[tuple[str, ...], IdentitySpec] = {
    ("nodes",): IdentitySpec(("id",), _G),
    ("partitions",): IdentitySpec(("source", "dataset", "file", "part"), _D),
    ("nodes", "inputs"): IdentitySpec(("node_id", "input_name"), _G),
    ("reports",): IdentitySpec(("id",), _R),
    ("provenance", "inputs"): IdentitySpec(
        ("role", "declared"),
        _P,
        True,
        MoveSpec(
            pair_key=("role", "sha256"),
            hash_field="sha256",
            location_fields=("declared", "resolved", "origin"),
            metadata_fields=("size", "note"),
        ),
    ),
    ("provenance", "registry_layers"): _LAYER,
    ("provenance", "execution_layers"): _LAYER,
    ("provenance", "execution_hook_layers"): _LAYER,
    ("provenance", "registry_changes", "added"): IdentitySpec(
        ("section", "key", "layer"), _P, True
    ),
    ("provenance", "registry_changes", "overwritten"): IdentitySpec(
        ("section", "key", "previous_layer", "new_layer"), _P, True
    ),
    ("provenance", "optional_profiles_skipped"): IdentitySpec(
        ("profile", "included_by"), _P, True
    ),
}

#: Lists whose duplicate identities mean the plan itself is broken.
STRICT_UNIQUE_PATHS: frozenset[tuple[str, ...]] = frozenset(
    {("nodes",), ("partitions",), ("reports",)}
)


def identity_spec(key_path: tuple[str, ...]) -> IdentitySpec | None:
    """Return the identity of the list at ``key_path`` (indices and selectors removed)."""
    return _SPECS.get(key_path)


def registered_paths() -> tuple[tuple[str, ...], ...]:
    return tuple(_SPECS)


def identity_of(
    spec: IdentitySpec, item: dict[str, Any]
) -> tuple[tuple[str, Any], ...]:
    return tuple((name, item[name]) for name in spec.fields)
