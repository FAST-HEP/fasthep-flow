from __future__ import annotations

import pytest

from hepflow.compiler.plan_diff.identity import identity_of, identity_spec
from hepflow.compiler.plan_diff.model import DiffSection

G, D, R, P = (
    DiffSection.GRAPH,
    DiffSection.DATA,
    DiffSection.REPORTS,
    DiffSection.PROVENANCE,
)


@pytest.mark.parametrize(
    ("path", "fields", "section", "collapse"),
    [
        (("nodes",), ("id",), G, False),
        (("partitions",), ("source", "dataset", "file", "part"), D, False),
        (("nodes", "inputs"), ("node_id", "input_name"), G, False),
        (("reports",), ("id",), R, False),
        (("provenance", "inputs"), ("role", "declared"), P, True),
        (("provenance", "registry_layers"), ("kind", "name"), P, True),
        (("provenance", "execution_layers"), ("kind", "name"), P, True),
        (("provenance", "execution_hook_layers"), ("kind", "name"), P, True),
        (
            ("provenance", "registry_changes", "added"),
            ("section", "key", "layer"),
            P,
            True,
        ),
        (
            ("provenance", "registry_changes", "overwritten"),
            ("section", "key", "previous_layer", "new_layer"),
            P,
            True,
        ),
        (
            ("provenance", "optional_profiles_skipped"),
            ("profile", "included_by"),
            P,
            True,
        ),
    ],
)
def test_every_registered_list_has_the_documented_identity(
    path: tuple[str, ...], fields: tuple[str, ...], section: DiffSection, collapse: bool
) -> None:
    spec = identity_spec(path)
    assert spec is not None
    assert (spec.fields, spec.section, spec.collapse_exact_duplicates) == (
        fields,
        section,
        collapse,
    )


@pytest.mark.parametrize(
    "path",
    [
        ("execution_hooks",),
        ("data_flow", "consumers"),
        ("nodes", "params"),
        ("provenance", "registry_layers", "extra"),
        ("context", "dataset_names"),
        ("something", "id"),
    ],
)
def test_unregistered_lists_have_no_identity(path: tuple[str, ...]) -> None:
    assert identity_spec(path) is None


def test_identity_of_returns_ordered_field_value_pairs() -> None:
    spec = identity_spec(("partitions",))
    assert spec is not None
    item = {"part": 1, "file": "f", "dataset": "d", "source": "s", "id": "x"}
    assert identity_of(spec, item) == (
        ("source", "s"),
        ("dataset", "d"),
        ("file", "f"),
        ("part", 1),
    )
