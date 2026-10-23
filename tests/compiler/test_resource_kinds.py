from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from hepflow.api import normalise_workflow_file
from hepflow.compiler.normalize import normalize_registry
from hepflow.registry.defaults import (
    REGISTRY_SECTIONS,
    default_runtime_registry_config,
)
from hepflow.registry.merge import RegistryLayer, merge_registry_layers


def test_resource_kinds_is_a_registry_section() -> None:
    assert "resource_kinds" in REGISTRY_SECTIONS


def test_builtin_registry_has_file_kind() -> None:
    kinds = default_runtime_registry_config()["resource_kinds"]

    assert kinds["file"] == {"path_fields": ["path"]}


def test_profile_layer_adds_a_resource_kind() -> None:
    result = merge_registry_layers(
        [
            RegistryLayer(
                name="builtin",
                kind="builtin",
                registry=default_runtime_registry_config(),
            ),
            RegistryLayer(
                name="chip",
                kind="profile",
                registry={
                    "resource_kinds": {
                        "data_quality_mask": {"path_fields": ["paths"]},
                    }
                },
            ),
        ]
    )

    kinds = result.registry["resource_kinds"]
    assert kinds["data_quality_mask"] == {"path_fields": ["paths"]}
    assert kinds["file"] == {"path_fields": ["path"]}


def test_later_layer_replaces_a_resource_kind() -> None:
    result = merge_registry_layers(
        [
            RegistryLayer(
                name="a",
                kind="profile",
                registry={"resource_kinds": {"k": {"path_fields": ["path"]}}},
            ),
            RegistryLayer(
                name="b",
                kind="profile",
                registry={"resource_kinds": {"k": {"path_fields": ["paths"]}}},
            ),
        ]
    )

    assert result.registry["resource_kinds"]["k"] == {"path_fields": ["paths"]}


def test_normalize_registry_keeps_resource_kinds() -> None:
    registry = normalize_registry(
        {"resource_kinds": {"correctionlib": {"path_fields": ["path"]}}}
    )

    assert registry["resource_kinds"] == {"correctionlib": {"path_fields": ["path"]}}


def test_resource_kind_may_declare_no_path_fields() -> None:
    registry = normalize_registry({"resource_kinds": {"opaque": {"path_fields": []}}})

    assert registry["resource_kinds"]["opaque"] == {"path_fields": []}


@pytest.mark.parametrize(
    ("raw", "message"),
    [
        ({"k": {"path_fields": "path"}}, "path_fields must be a list of strings"),
        ({"k": {"path_fields": [1]}}, "path_fields must be a list of strings"),
        ({"k": {"path_fields": [""]}}, "path_fields must be a list of strings"),
        ({"k": {}}, "path_fields must be a list of strings"),
        ({"k": "path"}, "must be a mapping"),
        ({"": {"path_fields": ["path"]}}, "keys must be non-empty strings"),
    ],
)
def test_resource_kind_entries_are_validated(raw: dict[str, Any], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        normalize_registry({"resource_kinds": raw})


def test_workflow_registry_adds_a_resource_kind_to_the_merged_registry(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    workflow = {
        **toy_workflow,
        "registry": {
            **toy_workflow["registry"],
            "resource_kinds": {"lumimask": {"path_fields": ["paths"]}},
        },
    }
    path = tmp_path / "workflow.yaml"
    path.write_text(yaml.safe_dump(workflow, sort_keys=False), encoding="utf-8")

    normalized = normalise_workflow_file(path, outdir=tmp_path / "build", root=tmp_path)

    kinds = normalized["registry"]["resource_kinds"]
    assert kinds["lumimask"] == {"path_fields": ["paths"]}
    assert kinds["file"] == {"path_fields": ["path"]}
