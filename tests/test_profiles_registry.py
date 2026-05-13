from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from hepflow.api import normalise_author_file
from hepflow.compiler.profiles import load_profile_registry_layer
from hepflow.registry.loaders import load_runtime_spec_and_impl
from hepflow.registry.merge import RegistryLayer, merge_registry_layers


def test_registry_entries_merge_and_load_objects(toy_registry: dict[str, object]) -> None:
    result = merge_registry_layers(
        [
            RegistryLayer(name="a", kind="test", registry={"sources": {}}),
            RegistryLayer(name="toy", kind="test", registry=toy_registry),
        ]
    )

    assert "toy.source" in result.registry["sources"]
    spec, impl = load_runtime_spec_and_impl(
        result.registry,
        "sources",
        "toy.source",
    )
    assert spec["name"] == "toy.source"
    assert impl(ctx={})["pt"] == [12, 18, 21, 28]


def test_missing_registry_item_errors_clearly(toy_registry: dict[str, object]) -> None:
    with pytest.raises(KeyError, match="Unknown runtime registry entry 'toy.missing'"):
        load_runtime_spec_and_impl(toy_registry, "transforms", "toy.missing")


def test_qualified_package_profile_loads_from_test_package(tmp_path: Path) -> None:
    layer = load_profile_registry_layer(
        "tests.toy_components:registry",
        project_root=tmp_path,
    )

    assert layer.path == "package:tests.toy_components.profiles/registry.yaml"
    assert "toy.scale" in layer.registry["transforms"]


def test_profile_registry_is_copied_into_normalized_author(tmp_path: Path) -> None:
    author = {
        "version": "1.0",
        "use": {"profiles": ["tests.toy_components:registry"]},
        "sources": {"events": {"kind": "toy.source"}},
        "analysis": {"stages": []},
    }
    author_path = tmp_path / "author.yaml"
    author_path.write_text(yaml.safe_dump(author), encoding="utf-8")

    normalized = normalise_author_file(author_path, outdir=tmp_path / "build")

    assert normalized["use"]["profiles"] == ["tests.toy_components:registry"]
    assert "toy.write" in normalized["registry"]["sinks"]
    assert "registry_layers" in normalized["provenance"]
