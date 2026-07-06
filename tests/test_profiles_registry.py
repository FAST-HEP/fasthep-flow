from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import pytest
import yaml

from hepflow.api import normalise_author_file
from hepflow.compiler.profiles import expand_profile_names, load_profile_registry_layer
from hepflow.registry.loaders import (
    load_runtime_spec_and_impl,
)
from hepflow.registry.merge import RegistryLayer, merge_registry_layers
from hepflow.runtime.handlers import run_sink


def test_registry_entries_merge_and_load_objects(toy_registry: dict[str, Any]) -> None:
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


def test_run_sink_passes_runtime_registry_context(toy_registry: dict[str, Any]) -> None:
    result = cast(
        dict[str, Any],
        run_sink(
            sink_name="toy.capture_registry",
            target={"value": 1},
            params={},
            ctx={},
            registry_cfg=toy_registry,
        ),
    )

    assert result["plan_has_registry"] is True
    assert result["product_handlers"] == []


def test_missing_registry_item_errors_clearly(toy_registry: dict[str, Any]) -> None:
    with pytest.raises(KeyError, match=r"Unknown runtime registry entry 'toy.missing'"):
        load_runtime_spec_and_impl(toy_registry, "transforms", "toy.missing")


def test_qualified_package_profile_loads_from_test_package(tmp_path: Path) -> None:
    layer = load_profile_registry_layer(
        "tests.toy_components:registry",
        project_root=tmp_path,
    )

    assert layer.path == "package:tests.toy_components.profiles/registry.yaml"
    assert "toy.scale" in layer.registry["transforms"]


def test_unqualified_builtin_registry_profile_still_loads(tmp_path: Path) -> None:
    layer = load_profile_registry_layer("registry", project_root=tmp_path)

    assert layer.path == "package:hepflow.profiles/registry.yaml"
    assert "local.default" in layer.registry["backends"]


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


def test_hep_debug_profile_expands_to_hep_stack_with_diagnostics(
    tmp_path: Path,
) -> None:
    author = {
        "version": "1.0",
        "use": {"profiles": ["hep_debug"]},
        "data": {
            "defaults": {"tree_primary": "events"},
            "datasets": [
                {"name": "data", "files": ["data.root"]},
            ],
        },
        "sources": {"events": {"kind": "root_tree", "tree": "events"}},
        "analysis": {"stages": []},
    }
    author_path = tmp_path / "author.yaml"
    author_path.write_text(yaml.safe_dump(author), encoding="utf-8")

    normalized = normalise_author_file(author_path, outdir=tmp_path / "build")

    registry = normalized["registry"]
    assert "root_tree" in registry["sources"]
    assert "hep.schema_snapshot" in registry["observers"]
    assert "hep.render.hist1d" in registry["sinks"]
    hook_kinds = {
        hook["kind"] for hook in normalized["execution_hooks"]
    }
    assert "hep.dataset_context" in hook_kinds
    assert "hep.error_report" in hook_kinds
    assert "hep.warning_capture" in hook_kinds


def test_hep_profile_excludes_runtime_diagnostics(tmp_path: Path) -> None:
    author = {
        "version": "1.0",
        "use": {"profiles": ["hep"]},
        "data": {
            "defaults": {"tree_primary": "events"},
            "datasets": [
                {"name": "data", "files": ["data.root"]},
            ],
        },
        "sources": {"events": {"kind": "root_tree", "tree": "events"}},
        "analysis": {"stages": []},
    }
    author_path = tmp_path / "author.yaml"
    author_path.write_text(yaml.safe_dump(author), encoding="utf-8")

    normalized = normalise_author_file(author_path, outdir=tmp_path / "build")

    hook_kinds = {
        hook["kind"] for hook in normalized["execution_hooks"]
    }
    assert "hep.dataset_context" in hook_kinds
    assert "hep.error_report" not in hook_kinds
    assert "hep.warning_capture" not in hook_kinds


def test_profile_include_cycles_error(tmp_path: Path) -> None:
    profiles = tmp_path / ".hepflow" / "profiles"
    profiles.mkdir(parents=True)
    (profiles / "a.yaml").write_text("includes: [b]\n", encoding="utf-8")
    (profiles / "b.yaml").write_text("includes: [a]\n", encoding="utf-8")

    with pytest.raises(ValueError, match="profile include cycle detected: a -> b -> a"):
        expand_profile_names(["a"], project_root=tmp_path)
