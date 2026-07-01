from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

import hepflow.api as api
from hepflow.api import (
    compile_author_file,
    make_plan_file,
    normalise_author_file,
    run_author_file,
    run_plan_file,
)
from hepflow.compiler.includes import load_author_with_includes
from hepflow.compiler.lower_graph import lower_author_to_graph
from hepflow.compiler.normalize import normalize_author
from hepflow.utils import read_yaml


def test_public_api_exports_stable_facade_symbols() -> None:
    assert api.__all__ == [
        "InitResult",
        "compile_author_file",
        "diff_plan_files",
        "init_project",
        "load_author_yaml",
        "load_plan_file",
        "make_plan_file",
        "normalise_author_file",
        "normalize_author_file",
        "provenance_artifact_text",
        "provenance_graph_text",
        "provenance_summary_text",
        "run_author_file",
        "run_plan_file",
    ]
    for name in api.__all__:
        assert hasattr(api, name)


def test_normalize_preserves_generic_toy_source(toy_author: dict[str, Any]) -> None:
    normalized = normalize_author(toy_author)

    assert normalized["sources"]["events"]["kind"] == "toy.source"
    assert normalized["sources"]["events"]["stream_type"] == "event_stream"


def test_top_level_sinks_errors_with_supported_syntax(
    toy_author: dict[str, Any],
) -> None:
    author = dict(toy_author)
    author["sinks"] = [
        {
            "kind": "toy.write",
            "from": "stage.Scale",
            "path": "output.json",
        }
    ]

    with pytest.raises(
        ValueError,
        match=r"Top-level 'sinks' is not supported.*analysis\.stages\[\]\.write",
    ):
        normalize_author(author)


def test_include_handling_then_normalization(
    tmp_path: Path, toy_registry: dict[str, Any]
) -> None:
    include_path = tmp_path / "registry.yaml"
    include_path.write_text(
        yaml.safe_dump({"registry": toy_registry}, sort_keys=False),
        encoding="utf-8",
    )
    author_path = tmp_path / "author.yaml"
    author_path.write_text(
        yaml.safe_dump(
            {
                "include": ["registry.yaml"],
                "sources": {"events": {"kind": "toy.source"}},
                "analysis": {"stages": []},
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    loaded = load_author_with_includes(author_path)
    normalized = normalize_author(loaded.doc)

    assert "toy.source" in normalized["registry"]["sources"]


def test_include_dataset_mapping_then_normalization(tmp_path: Path) -> None:
    include_path = tmp_path / "datasets.yaml"
    include_path.write_text(
        yaml.safe_dump(
            {
                "datasets": {
                    "DoubleMuon": {
                        "eventtype": "data",
                        "files": ["root://example.test/events.root"],
                    }
                }
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    author_path = tmp_path / "author.yaml"
    author_path.write_text(
        yaml.safe_dump(
            {
                "include": ["datasets.yaml"],
                "data": {"defaults": {"eventtype": "mc", "tree_primary": "Events"}},
                "sources": {"events": {"kind": "root_tree", "tree": "Events"}},
                "analysis": {"stages": []},
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    loaded = load_author_with_includes(author_path)
    normalized = normalize_author(loaded.doc)

    assert normalized["data"]["datasets"] == [
        {
            "name": "DoubleMuon",
            "files": ["root://example.test/events.root"],
            "nevents": None,
            "eventtype": "data",
            "group": "DoubleMuon",
            "meta": {},
        }
    ]


def test_root_tree_source_preserves_reader_options(toy_author: dict[str, Any]) -> None:
    author = {
        **toy_author,
        "sources": {
            "events": {
                "kind": "root_tree",
                "tree": "Events",
                "branches": ["Muon_pt"],
                "start": 10,
                "stop": 20,
            }
        },
    }

    normalized = normalize_author(author)

    assert normalized["sources"]["events"] == {
        "tree": "Events",
        "stream_type": "event_stream",
        "kind": "root_tree",
        "branches": ["Muon_pt"],
        "start": 10,
        "stop": 20,
    }


def test_lowering_and_plan_creation_write_graph_artifacts(
    tmp_path: Path,
    toy_author_path: Path,
) -> None:
    build_dir = tmp_path / "build"
    normalise_author_file(toy_author_path, outdir=build_dir)
    plan = make_plan_file(build_dir / "compile" / "normalized.yaml", outdir=build_dir)

    assert [node.id for node in plan.nodes] == [
        "read.events",
        "stage.Scale",
        "write.Scale.0",
    ]
    assert plan.get_node("write.Scale.0").params["when"] == "run_end"
    assert (build_dir / "compile" / "plan.yaml").exists()
    assert (build_dir / "graph" / "graph.mmd").exists()
    assert (build_dir / "graph" / "graph.dot").exists()
    assert (build_dir / "graph" / "graph.json").exists()
    assert not (build_dir / "plan.yaml").exists()
    assert not (build_dir / "normalized.yaml").exists()
    assert not (build_dir / "graph.mmd").exists()


def test_lower_graph_normalizes_sink_when_alias(toy_author: dict[str, Any]) -> None:
    toy_author = dict(toy_author)
    graph = lower_author_to_graph(normalize_author(toy_author))

    sink = graph.nodes["write.Scale.0"]["payload"]
    assert sink.params["when"] == "run_end"


def test_output_layout_is_normalized_and_resolved_for_writer(
    toy_author: dict[str, Any],
) -> None:
    author = dict(toy_author)
    author["outputs"] = {
        "small": {
            "tree": "events",
            "keep": ["Muon_Pt"],
        }
    }
    author["analysis"] = {
        "stages": [
            {
                "id": "Scale",
                "op": "toy.scale",
                "params": {"factor": 2},
                "write": {
                    "kind": "toy.write",
                    "path": "small.root",
                    "use": "small",
                },
            }
        ]
    }

    normalized = normalize_author(author)
    graph = lower_author_to_graph(normalized)

    assert normalized["outputs"]["small"] == {
        "tree": "events",
        "keep": ["Muon_Pt"],
    }
    sink = graph.nodes["write.Scale.0"]["payload"]
    assert sink.params == {
        "path": "small.root",
        "tree": "events",
        "keep": ["Muon_Pt"],
        "when": "partition_end",
    }
    assert sink.meta["output_layout"] == "small"


def test_writer_use_rejects_unknown_output_layout(toy_author: dict[str, Any]) -> None:
    author = dict(toy_author)
    author["analysis"] = {
        "stages": [
            {
                "id": "Scale",
                "op": "toy.scale",
                "params": {"factor": 2},
                "write": {
                    "kind": "toy.write",
                    "path": "small.root",
                    "use": "missing",
                },
            }
        ]
    }

    with pytest.raises(ValueError, match="unknown output layout 'missing'"):
        lower_author_to_graph(normalize_author(author))


def test_public_api_compile_and_run_roundtrip(
    toy_author_path: Path, tmp_path: Path
) -> None:
    build_dir = tmp_path / "api-build"

    plan = compile_author_file(toy_author_path, outdir=build_dir)
    assert plan.get_node("stage.Scale").impl == "toy.scale"

    result = run_plan_file(build_dir / "compile" / "plan.yaml", outdir=build_dir)
    assert result.success is True
    assert (build_dir / "run_summary.yaml").exists()
    assert (build_dir / "artifacts" / "files" / "output.json").exists()

    one_shot_dir = tmp_path / "one-shot"
    result = run_author_file(toy_author_path, outdir=one_shot_dir)
    assert result.success is True
    assert (one_shot_dir / "compile" / "normalized.yaml").exists()
    assert (one_shot_dir / "compile" / "plan.yaml").exists()
    assert (one_shot_dir / "run_summary.yaml").exists()


def test_public_api_accepts_str_and_path_inputs_and_serializes_paths_as_strings(
    toy_author_path: Path,
    tmp_path: Path,
) -> None:
    build_dir = tmp_path / "mixed-paths"

    normalized = normalise_author_file(str(toy_author_path), outdir=build_dir)
    assert_no_path_objects(normalized)

    plan = make_plan_file(
        str(build_dir / "compile" / "normalized.yaml"),
        outdir=str(build_dir),
    )
    assert_no_path_objects(plan.to_dict())

    result = run_plan_file(build_dir / "compile" / "plan.yaml", outdir=str(build_dir))
    assert result.success is True

    for path in [
        build_dir / "compile" / "normalized.yaml",
        build_dir / "compile" / "plan.yaml",
        build_dir / "run_summary.yaml",
    ]:
        payload = read_yaml(path)
        assert_no_path_objects(payload)

    plan_yaml = read_yaml(build_dir / "compile" / "plan.yaml")
    sink_node = next(node for node in plan_yaml["nodes"] if node["role"] == "sink")
    assert isinstance(sink_node["params"]["path"], str)


def assert_no_path_objects(value: object) -> None:
    if isinstance(value, Path):
        raise AssertionError(f"Found Path object in serialized payload: {value!r}")
    if isinstance(value, dict):
        for item in value.values():
            assert_no_path_objects(item)
        return
    if isinstance(value, list | tuple):
        for item in value:
            assert_no_path_objects(item)
