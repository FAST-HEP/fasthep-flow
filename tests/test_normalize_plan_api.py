from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

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


def test_normalize_preserves_generic_toy_source(toy_author: dict[str, Any]) -> None:
    normalized = normalize_author(toy_author)

    assert normalized["sources"]["events"]["kind"] == "toy.source"
    assert normalized["sources"]["events"]["stream_type"] == "event_stream"


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


def test_lowering_and_plan_creation_write_graph_artifacts(
    tmp_path: Path,
    toy_author_path: Path,
) -> None:
    build_dir = tmp_path / "build"
    normalise_author_file(toy_author_path, outdir=build_dir)
    plan = make_plan_file(build_dir / "normalized.yaml", outdir=build_dir)

    assert [node.id for node in plan.nodes] == [
        "read.events",
        "stage.Scale",
        "write.Scale.0",
    ]
    assert plan.get_node("write.Scale.0").params["when"] == "run_end"
    assert (build_dir / "plan.yaml").exists()
    assert (build_dir / "graph.mmd").exists()
    assert (build_dir / "graph.dot").exists()


def test_lower_graph_normalizes_sink_when_alias(toy_author: dict[str, Any]) -> None:
    toy_author = dict(toy_author)
    graph = lower_author_to_graph(normalize_author(toy_author))

    sink = graph.nodes["write.Scale.0"]["payload"]
    assert sink.params["when"] == "run_end"


def test_public_api_compile_and_run_roundtrip(
    toy_author_path: Path, tmp_path: Path
) -> None:
    build_dir = tmp_path / "api-build"

    plan = compile_author_file(toy_author_path, outdir=build_dir)
    assert plan.get_node("stage.Scale").impl == "toy.scale"

    result = run_plan_file(build_dir / "plan.yaml", outdir=build_dir)
    assert result.success is True
    assert (build_dir / "run_summary.yaml").exists()
    assert (build_dir / "output.json").exists()

    one_shot_dir = tmp_path / "one-shot"
    result = run_author_file(toy_author_path, outdir=one_shot_dir)
    assert result.success is True
    assert (one_shot_dir / "normalized.yaml").exists()
    assert (one_shot_dir / "plan.yaml").exists()
    assert (one_shot_dir / "run_summary.yaml").exists()


def test_public_api_accepts_str_and_path_inputs_and_serializes_paths_as_strings(
    toy_author_path: Path,
    tmp_path: Path,
) -> None:
    build_dir = tmp_path / "mixed-paths"

    normalized = normalise_author_file(str(toy_author_path), outdir=build_dir)
    assert_no_path_objects(normalized)

    plan = make_plan_file(str(build_dir / "normalized.yaml"), outdir=str(build_dir))
    assert_no_path_objects(plan.to_dict())

    result = run_plan_file(build_dir / "plan.yaml", outdir=str(build_dir))
    assert result.success is True

    for filename in ["normalized.yaml", "plan.yaml", "run_summary.yaml"]:
        payload = read_yaml(build_dir / filename)
        assert_no_path_objects(payload)

    plan_yaml = read_yaml(build_dir / "plan.yaml")
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
