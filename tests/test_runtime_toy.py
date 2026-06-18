from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml

from hepflow.api import compile_author_file, run_plan_file
from hepflow.model.plan import ExecutionNode, ExecutionPlan, PlanInputRef
from hepflow.runtime.engine import _source_should_read_metadata_only


def test_runtime_executes_toy_source_transform_and_final_sink(
    toy_author_path: Path,
    tmp_path: Path,
) -> None:
    build_dir = tmp_path / "build"
    compile_author_file(toy_author_path, outdir=build_dir)

    result = run_plan_file(build_dir / "compile" / "plan.yaml", outdir=build_dir)

    assert result.success is True
    payload = json.loads(
        (build_dir / "artifacts" / "files" / "output.json").read_text(
            encoding="utf-8"
        )
    )
    assert payload["pt"] == [12, 18, 21, 28]
    assert payload["scaled_pt"] == [24, 36, 42, 56]


def test_partition_dataset_and_run_end_sink_timing(
    tmp_path: Path,
    toy_author: dict[str, Any],
) -> None:
    author = {
        **toy_author,
        "data": {
            "datasets": [
                {
                    "name": "toydata",
                    "files": ["toy://events"],
                    "nevents": 4,
                }
            ]
        },
    }
    stage = author["analysis"]["stages"][0]
    stage["write"] = [
        {"kind": "toy.write", "path": "partition.json", "when": "partition"},
        {"kind": "toy.write", "path": "dataset.json", "when": "dataset"},
        {"kind": "toy.write", "path": "run.json", "when": "final"},
    ]

    author_path = tmp_path / "author.yaml"

    author_path.write_text(yaml.safe_dump(author, sort_keys=False), encoding="utf-8")
    build_dir = tmp_path / "build"
    compile_author_file(author_path, outdir=build_dir, chunk_size=2)

    result = run_plan_file(build_dir / "compile" / "plan.yaml", outdir=build_dir)

    assert result.success is True
    files_dir = build_dir / "artifacts" / "files"
    assert (files_dir / "partition" / "toydata" / "0_0.json").exists()
    assert (files_dir / "partition" / "toydata" / "0_1.json").exists()
    assert (files_dir / "dataset.json").exists()
    assert (files_dir / "run.json").exists()


def test_root_tree_source_metadata_only_when_only_schema_observers() -> None:
    plan = ExecutionPlan()
    source = ExecutionNode(
        id="read.events",
        graph_node_id="read.events",
        role="source",
        impl="root_tree",
        outputs={"stream": "event_stream"},
    )
    observer = ExecutionNode(
        id="observe.schema.read_events",
        graph_node_id="observe.schema.read_events",
        role="observer",
        impl="hep.schema_snapshot",
        inputs=[
            PlanInputRef(
                node_id="read.events",
                output_name="stream",
                input_name="target",
            )
        ],
        outputs={"report": "report"},
    )
    plan.add_node(source)
    plan.add_node(observer)

    assert _source_should_read_metadata_only(plan, source) is True


def test_root_tree_source_not_metadata_only_when_transform_consumes_stream() -> None:
    plan = ExecutionPlan()
    source = ExecutionNode(
        id="read.events",
        graph_node_id="read.events",
        role="source",
        impl="root_tree",
        outputs={"stream": "event_stream"},
    )
    transform = ExecutionNode(
        id="stage.Derived",
        graph_node_id="stage.Derived",
        role="transform",
        impl="hep.define",
        inputs=[
            PlanInputRef(
                node_id="read.events",
                output_name="stream",
                input_name="stream",
            )
        ],
        outputs={"events": "event_stream"},
    )
    plan.add_node(source)
    plan.add_node(transform)

    assert _source_should_read_metadata_only(plan, source) is False
