from __future__ import annotations

import json
from pathlib import Path

import yaml

from hepflow.api import compile_author_file, run_plan_file


def test_runtime_executes_toy_source_transform_and_final_sink(
    toy_author_path: Path,
    tmp_path: Path,
) -> None:
    build_dir = tmp_path / "build"
    compile_author_file(toy_author_path, outdir=build_dir)

    result = run_plan_file(build_dir / "plan.yaml", outdir=build_dir)

    assert result.success is True
    payload = json.loads((build_dir / "output.json").read_text(encoding="utf-8"))
    assert payload["pt"] == [12, 18, 21, 28]
    assert payload["scaled_pt"] == [24, 36, 42, 56]


def test_partition_dataset_and_run_end_sink_timing(
    tmp_path: Path,
    toy_author: dict[str, object],
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

    result = run_plan_file(build_dir / "plan.yaml", outdir=build_dir)

    assert result.success is True
    assert (build_dir / "partition" / "toydata" / "0_0.json").exists()
    assert (build_dir / "partition" / "toydata" / "0_1.json").exists()
    assert (build_dir / "dataset.json").exists()
    assert (build_dir / "run.json").exists()
