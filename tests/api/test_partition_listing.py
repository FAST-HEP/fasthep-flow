from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from hepflow.api import list_workflow_partitions


def _write_workflow(path: Path) -> Path:
    workflow = {
        "version": "1.0",
        "execution": {"partitioning": {"chunk_size": 6}},
        "data": {
            "datasets": [
                {
                    "name": "counted",
                    "files": ["a.root", "b.root"],
                    "file_nevents": [10, 7],
                    "chunk_size": 5,
                },
                {"name": "single", "files": ["c.root"], "nevents": "12"},
                {"name": "uncounted", "files": ["d.root"]},
            ]
        },
        "sources": {
            "events": {
                "kind": "root_tree",
                "tree": "events",
                "stream_type": "event_stream",
                "branches": ["pt"],
            }
        },
    }
    path.write_text(yaml.safe_dump(workflow, sort_keys=False))
    return path


def test_listing_uses_dataset_and_workflow_chunk_sizes_without_writing(
    tmp_path: Path,
) -> None:
    workflow = _write_workflow(tmp_path / "workflow.yaml")
    before = sorted(path.relative_to(tmp_path) for path in tmp_path.rglob("*"))

    with pytest.warns(UserWarning, match="whole-file partition"):
        result = list_workflow_partitions(workflow)

    assert result == [
        {
            "dataset": "counted",
            "chunk_size": 5,
            "n_partitions": 4,
            "n_files": 2,
            "n_uncounted": 0,
        },
        {
            "dataset": "single",
            "chunk_size": 6,
            "n_partitions": 2,
            "n_files": 1,
            "n_uncounted": 0,
        },
        {
            "dataset": "uncounted",
            "chunk_size": 6,
            "n_partitions": 1,
            "n_files": 1,
            "n_uncounted": 1,
        },
    ]
    assert sorted(path.relative_to(tmp_path) for path in tmp_path.rglob("*")) == before


def test_listing_cli_chunk_size_overrides_and_dataset_filter(tmp_path: Path) -> None:
    workflow = _write_workflow(tmp_path / "workflow.yaml")

    result = list_workflow_partitions(workflow, dataset="single", chunk_size=4)

    assert result == [
        {
            "dataset": "single",
            "chunk_size": 4,
            "n_partitions": 3,
            "n_files": 1,
            "n_uncounted": 0,
        }
    ]


def test_listing_unknown_dataset_names_available_choices(tmp_path: Path) -> None:
    workflow = _write_workflow(tmp_path / "workflow.yaml")

    with pytest.raises(ValueError, match=r"missing.*counted, single, uncounted"):
        list_workflow_partitions(workflow, dataset="missing")
