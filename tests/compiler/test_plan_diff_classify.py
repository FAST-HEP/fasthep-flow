from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import pytest
import yaml

from hepflow.api import compile_workflow_file
from hepflow.compiler.plan_diff.classify import (
    CLASSIFIER_VERSION,
    classifier_table,
    classify,
)
from hepflow.compiler.plan_diff.model import DiffSection, Index, Key, Selector
from hepflow.utils import read_yaml

G, D, E, R, P, U = (
    DiffSection.GRAPH,
    DiffSection.DATA,
    DiffSection.EXECUTION,
    DiffSection.REPORTS,
    DiffSection.PROVENANCE,
    DiffSection.UNKNOWN,
)


def _loc(*keys: str) -> tuple[Key, ...]:
    return tuple(Key(k) for k in keys)


def _leaf_paths(value: Any, prefix: tuple[str, ...] = ()) -> list[tuple[str, ...]]:
    if isinstance(value, dict):
        if not value:
            return [prefix]
        return [p for k, v in value.items() for p in _leaf_paths(v, (*prefix, k))]
    if isinstance(value, list):
        if not value:
            return [prefix]
        return [p for v in value for p in _leaf_paths(v, prefix)]
    return [prefix]


def _mapping_project(tmp_path: Path) -> Path:
    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "x.json").write_text('{"x": 1}', encoding="utf-8")
    (tmp_path / "schemas").mkdir()
    (tmp_path / "schemas" / "map.yaml").write_text(
        yaml.safe_dump({"version": 1, "fields": {"pt": {}}}), encoding="utf-8"
    )
    workflow = {
        "version": "1.0",
        "use": {"profiles": ["tests.toy_components:registry"]},
        "sources": {"events": {"kind": "toy.source"}},
        "analysis": {
            "stages": [
                {
                    "id": "PathStage",
                    "op": "toy.path_param",
                    "params": {"path": "data/x.json"},
                },
                {
                    "id": "Config",
                    "op": "toy.mapping_config",
                    "params": {"config": "schemas/map.yaml"},
                },
            ]
        },
    }
    entry = tmp_path / "entry" / "workflow.yaml"
    entry.parent.mkdir()
    entry.write_text(yaml.safe_dump(workflow, sort_keys=False), encoding="utf-8")
    return entry


def _plans(tmp_path: Path, toy_workflow: dict[str, Any]) -> list[dict[str, Any]]:
    plans = []
    toy = tmp_path / "toy.yaml"
    toy.write_text(yaml.safe_dump(toy_workflow, sort_keys=False), encoding="utf-8")
    compile_workflow_file(toy, outdir=tmp_path / "b1")
    plans.append(read_yaml(tmp_path / "b1" / "compile" / "plan.yaml"))

    systematic = {
        **toy_workflow,
        "systematics": {
            "include_nominal": True,
            "variations": [
                {
                    "name": "up",
                    "group": "g",
                    "direction": "up",
                    "applies_to": "mc",
                    "requires": ["stage.TriggerEfficiencyWeights"],
                    "weight": {"multiply": "TriggerEffWeight_up"},
                }
            ],
        },
    }
    sys_path = tmp_path / "sys.yaml"
    sys_path.write_text(yaml.safe_dump(systematic, sort_keys=False), encoding="utf-8")
    compile_workflow_file(sys_path, outdir=tmp_path / "b2")
    for name in ("nominal", "up"):
        plans.append(read_yaml(tmp_path / "b2" / "compile" / name / "plan.yaml"))

    project = tmp_path / "proj"
    project.mkdir()
    entry = _mapping_project(project)
    compile_workflow_file(entry, outdir=tmp_path / "b3", root=project)
    plans.append(read_yaml(tmp_path / "b3" / "compile" / "plan.yaml"))
    return plans


def test_every_leaf_of_compiled_plans_is_classified_known(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    """Covers plain, systematics-variation and mapping-hook plans.

    The whole ``data_flow`` mapping is graph by prefix, so underscore-prefixed keys need no row.
    """
    unknown = {
        ".".join(path)
        for plan in _plans(tmp_path, toy_workflow)
        for path in _leaf_paths(plan)
        if classify(_loc(*path)) is U
    }
    assert unknown == set()


@pytest.mark.parametrize(
    ("path", "section"),
    [
        (("context", "datasets", "x", "files"), D),
        (("context", "dataset_names"), D),
        (("partitions", "id"), D),
        (("context", "globals", "a"), G),
        (("context", "variation", "name"), G),
        (("data_flow", "consumers"), G),
        (("context", "workflow_path"), P),
        (("context", "project_root"), P),
        (("execution", "backend"), E),
        (("execution_hooks",), E),
        (("reports", "id"), R),
        (("provenance", "inputs", "role"), P),
        (("provenance", "compilation", "flow_version"), P),
        (("provenance", "root"), P),
        (("nodes", "id"), G),
        (("nodes", "graph_node_id"), G),
        (("nodes", "role"), G),
        (("nodes", "impl"), G),
        (("nodes", "inputs", "node_id"), G),
        (("nodes", "params", "path"), G),
        (("nodes", "outputs", "stream"), G),
        (("nodes", "input_scope"), E),
        (("nodes", "output_scope"), E),
        (("nodes", "partitioning", "mode"), E),
        (("nodes", "materialize"), E),
        (("nodes", "meta", "execution", "x"), E),
        (("nodes", "meta", "compile_hooks", "p", "hook"), P),
        (("nodes", "meta", "stage_id"), G),
        (("nodes", "meta", "variation", "name"), G),
        (("nodes", "meta", "collection_for"), G),
        (("registry", "functions", "f"), G),
        (("registry", "render", "r"), G),
        (("registry", "backends", "b"), E),
        (("registry", "resource_kinds", "k"), E),
        (("registry", "report_templates", "t"), R),
        (("registry", "compile_hooks", "h"), P),
    ],
)
def test_table_rows_classify_to_documented_sections(
    path: tuple[str, ...], section: DiffSection
) -> None:
    assert classify(_loc(*path)) is section


@pytest.mark.parametrize(
    "path",
    [
        ("foo",),
        ("context", "new_field"),
        ("nodes", "meta", "surprise"),
        ("nodes", "meta"),
        ("nodes", "unexpected"),
        ("registry", "brand_new_section", "x"),
        ("registry",),
    ],
)
def test_unregistered_paths_are_unknown(path: tuple[str, ...]) -> None:
    assert classify(_loc(*path)) is U


def test_classification_ignores_key_names_like_path_source_hash() -> None:
    assert classify(_loc("context", "source")) is U
    assert classify(_loc("nodes", "params", "path")) is G
    assert classify(_loc("provenance", "inputs", "path")) is P
    assert classify(_loc("foo", "sha256")) is U


def test_indices_and_selectors_are_skipped() -> None:
    location = (Key("nodes"), Selector((("id", "n"),)), Key("params"), Index(0))
    assert classify(location) is G


def test_whole_object_section_overrides() -> None:
    assert classify(_loc("nodes"), whole_object_section=G) is G
    assert classify(_loc("nodes")) is U


def test_classifier_version_guard() -> None:
    digest = hashlib.sha256(repr(classifier_table()).encode()).hexdigest()
    assert (CLASSIFIER_VERSION, digest) == (
        1,
        "4415ea01960951db5ed6ed5fef8c4fd3d29bc38c3f61b7ae9b0a050992a73f1e",
    ), "table changed: bump CLASSIFIER_VERSION and update the pinned hash"


def test_docs_list_every_classifier_row() -> None:
    page = (
        Path(__file__).resolve().parents[2] / "docs" / "reference" / "plan-diff.md"
    ).read_text(encoding="utf-8")
    missing = [
        ".".join(prefix)
        for prefix, _ in classifier_table()
        if f"`{'.'.join(prefix)}`" not in page
    ]
    assert missing == []
    assert f"Classifier version: {CLASSIFIER_VERSION}" in page
