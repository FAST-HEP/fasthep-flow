from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

import pytest
import yaml

from hepflow.api import compile_workflow_file
from hepflow.compiler.plan_diff.errors import InvalidPlan
from hepflow.compiler.plan_diff.validate import validate_plan
from hepflow.utils import read_yaml


def _plan() -> dict[str, Any]:
    return {
        "context": {"project_root": "/p"},
        "nodes": [
            {"id": "a", "inputs": [{"node_id": "x", "input_name": "stream"}]},
            {"id": "b", "inputs": []},
        ],
        "partitions": [{"source": "s", "dataset": "d", "file": "f", "part": 0}],
        "reports": [{"id": "r"}],
        "provenance": {"inputs": [{"role": "workflow", "declared": "w.yaml"}]},
    }


def _invalid(plan: Any, path: str | None = None) -> InvalidPlan:
    with pytest.raises(InvalidPlan) as info:
        validate_plan(plan, side="old")
    if path is not None:
        assert info.value.path == path
    return info.value


def test_valid_compiled_plan_passes(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    workflow = tmp_path / "wf.yaml"
    workflow.write_text(yaml.safe_dump(toy_workflow, sort_keys=False), encoding="utf-8")
    compile_workflow_file(workflow, outdir=tmp_path / "build")

    validate_plan(read_yaml(tmp_path / "build" / "compile" / "plan.yaml"), side="old")


def test_minimal_plan_passes() -> None:
    validate_plan(_plan(), side="new")


def test_list_root_rejected() -> None:
    _invalid([1, 2])


def test_non_string_key_rejected() -> None:
    plan = _plan()
    plan["context"][1] = "x"
    _invalid(plan, "context")


def test_yaml_date_rejected() -> None:
    plan = _plan()
    plan["context"].update(yaml.safe_load("d: 2020-01-01"))
    _invalid(plan, "context.d")


def test_yaml_set_rejected() -> None:
    plan = _plan()
    plan["context"].update(yaml.safe_load("s: !!set {a, b}"))
    _invalid(plan, "context.s")


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_non_finite_floats_rejected(value: float) -> None:
    plan = _plan()
    plan["nodes"][0]["x"] = value
    _invalid(plan, "nodes[0].x")


def test_wrong_container_type_at_known_path_rejected() -> None:
    plan = _plan()
    plan["nodes"] = {"a": 1}
    _invalid(plan, "nodes")


def test_missing_node_id_rejected() -> None:
    plan = _plan()
    del plan["nodes"][1]["id"]
    _invalid(plan, "nodes[1].id")


def test_node_id_must_be_a_string() -> None:
    plan = _plan()
    plan["nodes"][0]["id"] = 3
    _invalid(plan, "nodes[0].id")


def test_missing_provenance_identity_field_rejected() -> None:
    plan = _plan()
    del plan["provenance"]["inputs"][0]["declared"]
    _invalid(plan, "provenance.inputs[0].declared")


def test_duplicate_node_ids_rejected() -> None:
    plan = _plan()
    plan["nodes"].append({"id": "a"})
    _invalid(plan, "nodes[2]")


def test_duplicate_partition_identity_rejected() -> None:
    plan = _plan()
    plan["partitions"].append(dict(plan["partitions"][0], id="other"))
    _invalid(plan, "partitions[1]")


def test_duplicate_report_ids_rejected() -> None:
    plan = _plan()
    plan["reports"].append({"id": "r"})
    _invalid(plan, "reports[1]")


def test_unknown_field_with_valid_types_is_accepted() -> None:
    plan = _plan()
    plan["brand_new"] = {"k": [1, 2.5, "x", None, True]}
    validate_plan(plan, side="old")


def test_validation_does_not_mutate_the_plan() -> None:
    plan = _plan()
    before = copy.deepcopy(plan)
    validate_plan(plan, side="old")
    assert plan == before


def test_message_names_the_side() -> None:
    assert "old plan" in _invalid([1]).message
