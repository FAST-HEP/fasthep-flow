from __future__ import annotations

from typing import Any

import pytest

from hepflow.compiler.normalize import normalize_workflow
from hepflow.compiler.plan import build_plan_from_normalized

GLOBAL = {"match": "/store/", "prefix": "root://xrd.example/"}


def _workflow(
    registry: dict[str, Any], files: list[str], file_access: Any = None
) -> dict[str, Any]:
    workflow: dict[str, Any] = {
        "version": "1.0",
        "registry": registry,
        "data": {"datasets": [{"name": "sample", "files": files}]},
        "sources": {
            "events": {
                "kind": "toy.source",
                "stream_type": "event_stream",
                "branches": ["pt"],
            }
        },
    }
    if file_access is not None:
        workflow["execution"] = {"file_access": file_access}
    return workflow


def _plan(workflow: dict[str, Any]) -> Any:
    _graph, plan = build_plan_from_normalized(normalize_workflow(workflow))
    return plan


def test_partition_file_gets_the_prefix_of_the_matching_rule(
    toy_registry: dict[str, Any],
) -> None:
    plan = _plan(_workflow(toy_registry, ["/store/mc/a.root"], [GLOBAL]))

    assert [p.file for p in plan.partitions] == ["root://xrd.example//store/mc/a.root"]


def test_plan_datasets_list_the_urls_and_keep_the_logical_names(
    toy_registry: dict[str, Any],
) -> None:
    # Everything that opens files (sources, compile hooks) reads the URLs.
    plan = _plan(_workflow(toy_registry, ["/store/mc/a.root"], [GLOBAL]))

    dataset = plan.context["datasets"]["sample"]
    assert dataset["files"] == ["root://xrd.example//store/mc/a.root"]
    assert dataset["logical_files"] == ["/store/mc/a.root"]


def test_without_a_matching_rule_the_plan_has_no_logical_names(
    toy_registry: dict[str, Any],
) -> None:
    plan = _plan(_workflow(toy_registry, ["local/a.root"], [GLOBAL]))

    assert "logical_files" not in plan.context["datasets"]["sample"]


def test_the_first_matching_rule_wins(toy_registry: dict[str, Any]) -> None:
    rules = [
        {"match": "/store/data/", "prefix": "root://local.example//eos/cms"},
        GLOBAL,
    ]
    plan = _plan(
        _workflow(toy_registry, ["/store/data/d.root", "/store/mc/m.root"], rules)
    )

    assert [p.file for p in plan.partitions] == [
        "root://local.example//eos/cms/store/data/d.root",
        "root://xrd.example//store/mc/m.root",
    ]


def test_files_without_a_matching_rule_are_unchanged(
    toy_registry: dict[str, Any],
) -> None:
    files = ["local/a.root", "/data/b.root", "root://other.example//store/c.root"]
    plan = _plan(_workflow(toy_registry, files, [GLOBAL]))

    assert [p.file for p in plan.partitions] == files


def test_the_plan_records_the_rules(toy_registry: dict[str, Any]) -> None:
    plan = _plan(_workflow(toy_registry, ["/store/mc/a.root"], [GLOBAL]))

    assert plan.execution["file_access"] == [GLOBAL]


@pytest.mark.parametrize(
    "file_access",
    [
        {"match": "/store/", "prefix": "root://x/"},
        [{"match": "/store/"}],
        [{"match": "", "prefix": "root://x/"}],
        [{"match": "/store/", "prefix": "root://x/", "replace": True}],
    ],
)
def test_malformed_rules_are_rejected(
    toy_registry: dict[str, Any], file_access: Any
) -> None:
    with pytest.raises(ValueError, match=r"execution\.file_access"):
        _plan(_workflow(toy_registry, ["/store/mc/a.root"], file_access))
