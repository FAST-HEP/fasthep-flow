from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

import pytest
import yaml

from hepflow.api import compile_workflow_file
from hepflow.compiler.plan_diff import diff_plan_files, diff_plans
from hepflow.compiler.plan_diff.errors import InvalidPlan
from hepflow.compiler.plan_diff.model import DiffKind, DiffSection, PlanDiffReport

G = DiffSection.GRAPH
D = DiffSection.DATA
P = DiffSection.PROVENANCE
U = DiffSection.UNKNOWN


def _node(node_id: str, **params: Any) -> dict[str, Any]:
    return {
        "id": node_id,
        "role": "transform",
        "inputs": [{"node_id": "src", "output_name": "stream", "input_name": "stream"}],
        "params": params or {"x": 1},
        "meta": {"stage_id": node_id},
    }


def _partition(file_index: int, part: int = 0) -> dict[str, Any]:
    return {
        "id": f"events__ds__{file_index}_{part}",
        "source": "events",
        "dataset": "ds",
        "file": f"f{file_index}.root",
        "part": part,
    }


def _plan() -> dict[str, Any]:
    return {
        "context": {
            "workflow_path": "/a/wf.yaml",
            "project_root": "/a",
            "datasets": {"ds": {"files": ["f0.root", "f1.root"]}},
        },
        "nodes": [_node("n1"), _node("n2")],
        "partitions": [_partition(0), _partition(1)],
        "reports": [{"id": "r1", "outputs": [{"path": "a.png", "format": "png"}]}],
        "execution": {"backend": "local"},
        "execution_hooks": [{"events": ["start"]}, {"events": ["end"]}],
        "provenance": {
            "root": "/a",
            "inputs": [
                {"role": "workflow", "declared": "wf.yaml", "resolved": "/a/wf.yaml"}
            ],
            "compilation": {"flow_version": "1.0"},
        },
    }


def _diff(old: dict[str, Any], new: dict[str, Any]) -> PlanDiffReport:
    return diff_plans(old, new)


def _mutated(mutate: Any) -> dict[str, Any]:
    plan = copy.deepcopy(_plan())
    mutate(plan)
    return plan


def test_equal_plans_have_no_entries() -> None:
    report = _diff(_plan(), _plan())
    assert report.equal
    assert report.semantically_equal
    assert report.entries == ()


def test_mapping_key_order_is_irrelevant() -> None:
    reordered = dict(reversed(list(_plan().items())))
    assert _diff(_plan(), reordered).equal


def test_provenance_only_difference_is_not_semantic() -> None:
    def mutate(plan: dict[str, Any]) -> None:
        plan["context"]["workflow_path"] = "/b/wf.yaml"
        plan["provenance"]["root"] = "/b"

    report = _diff(_plan(), _mutated(mutate))
    assert not report.equal
    assert report.semantically_equal
    assert {e.section for e in report.entries} == {P}


@pytest.mark.parametrize(
    ("old", "new"), [(1, True), (1, 1.0), (1, "1"), (0, False), (None, 0), ("", None)]
)
def test_scalar_types_are_distinct(old: Any, new: Any) -> None:
    a = _mutated(lambda p: p["nodes"][0]["params"].update(x=old))
    b = _mutated(lambda p: p["nodes"][0]["params"].update(x=new))
    report = _diff(a, b)
    assert [e.kind for e in report.entries] == [DiffKind.CHANGED]


def test_floats_have_no_tolerance() -> None:
    a = _mutated(lambda p: p["nodes"][0]["params"].update(x=0.1))
    b = _mutated(lambda p: p["nodes"][0]["params"].update(x=0.1 + 1e-12))
    assert len(_diff(a, b).entries) == 1


def test_null_versus_absent_is_added_or_removed() -> None:
    with_null = _mutated(lambda p: p["nodes"][0]["params"].update(y=None))
    report = _diff(_plan(), with_null)
    (entry,) = report.entries
    assert entry.kind is DiffKind.ADDED
    assert entry.new_present
    assert entry.new is None
    assert not entry.old_present
    (back,) = _diff(with_null, _plan()).entries
    assert back.kind is DiffKind.REMOVED
    assert back.old_present
    assert back.old is None


def test_changed_unknown_field_is_unknown_and_semantic() -> None:
    a = _mutated(lambda p: p.update(brand_new=1))
    b = _mutated(lambda p: p.update(brand_new=2))
    report = _diff(a, b)
    assert [e.section for e in report.entries] == [U]
    assert not report.semantically_equal


def test_new_top_level_key_is_unknown() -> None:
    report = _diff(_plan(), _mutated(lambda p: p.update(extra={"a": 1})))
    (entry,) = report.entries
    assert (entry.section, entry.kind) == (U, DiffKind.ADDED)


def test_one_node_parameter_change_is_one_graph_entry() -> None:
    report = _diff(_plan(), _mutated(lambda p: p["nodes"][0]["params"].update(x=2)))
    (entry,) = report.entries
    assert (entry.section, entry.kind) == (G, DiffKind.CHANGED)
    assert entry.rendered_location == "nodes[id='n1'].params.x"
    assert (entry.old, entry.new) == (1, 2)


def test_inserted_node_is_one_added_entry_keyed_by_id() -> None:
    report = _diff(_plan(), _mutated(lambda p: p["nodes"].insert(1, _node("mid", x=9))))
    (entry,) = report.entries
    assert (entry.section, entry.kind) == (G, DiffKind.ADDED)
    assert entry.identity == {"id": "mid"}
    assert entry.new["id"] == "mid"
    assert not entry.old_present


def test_removed_node_is_one_removed_entry() -> None:
    report = _diff(_plan(), _mutated(lambda p: p["nodes"].pop(0)))
    (entry,) = report.entries
    assert (entry.kind, entry.identity) == (DiffKind.REMOVED, {"id": "n1"})


def test_node_reorder_is_equal() -> None:
    assert _diff(_plan(), _mutated(lambda p: p["nodes"].reverse())).equal


def test_partition_matched_by_source_dataset_file_part() -> None:
    # Inserting file f0 first shifts the file index embedded in every partition id,
    # but not the identities: f0 is one addition and the two shifted ids are leaf changes.
    old_partitions = [
        dict(_partition(1), id="events__ds__0_0"),
        dict(_partition(2), id="events__ds__1_0"),
    ]
    new_partitions = [
        dict(_partition(0), id="events__ds__0_0"),
        dict(_partition(1), id="events__ds__1_0"),
        dict(_partition(2), id="events__ds__2_0"),
    ]
    old = _mutated(lambda p: p.update(partitions=old_partitions))
    new = _mutated(lambda p: p.update(partitions=new_partitions))
    entries = _diff(old, new).entries
    assert {e.section for e in entries} == {D}
    assert sorted(e.kind.value for e in entries) == ["added", "changed", "changed"]
    (added,) = (e for e in entries if e.kind is DiffKind.ADDED)
    assert added.identity == {
        "source": "events",
        "dataset": "ds",
        "file": "f0.root",
        "part": 0,
    }


def test_node_input_matched_by_node_id_and_input_name() -> None:
    report = _diff(
        _plan(),
        _mutated(lambda p: p["nodes"][0]["inputs"][0].update(output_name="other")),
    )
    (entry,) = report.entries
    assert entry.kind is DiffKind.CHANGED
    assert entry.rendered_location.endswith(
        "inputs[node_id='src',input_name='stream'].output_name"
    )


def test_report_matched_by_id() -> None:
    report = _diff(
        _plan(),
        _mutated(lambda p: p["reports"].insert(0, {"id": "r0", "outputs": []})),
    )
    (entry,) = report.entries
    assert (entry.section, entry.kind, entry.identity) == (
        DiffSection.REPORTS,
        DiffKind.ADDED,
        {"id": "r0"},
    )


def test_provenance_input_rows_matched_by_role_and_declared() -> None:
    report = _diff(
        _plan(),
        _mutated(lambda p: p["provenance"]["inputs"][0].update(resolved="/b/wf.yaml")),
    )
    (entry,) = report.entries
    assert (entry.section, entry.kind) == (P, DiffKind.CHANGED)


def test_exact_duplicate_provenance_rows_collapse() -> None:
    def duplicate(plan: dict[str, Any]) -> None:
        plan["provenance"]["inputs"].append(dict(plan["provenance"]["inputs"][0]))

    assert _diff(_plan(), _mutated(duplicate)).equal


def test_non_identical_duplicate_rows_give_identity_conflict_and_fallback_entries() -> (
    None
):
    def duplicate(plan: dict[str, Any]) -> None:
        row = dict(plan["provenance"]["inputs"][0], resolved="/elsewhere/wf.yaml")
        plan["provenance"]["inputs"].append(row)

    report = _diff(_plan(), _mutated(duplicate))
    kinds = [e.kind for e in report.entries]
    assert DiffKind.IDENTITY_CONFLICT in kinds
    assert all(
        e.fallback for e in report.entries if e.kind is not DiffKind.IDENTITY_CONFLICT
    )
    assert report.semantically_equal


def test_layer_lists_matched_by_kind_and_name() -> None:
    layers = [{"kind": "profile", "name": "a"}, {"kind": "profile", "name": "b"}]
    old = _mutated(lambda p: p["provenance"].update(registry_layers=layers))
    new = _mutated(lambda p: p["provenance"].update(registry_layers=layers[::-1]))
    assert _diff(old, new).equal


@pytest.mark.parametrize(
    "mutate",
    [
        lambda p: p["nodes"].append(_node("n1")),
        lambda p: p["partitions"].append(_partition(0)),
        lambda p: p["reports"].append({"id": "r1"}),
    ],
)
def test_duplicate_strict_identities_are_invalid(mutate: Any) -> None:
    with pytest.raises(InvalidPlan):
        _diff(_plan(), _mutated(mutate))


def test_anonymous_insertion_in_the_middle_is_one_added() -> None:
    old = _mutated(lambda p: p.update(execution_hooks=[{"e": i} for i in range(6)]))
    new = _mutated(
        lambda p: p.update(
            execution_hooks=[{"e": 0}, {"e": 1}, {"e": 2}, {"e": "new"}]
            + [{"e": i} for i in range(3, 6)]
        )
    )
    (entry,) = _diff(old, new).entries
    assert (entry.section, entry.kind) == (DiffSection.EXECUTION, DiffKind.ADDED)
    assert entry.new == {"e": "new"}


def test_anonymous_pure_reorder_is_a_difference() -> None:
    new = _mutated(lambda p: p["execution_hooks"].reverse())
    report = _diff(_plan(), new)
    assert not report.equal
    assert {e.section for e in report.entries} == {DiffSection.EXECUTION}


def test_changed_anonymous_item_is_a_leaf_change_at_its_index() -> None:
    new = _mutated(lambda p: p["execution_hooks"][1].update(events=["stop"]))
    (entry,) = _diff(_plan(), new).entries
    assert entry.kind is DiffKind.CHANGED
    assert entry.rendered_location == "execution_hooks[1].events[0]"


def test_compile_hooks_record_changes_are_provenance_only() -> None:
    old = _mutated(
        lambda p: p["nodes"][0]["meta"].update(
            compile_hooks={"param": [{"hook": "flow.load_mapping", "sha256": "a"}]}
        )
    )
    new = _mutated(
        lambda p: p["nodes"][0]["meta"].update(
            compile_hooks={"param": [{"hook": "flow.load_mapping", "sha256": "b"}]}
        )
    )
    report = _diff(old, new)
    assert not report.equal
    assert report.semantically_equal


def test_flow_version_mismatch_warns_and_adds_provenance_entry() -> None:
    report = _diff(
        _plan(),
        _mutated(lambda p: p["provenance"]["compilation"].update(flow_version="2.0")),
    )
    (entry,) = report.entries
    assert (entry.section, entry.kind) == (P, DiffKind.CHANGED)
    (warning,) = report.warnings
    assert warning.code == "flow_version_mismatch"
    assert warning.details == {"old": "1.0", "new": "2.0"}
    assert report.semantically_equal


def test_flow_version_unknown_names_the_missing_side() -> None:
    new = _mutated(lambda p: p["provenance"].pop("compilation"))
    (warning,) = _diff(_plan(), new).warnings
    assert warning.code == "flow_version_unknown"
    assert warning.details == {"missing": ["new"]}
    both = _diff(new, new)
    assert both.warnings == ()


def test_warnings_do_not_change_equality() -> None:
    same = _diff(_plan(), _plan())
    assert same.warnings == ()
    assert same.equal


def test_large_plan_inserting_one_node_yields_exactly_one_addition() -> None:
    old = _plan()
    old["nodes"] = [_node(f"n{i}", x=i) for i in range(5000)]
    new = copy.deepcopy(old)
    new["nodes"].insert(2500, _node("inserted", x=-1))
    (entry,) = _diff(old, new).entries
    assert entry.kind is DiffKind.ADDED
    assert entry.identity == {"id": "inserted"}


def test_diff_plan_files_rejects_non_mapping_root_and_bad_yaml(tmp_path: Path) -> None:
    good = tmp_path / "good.yaml"
    good.write_text(yaml.safe_dump(_plan()), encoding="utf-8")
    for name, text in {
        "list.yaml": "- 1\n- 2\n",
        "bad.yaml": "a: [unclosed\n",
        "empty.yaml": "",
    }.items():
        path = tmp_path / name
        path.write_text(text, encoding="utf-8")
        with pytest.raises(InvalidPlan):
            diff_plan_files(good, path)


def test_diff_plan_files_on_real_compiled_plans(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    plans = []
    for name in ("one", "two"):
        directory = tmp_path / name
        directory.mkdir()
        workflow = directory / "wf.yaml"
        workflow.write_text(
            yaml.safe_dump(toy_workflow, sort_keys=False), encoding="utf-8"
        )
        compile_workflow_file(workflow, outdir=directory / "build", root=directory)
        plans.append(directory / "build" / "compile" / "plan.yaml")

    report = diff_plan_files(*plans)
    assert report.semantically_equal
    assert report.entries
    assert {e.section for e in report.entries} == {P}


def test_a_plan_with_duplicate_identities_equals_itself() -> None:
    def duplicate_rows(plan: dict[str, Any]) -> None:
        first = plan["provenance"]["inputs"][0]
        plan["provenance"]["inputs"].append(dict(first, resolved="/elsewhere/wf.yaml"))
        plan["nodes"][0]["inputs"].append(
            {"node_id": "src", "output_name": "other", "input_name": "stream"}
        )

    plan = _mutated(duplicate_rows)
    report = _diff(plan, copy.deepcopy(plan))
    assert report.equal
    assert report.entries == ()


def test_conflict_is_reported_only_when_duplicate_groups_differ() -> None:
    def duplicate_rows(plan: dict[str, Any]) -> None:
        first = plan["provenance"]["inputs"][0]
        plan["provenance"]["inputs"].append(dict(first, resolved="/elsewhere/wf.yaml"))

    old = _mutated(duplicate_rows)
    new = copy.deepcopy(old)
    new["provenance"]["inputs"][1]["resolved"] = "/changed/wf.yaml"
    report = _diff(old, new)
    assert DiffKind.IDENTITY_CONFLICT in [e.kind for e in report.entries]
    assert report.semantically_equal


def test_changed_path_leaf_carries_displays_relative_to_each_root() -> None:
    old = _mutated(lambda p: p["context"].update(workflow_path="/a/wf.yaml"))
    new = _mutated(
        lambda p: p["context"].update(
            workflow_path="/b/skim/wf.yaml", project_root="/b"
        )
    )
    entries = [
        e
        for e in _diff(old, new).entries
        if e.rendered_location == "context.workflow_path"
    ]
    (entry,) = entries
    assert (entry.old, entry.new) == ("/a/wf.yaml", "/b/skim/wf.yaml")
    assert entry.old_display == {"value": "wf.yaml", "label": "relative"}
    assert entry.new_display == {"value": "skim/wf.yaml", "label": "relative"}


def test_non_path_leaf_gets_no_display() -> None:
    (entry,) = _diff(
        _plan(), _mutated(lambda p: p["nodes"][0]["params"].update(path="b.txt", x=1))
    ).entries
    assert entry.old_display is None
    assert entry.new_display is None


def test_path_display_without_a_recorded_root() -> None:
    old = _mutated(lambda p: p["context"].pop("project_root"))
    new = copy.deepcopy(old)
    new["context"]["workflow_path"] = "/c/wf.yaml"
    (entry,) = _diff(old, new).entries
    assert entry.new_display == {"value": "/c/wf.yaml", "label": "no_root"}


def test_root_fields_are_displayed_as_written_with_the_root_label() -> None:
    old = _mutated(lambda p: p["provenance"].update(root="/a"))
    new = _mutated(lambda p: p["provenance"].update(root="/b"))
    (entry,) = _diff(old, new).entries
    assert entry.old_display == {"value": "/a", "label": "root"}
    assert entry.new_display == {"value": "/b", "label": "root"}


def test_a_directory_is_not_a_plan_file(tmp_path: Path) -> None:
    good = tmp_path / "good.yaml"
    good.write_text(yaml.safe_dump(_plan()), encoding="utf-8")
    with pytest.raises(InvalidPlan):
        diff_plan_files(good, tmp_path)
