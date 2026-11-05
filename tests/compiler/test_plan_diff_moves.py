from __future__ import annotations

import copy
import hashlib
import json
from typing import Any

import pytest

from hepflow.compiler.plan_diff import diff_plans
from hepflow.compiler.plan_diff.model import DiffKind, DiffSection, PlanDiffReport

P = DiffSection.PROVENANCE
SHA = hashlib.sha256(b"x").hexdigest()
SHA2 = hashlib.sha256(b"y").hexdigest()


def _row(declared: str, sha: Any = SHA, role: str = "workflow", **extra: Any) -> dict:
    row = {
        "role": role,
        "declared": declared,
        "resolved": f"/a/proj/{declared}",
        "origin": "root",
        "sha256": sha,
        "size": 1,
        "note": None,
    }
    row.update(extra)
    return row


def _plan(rows: list[dict[str, Any]], root: str = "/a/proj") -> dict[str, Any]:
    return {
        "context": {"project_root": root, "workflow_path": f"{root}/wf.yaml"},
        "nodes": [],
        "provenance": {"root": root, "inputs": rows},
    }


def _diff(old_rows: list[dict], new_rows: list[dict]) -> PlanDiffReport:
    return diff_plans(_plan(old_rows), _plan(new_rows))


def test_unique_equal_hash_relocation_is_one_moved_entry() -> None:
    report = _diff([_row("skim/legacy/x.yaml")], [_row("skim/productions/x.yaml")])
    (entry,) = report.entries
    assert (entry.section, entry.kind) == (P, DiffKind.MOVED)
    assert not report.equal
    assert report.semantically_equal


def test_moved_entry_carries_raw_rows_identity_and_displays() -> None:
    old = _row("skim/legacy/x.yaml")
    new = _row("skim/productions/x.yaml")
    (entry,) = _diff([old], [new]).entries
    assert (entry.old, entry.new) == (old, new)
    assert entry.identity == {"role": "workflow", "sha256": SHA}
    assert entry.old_present
    assert entry.new_present
    assert entry.old_display["declared"] == {
        "value": "skim/legacy/x.yaml",
        "label": "relative",
    }
    assert entry.new_display["resolved"] == {
        "value": "skim/productions/x.yaml",
        "label": "relative",
    }
    assert entry.rendered_location.startswith("provenance.inputs[role='workflow'")


def test_changed_size_prevents_a_move_and_leaves_added_and_removed() -> None:
    report = _diff([_row("a.yaml")], [_row("b.yaml", size=2)])
    assert sorted(e.kind.value for e in report.entries) == ["added", "removed"]


def test_changed_note_prevents_a_move() -> None:
    report = _diff([_row("a.yaml")], [_row("b.yaml", note="changed")])
    assert DiffKind.MOVED not in [e.kind for e in report.entries]


def test_changed_role_prevents_a_move() -> None:
    report = _diff([_row("a.yaml")], [_row("b.yaml", role="other")])
    assert sorted(e.kind.value for e in report.entries) == ["added", "removed"]


def test_ambiguous_hashes_are_not_guessed() -> None:
    two_old = _diff([_row("a.yaml"), _row("b.yaml")], [_row("c.yaml")])
    assert DiffKind.MOVED not in [e.kind for e in two_old.entries]
    assert len(two_old.entries) == 3
    two_new = _diff([_row("a.yaml")], [_row("b.yaml"), _row("c.yaml")])
    assert DiffKind.MOVED not in [e.kind for e in two_new.entries]
    assert len(two_new.entries) == 3


@pytest.mark.parametrize(
    "bad",
    [None, "abc", SHA[:63], SHA.upper(), SHA[:-1] + "g", 12345, "x" * 64],
)
def test_missing_or_malformed_hash_prevents_a_move(bad: Any) -> None:
    report = _diff([_row("a.yaml", sha=bad)], [_row("b.yaml", sha=bad)])
    assert DiffKind.MOVED not in [e.kind for e in report.entries]
    absent_old = _row("a.yaml")
    del absent_old["sha256"]
    absent_new = _row("b.yaml")
    del absent_new["sha256"]
    report = _diff([absent_old], [absent_new])
    assert DiffKind.MOVED not in [e.kind for e in report.entries]


def test_same_identity_changed_resolved_is_a_leaf_change_not_a_move() -> None:
    old = _row("a.yaml")
    new = _row("a.yaml", resolved="/b/proj/a.yaml")
    (entry,) = _diff([old], [new]).entries
    assert entry.kind is DiffKind.CHANGED
    assert entry.rendered_location.endswith(".resolved")


def test_several_independent_moves_are_each_reported() -> None:
    old = [
        _row(f"old/{n}.yaml", sha=hashlib.sha256(n.encode()).hexdigest()) for n in "abc"
    ]
    new = [
        _row(f"new/{n}.yaml", sha=hashlib.sha256(n.encode()).hexdigest()) for n in "abc"
    ]
    report = _diff(old, new)
    assert [e.kind for e in report.entries] == [DiffKind.MOVED] * 3


def test_unmatched_row_without_a_partner_stays_added() -> None:
    report = _diff([_row("a.yaml")], [_row("a.yaml"), _row("b.yaml", sha=SHA2)])
    (entry,) = report.entries
    assert entry.kind is DiffKind.ADDED


def test_motivating_relocation_is_provenance_only() -> None:
    old = _plan(
        [_row("skim/legacy/Run3/x.yaml"), _row("data/cfg.json", role="param:S.p")],
        root="/a/chip",
    )
    new = _plan(
        [_row("skim/productions/x.yaml"), _row("data/cfg.json", role="param:S.p")],
        root="/b/chip",
    )
    for plan, root in ((old, "/a/chip"), (new, "/b/chip")):
        for row in plan["provenance"]["inputs"]:
            row["resolved"] = f"{root}/{row['declared']}"
        plan["nodes"] = [
            {
                "id": "n",
                "meta": {
                    "compile_hooks": {
                        "p": [{"hook": "h", "resolved_path": f"{root}/s.yaml"}]
                    }
                },
            }
        ]
    report = diff_plans(old, new)
    assert report.semantically_equal
    assert not report.equal
    assert {e.section for e in report.entries} == {P}
    assert {e.kind for e in report.entries} <= {DiffKind.CHANGED, DiffKind.MOVED}
    assert sum(e.kind is DiffKind.MOVED for e in report.entries) == 1


def test_moved_roundtrips_through_to_dict() -> None:
    report = _diff([_row("a.yaml")], [_row("b.yaml")])
    data = report.to_dict()
    json.dumps(data, sort_keys=True)
    (entry,) = data["entries"]
    assert entry["kind"] == "moved"
    assert "old_display" in entry
    assert "new_display" in entry
    assert data["counts"]["kinds"]["moved"] == 1


def test_moves_do_not_mutate_the_plans() -> None:
    old, new = _plan([_row("a.yaml")]), _plan([_row("b.yaml")])
    before = (copy.deepcopy(old), copy.deepcopy(new))
    diff_plans(old, new)
    assert (old, new) == before
