from __future__ import annotations

import hashlib
import shutil
from pathlib import Path
from typing import Any

import yaml

from hepflow.api import compile_workflow_file, normalise_workflow_file
from hepflow.compiler.bundle import plan_hash, read_invocation, write_invocation


def _write_project(project: Path, workflow: dict[str, Any]) -> Path:
    project.mkdir(parents=True, exist_ok=True)
    entry = project / "workflow.yaml"
    entry.write_text(yaml.safe_dump(workflow, sort_keys=False), encoding="utf-8")
    return entry


def test_invocation_round_trip(tmp_path: Path) -> None:
    compile_dir = tmp_path / "compile"
    compile_dir.mkdir()
    assert read_invocation(compile_dir) == {}

    path = write_invocation(
        compile_dir,
        workflow_path="w.yaml",
        outdir="out",
        root="/r",
        invocation={"tool": "x"},
    )
    data = read_invocation(compile_dir)

    assert path == compile_dir / "invocation.yaml"
    assert data["workflow_path"] == "w.yaml"
    assert data["outdir"] == "out"
    assert data["root"] == "/r"
    assert data["caller"] == {"tool": "x"}
    assert data["timestamp"].endswith(("Z", "+00:00"))
    assert data["hostname"]


def test_invocation_file_is_not_in_normalized_or_plan(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    entry = _write_project(tmp_path / "proj", toy_workflow)
    out = tmp_path / "build"

    compile_workflow_file(
        entry, outdir=out, root=tmp_path / "proj", invocation={"tool": "cli"}
    )

    invocation = read_invocation(out / "compile")
    assert invocation["caller"] == {"tool": "cli"}
    for name in ("normalized.yaml", "plan.yaml"):
        for line in (out / "compile" / name).read_text(encoding="utf-8").splitlines():
            assert "timestamp" not in line
            assert "hostname" not in line


def test_include_rows_in_manifest(tmp_path: Path, toy_workflow: dict[str, Any]) -> None:
    project = tmp_path / "proj"
    included = {"sources": toy_workflow.pop("sources")}
    (project / "parts").mkdir(parents=True)
    (project / "parts" / "sources.yaml").write_text(
        yaml.safe_dump(included), encoding="utf-8"
    )
    entry = _write_project(project, {"include": "parts/sources.yaml", **toy_workflow})

    normalized = normalise_workflow_file(entry, outdir=tmp_path / "b", root=project)

    rows = {r["role"]: r for r in normalized["provenance"]["inputs"]}
    assert set(rows) == {"workflow", "include"}
    roles = [r["role"] for r in normalized["provenance"]["inputs"]]
    assert roles.count("workflow") == 1
    assert roles.count("include") == 1
    assert rows["workflow"]["resolved"] == str(entry)
    assert rows["include"]["resolved"] == str(project / "parts" / "sources.yaml")
    assert all(len(r["sha256"]) == 64 for r in rows.values())


def test_local_profile_row(tmp_path: Path, toy_workflow: dict[str, Any]) -> None:
    project = tmp_path / "proj"
    (project / "profiles").mkdir(parents=True)
    profile = project / "profiles" / "x.yaml"
    profile.write_text(yaml.safe_dump({"execution": {"config": {"a": 1}}}))
    entry = _write_project(
        project, {**toy_workflow, "use": {"profiles": ["profiles/x.yaml", "basic"]}}
    )

    normalized = normalise_workflow_file(entry, outdir=tmp_path / "b", root=project)

    profiles = [r for r in normalized["provenance"]["inputs"] if r["role"] == "profile"]
    assert [r["resolved"] for r in profiles] == [str(profile)]
    assert len(profiles[0]["sha256"]) == 64


def test_packaged_profile_is_not_a_row(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    project = tmp_path / "proj"
    entry = _write_project(project, {**toy_workflow, "use": {"profiles": ["basic"]}})

    normalized = normalise_workflow_file(entry, outdir=tmp_path / "b", root=project)

    assert not [r for r in normalized["provenance"]["inputs"] if r["role"] == "profile"]


def _hash_of(entry: Path, root: Path, out: Path) -> dict[str, str]:
    compile_workflow_file(entry, outdir=out, root=root)
    return plan_hash(out / "compile" / "plan.yaml", str(root))


def test_plan_hash_ignores_root_prefix(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    first = tmp_path / "one"
    entry = _write_project(first, toy_workflow)
    second = tmp_path / "two"
    shutil.copytree(first, second)

    h1 = _hash_of(entry, first, tmp_path / "b1")
    h2 = _hash_of(second / "workflow.yaml", second, tmp_path / "b2")

    assert h1 == h2
    assert h1["canonicalization"] == "root-prefix"
    assert len(h1["sha256"]) == 64


def test_plan_hash_ignores_outdir(tmp_path: Path, toy_workflow: dict[str, Any]) -> None:
    project = tmp_path / "proj"
    entry = _write_project(project, toy_workflow)

    assert _hash_of(entry, project, tmp_path / "b1") == _hash_of(
        entry, project, tmp_path / "other" / "b2"
    )


def test_plan_hash_changes_with_the_plan(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    project = tmp_path / "proj"
    entry = _write_project(project, toy_workflow)
    before = _hash_of(entry, project, tmp_path / "b1")
    toy_workflow["analysis"]["stages"][0]["params"]["factor"] = 3
    _write_project(project, toy_workflow)

    assert _hash_of(entry, project, tmp_path / "b2") != before


def test_plan_hash_with_symlinked_root(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    real = tmp_path / "real"
    included = {"sources": toy_workflow.pop("sources")}
    (real / "parts").mkdir(parents=True)
    (real / "parts" / "sources.yaml").write_text(yaml.safe_dump(included))
    _write_project(real, {"include": "parts/sources.yaml", **toy_workflow})
    link = tmp_path / "link"
    link.symlink_to(real, target_is_directory=True)

    h_link = _hash_of(link / "workflow.yaml", link, tmp_path / "b1")
    h_real = _hash_of(real / "workflow.yaml", real, tmp_path / "b2")
    copy = tmp_path / "copy"
    shutil.copytree(real, copy)
    h_copy = _hash_of(copy / "workflow.yaml", copy, tmp_path / "b3")

    assert h_link == h_real == h_copy
    normalized = yaml.safe_load((tmp_path / "b1/compile/normalized.yaml").read_text())
    rows = [
        r
        for r in normalized["provenance"]["inputs"]
        if r["role"] in {"workflow", "include"}
    ]
    assert len(rows) == 2
    assert all(r["resolved"].startswith(str(link) + "/") for r in rows)


def test_plan_hash_replaces_at_path_boundaries(tmp_path: Path) -> None:
    plan = tmp_path / "plan.yaml"
    plan.write_text("a: /a/b/x\nb: /a/bc/x\n", encoding="utf-8")
    expected = hashlib.sha256(b"a: <root>/x\nb: /a/bc/x\n").hexdigest()

    assert plan_hash(plan, "/a/b")["sha256"] == expected
    assert plan_hash(plan, "/a/b/")["sha256"] == expected


def test_plan_hash_is_root_independent_for_roots_with_spaces(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    hashes: list[dict[str, str]] = []
    for name in (
        "my analysis project one with a rather long name",
        "another analysis checkout two",
    ):
        project = tmp_path / name
        entry = _write_project(project, toy_workflow)
        out = tmp_path / f"build-{len(hashes)}"
        compile_workflow_file(entry, outdir=out, root=project)
        hashes.append(plan_hash(out / "compile" / "plan.yaml", str(project)))

    assert hashes[0] == hashes[1]


def test_invocation_paths_and_sets_round_trip_as_plain_values(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    entry = _write_project(tmp_path / "proj", toy_workflow)
    out = tmp_path / "build"

    compile_workflow_file(
        entry,
        outdir=out,
        root=tmp_path / "proj",
        invocation={"out": Path("/x"), "tags": {"b", "a"}},
    )

    assert read_invocation(out / "compile")["caller"] == {
        "out": "/x",
        "tags": ["a", "b"],
    }
