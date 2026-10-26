from __future__ import annotations

import hashlib
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import networkx as nx
import pytest
import yaml

from hepflow.api import compile_workflow_file
from hepflow.compiler import bundle as bundle_module
from hepflow.compiler.artifacts import write_plan_artifacts
from hepflow.compiler.bundle import bundle_id, plan_hash
from hepflow.compiler.hooks import software as software_module
from hepflow.compiler.inputs_manifest import write_inputs_manifest
from hepflow.model.plan import ExecutionPlan
from hepflow.registry.defaults import default_runtime_registry_config

REAL_SOFTWARE_CAPTURE = True
SOFTWARE_HOOK = {
    "impl": "hepflow.compiler.hooks.software:software",
    "spec": "hepflow.compiler.hooks.software:SOFTWARE_HOOK_SPEC",
    "params": {"git": False},
}
V2_KEYS = [
    "version",
    "root",
    "bundle_id",
    "plan_hash",
    "software",
    "invocation",
    "hooks",
    "inputs",
]


_TOY = "tests.toy_components.compile_hooks"


def _project(
    tmp_path: Path,
    toy_workflow: dict[str, Any],
    extra_hooks: dict[str, Any] | None = None,
) -> Path:
    project = tmp_path / "proj"
    (project / "parts").mkdir(parents=True)
    included = {"sources": toy_workflow.pop("sources")}
    (project / "parts" / "sources.yaml").write_text(
        yaml.safe_dump(included), encoding="utf-8"
    )
    registry = dict(toy_workflow["registry"])
    registry["compile_hooks"] = {
        **dict(registry.get("compile_hooks") or {}),
        "flow.software": SOFTWARE_HOOK,
        **(extra_hooks or {}),
    }
    workflow = {"include": "parts/sources.yaml", **toy_workflow, "registry": registry}
    entry = project / "workflow.yaml"
    entry.write_text(yaml.safe_dump(workflow, sort_keys=False), encoding="utf-8")
    return entry


def _manifest(compile_dir: Path) -> dict[str, Any]:
    text = (compile_dir / "inputs.manifest.yaml").read_text(encoding="utf-8")
    return yaml.safe_load(text)


def _set_clock(monkeypatch: pytest.MonkeyPatch, when: datetime) -> None:
    class _Clock(datetime):
        @classmethod
        def now(cls, tz: Any = None) -> Any:
            return when

    monkeypatch.setattr(bundle_module, "datetime", _Clock)


def test_manifest_version_2_keys_in_order(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    entry = _project(tmp_path, toy_workflow)
    out = tmp_path / "build"

    compile_workflow_file(
        entry, outdir=out, root=entry.parent, chunk_size=7, invocation={"tool": "t"}
    )

    manifest = _manifest(out / "compile")
    assert list(manifest) == V2_KEYS
    assert manifest["version"] == 2
    assert manifest["plan_hash"] == plan_hash(
        out / "compile" / "plan.yaml", str(entry.parent)
    )
    assert manifest["bundle_id"] == bundle_id(manifest)
    assert manifest["invocation"]["chunk_size"] == 7
    assert manifest["invocation"]["caller"] == {"tool": "t"}
    assert manifest["invocation"]["timestamp"]
    hooks = {record["name"]: record for record in manifest["hooks"]}
    assert hooks["flow.software"]["params"] == {"git": False}
    assert hooks["flow.software"]["phase"] == "after_datasets"


def test_software_listed_not_inlined(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    entry = _project(tmp_path, toy_workflow)
    out = tmp_path / "build"

    compile_workflow_file(entry, outdir=out, root=entry.parent)

    manifest = _manifest(out / "compile")
    data = (out / "compile" / "software.json").read_bytes()
    assert manifest["software"] == {
        "path": "software.json",
        "sha256": hashlib.sha256(data).hexdigest(),
        "size": len(data),
    }
    text = (out / "compile" / "inputs.manifest.yaml").read_text(encoding="utf-8")
    assert "distributions" not in text


def test_bundle_id_stable_across_outdirs_and_time(
    tmp_path: Path, toy_workflow: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    entry = _project(tmp_path, toy_workflow)

    _set_clock(monkeypatch, datetime(2026, 1, 1, tzinfo=UTC))
    compile_workflow_file(entry, outdir=tmp_path / "a", root=entry.parent)
    _set_clock(monkeypatch, datetime(2026, 6, 1, tzinfo=UTC))
    compile_workflow_file(
        entry, outdir=tmp_path / "b", root=entry.parent, invocation={"tool": "cli"}
    )

    first = _manifest(tmp_path / "a" / "compile")
    second = _manifest(tmp_path / "b" / "compile")
    assert first["invocation"]["timestamp"] != second["invocation"]["timestamp"]
    assert first["invocation"]["outdir"] != second["invocation"]["outdir"]
    assert first["bundle_id"] == second["bundle_id"]


def test_bundle_id_changes_with_an_input_file(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    entry = _project(tmp_path, toy_workflow)
    compile_workflow_file(entry, outdir=tmp_path / "a", root=entry.parent)

    include = entry.parent / "parts" / "sources.yaml"
    include.write_text(include.read_text(encoding="utf-8") + "# edited\n")
    compile_workflow_file(entry, outdir=tmp_path / "b", root=entry.parent)

    first = _manifest(tmp_path / "a" / "compile")
    second = _manifest(tmp_path / "b" / "compile")
    assert first["bundle_id"] != second["bundle_id"]


def test_systematics_each_variation_has_a_manifest(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    toy_workflow["systematics"] = {
        "include_nominal": True,
        "variations": [
            {
                "name": "trigger_eff_up",
                "group": "trigger_eff",
                "direction": "up",
                "applies_to": "mc",
                "requires": ["stage.TriggerEfficiencyWeights"],
                "weight": {"multiply": "TriggerEffWeight_up"},
            }
        ],
    }
    entry = _project(tmp_path, toy_workflow)
    out = tmp_path / "build"

    compile_workflow_file(entry, outdir=out, root=entry.parent, chunk_size=5)

    ids = set()
    for variation in ("nominal", "trigger_eff_up"):
        compile_dir = out / "compile" / variation
        manifest = _manifest(compile_dir)
        assert list(manifest) == V2_KEYS
        assert manifest["plan_hash"] == plan_hash(
            compile_dir / "plan.yaml", str(entry.parent)
        )
        assert manifest["bundle_id"] == bundle_id(manifest)
        # invocation.yaml lives in the base compile directory.
        assert manifest["invocation"]["outdir"] == str(out)
        assert manifest["invocation"]["chunk_size"] == 5
        assert manifest["software"]["path"] == "software.json"
        assert (compile_dir / "software.json").is_file()
        ids.add(manifest["bundle_id"])
    assert len(ids) == 2
    assert not (out / "compile" / "inputs.manifest.yaml").exists()


def _plan_without_software_hook() -> ExecutionPlan:
    registry = default_runtime_registry_config()
    registry["compile_hooks"] = {
        name: entry
        for name, entry in registry["compile_hooks"].items()
        if name != "flow.software"
    }
    return ExecutionPlan(context={"datasets": {}}, registry=registry)


def test_no_software_hook_gives_null(tmp_path: Path) -> None:
    plan = _plan_without_software_hook()

    write_plan_artifacts(
        plan=plan, graph=nx.DiGraph(), outdir=tmp_path, normalized={"sources": {}}
    )

    manifest = _manifest(tmp_path / "compile")
    assert list(manifest) == V2_KEYS
    assert manifest["software"] is None
    assert "flow.software" not in [record["name"] for record in manifest["hooks"]]
    assert manifest["invocation"] == {"chunk_size": None}


def test_stale_software_json_is_not_listed(tmp_path: Path) -> None:
    (tmp_path / "compile").mkdir()
    (tmp_path / "compile" / "software.json").write_text("{}", encoding="utf-8")
    plan = _plan_without_software_hook()

    write_plan_artifacts(
        plan=plan, graph=nx.DiGraph(), outdir=tmp_path, normalized={"sources": {}}
    )

    assert _manifest(tmp_path / "compile")["software"] is None


def test_bundle_id_ignores_invocation_and_volatile_keys() -> None:
    manifest: dict[str, Any] = {
        "version": 2,
        "root": "/r",
        "bundle_id": "x",
        "plan_hash": {"sha256": "p", "canonicalization": "root-prefix"},
        "software": None,
        "invocation": {"timestamp": "t1", "hostname": "h1"},
        "hooks": [],
        "inputs": [],
    }
    other = {**manifest, "bundle_id": "y", "invocation": {"timestamp": "t2"}}
    changed = {**manifest, "plan_hash": {"sha256": "q"}}

    assert bundle_id(manifest) == bundle_id(other)
    assert bundle_id(manifest) != bundle_id(changed)
    assert len(bundle_id(manifest)) == 64


def test_hook_params_are_copied_and_made_yaml_safe(tmp_path: Path) -> None:
    params: dict[str, Any] = {
        "nested": {"p": Path("/x")},
        "items": (1, 2),
        "tags": {"b", "a"},
        "other": object,
    }
    hooks = [{"name": "h", "phase": "after_compile", "params": params, "artifacts": {}}]

    write_inputs_manifest(tmp_path, root="/r", inputs=[], hooks=hooks)

    written = _manifest(tmp_path)["hooks"][0]["params"]
    assert written == {
        "nested": {"p": "/x"},
        "items": [1, 2],
        "tags": ["a", "b"],
        "other": repr(object),
    }


def test_bundle_id_ignores_outdir_dependent_hook_artifacts(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    hook = {
        "spec": f"{_TOY}:OUTDIR_ARTIFACT_HOOK_SPEC",
        "impl": f"{_TOY}:outdir_artifact_hook",
    }
    entry = _project(tmp_path, toy_workflow, {"toy.outdir_artifact": hook})

    compile_workflow_file(entry, outdir=tmp_path / "a", root=entry.parent)
    compile_workflow_file(entry, outdir=tmp_path / "b", root=entry.parent)

    first = _manifest(tmp_path / "a" / "compile")
    second = _manifest(tmp_path / "b" / "compile")

    def artifact(manifest: dict[str, Any]) -> dict[str, Any]:
        (record,) = [r for r in manifest["hooks"] if r["name"] == "toy.outdir_artifact"]
        return dict(record["artifacts"]["outdir_artifact"])

    assert artifact(first)["sha256"] != artifact(second)["sha256"]
    assert first["plan_hash"] == second["plan_hash"]
    assert first["bundle_id"] == second["bundle_id"]


def test_software_json_still_changes_bundle_id(
    tmp_path: Path, toy_workflow: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    entry = _project(tmp_path, toy_workflow)

    monkeypatch.setattr(software_module, "python_info", lambda: {"version": "3.0"})
    compile_workflow_file(entry, outdir=tmp_path / "a", root=entry.parent)
    monkeypatch.setattr(software_module, "python_info", lambda: {"version": "3.1"})
    compile_workflow_file(entry, outdir=tmp_path / "b", root=entry.parent)

    first = _manifest(tmp_path / "a" / "compile")
    second = _manifest(tmp_path / "b" / "compile")
    assert first["software"]["sha256"] != second["software"]["sha256"]
    assert first["bundle_id"] != second["bundle_id"]


def test_bundle_id_counts_hook_artifact_names_not_hashes() -> None:
    def manifest(artifacts: dict[str, Any]) -> dict[str, Any]:
        record = {"name": "h", "phase": "after_compile", "params": {}}
        return {"version": 2, "hooks": [{**record, "artifacts": artifacts}]}

    one = manifest({"a": {"path": "a.json", "sha256": "1"}})
    two = manifest({"a": {"path": "a.json", "sha256": "2"}})
    renamed = manifest({"b": {"path": "b.json", "sha256": "1"}})

    assert bundle_id(one) == bundle_id(two)
    assert bundle_id(one) != bundle_id(renamed)


def test_failed_recompile_removes_the_old_manifest(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    good = _project(tmp_path / "good", deepcopy(toy_workflow))
    failing = {
        "spec": f"{_TOY}:FAILING_AFTER_COMPILE_HOOK_SPEC",
        "impl": f"{_TOY}:failing_compile_hook",
    }
    bad = _project(tmp_path / "bad", toy_workflow, {"toy.fail": failing})
    out = tmp_path / "build"

    compile_workflow_file(good, outdir=out, root=good.parent)
    assert (out / "compile" / "inputs.manifest.yaml").is_file()

    with pytest.raises(RuntimeError, match="compile boom"):
        compile_workflow_file(bad, outdir=out, root=bad.parent)

    assert (out / "compile" / "plan.yaml").is_file()
    assert not (out / "compile" / "inputs.manifest.yaml").exists()
