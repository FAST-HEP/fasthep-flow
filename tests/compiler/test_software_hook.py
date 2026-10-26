from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from hepflow.build_layout import BuildPaths
from hepflow.compiler.hooks import software as software_module
from hepflow.compiler.hooks.model import CompileHookContext
from hepflow.compiler.hooks.software import SOFTWARE_HOOK_SPEC, software
from hepflow.registry.defaults import default_runtime_registry_config

REAL_SOFTWARE_CAPTURE = True
SOFTWARE_IMPL = "hepflow.compiler.hooks.software:software"
SOFTWARE_SPEC = "hepflow.compiler.hooks.software:SOFTWARE_HOOK_SPEC"


def _context(
    tmp_path: Path, provenance: dict[str, Any] | None = None
) -> CompileHookContext:
    return CompileHookContext(
        normalized={"provenance": provenance or {}},
        plan_context={"project_root": str(tmp_path)},
        build_paths=BuildPaths(root=tmp_path / "out"),
    )


def test_software_artifact_has_python_and_distributions(tmp_path: Path) -> None:
    (tmp_path / "pixi.lock").write_text("version: 6\n", encoding="utf-8")

    result = software(_context(tmp_path))

    assert set(result) == {"software"}
    data = result["software"]
    assert set(data) == {
        "python",
        "distributions",
        "direct_urls",
        "git",
        "layers",
        "lockfile",
        "environment",
    }
    assert data["python"]["version"]
    names = {row["name"] for row in data["distributions"]}
    assert "pytest" in names
    assert all(set(row) == {"name", "version"} for row in data["distributions"])
    assert set(data["git"]) == {"root", "editable"}
    assert data["lockfile"]["name"] == "pixi.lock"
    for row in data["direct_urls"]:
        assert set(row) == {"name", "url", "vcs", "commit_id", "editable"}


def test_software_layers_come_from_registry_provenance(tmp_path: Path) -> None:
    provenance = {
        "registry_layers": [
            {"name": "builtin", "kind": "builtin"},
            {
                "name": "hepflow:default",
                "kind": "profile",
                "path": "package:hepflow.profiles/default.yaml",
            },
            {"name": "./local.yaml", "kind": "profile", "path": "/x/local.yaml"},
            {"name": "workflow", "kind": "workflow", "path": "/x/workflow.yaml"},
        ]
    }

    data = software(_context(tmp_path, provenance))["software"]

    assert data["layers"] == ["fasthep-flow"]


def test_environment_is_allowlisted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FASTHEP_X", "1")
    monkeypatch.setenv("HEPFLOW_Y", "2")
    monkeypatch.setenv("SECRET_TOKEN", "x")

    env = software(_context(tmp_path))["software"]["environment"]

    assert env["FASTHEP_X"] == "1"
    assert env["HEPFLOW_Y"] == "2"
    assert "SECRET_TOKEN" not in env
    assert all(key.startswith(("FASTHEP_", "HEPFLOW_")) for key in env)


def test_missing_git_gives_null_and_note(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("PATH", "")

    data = software(_context(tmp_path))["software"]

    assert data["git"]["root"]["revision"] is None
    assert data["git"]["root"]["note"]


def test_hook_spec_runs_after_datasets() -> None:
    assert SOFTWARE_HOOK_SPEC["lifecycle"] == {"when": "after_datasets"}
    assert SOFTWARE_HOOK_SPEC["result"] == {"artifacts": ["software"]}


def test_default_registry_and_profile_yaml_list_flow_software() -> None:
    defaults = default_runtime_registry_config()["compile_hooks"]["flow.software"]
    yaml_path = Path(__file__).parents[2] / "src/hepflow/profiles/registry.yaml"
    loaded = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
    profile = loaded["registry"]["compile_hooks"]["flow.software"]

    assert defaults["impl"] == profile["impl"] == SOFTWARE_IMPL
    assert defaults["spec"] == profile["spec"] == SOFTWARE_SPEC


def test_git_capture_can_be_disabled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[str] = []

    def fake(path: Any, **_: Any) -> Any:
        calls.append(str(path))
        raise AssertionError("git_state must not run")

    monkeypatch.setattr(software_module, "git_state", fake)

    data = software(_context(tmp_path), git=False)["software"]

    assert calls == []
    assert data["git"] is None
    assert data["git_note"] == "disabled by hook parameter 'git'"


def test_unknown_parameter_raises(tmp_path: Path) -> None:
    with pytest.raises(TypeError):
        software(_context(tmp_path), bogus=1)  # type: ignore[call-arg]


def test_git_state_is_called_once_per_distinct_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[str] = []
    real = software_module.git_state

    def counting(path: Any, **kwargs: Any) -> Any:
        calls.append(str(path))
        return real(path, **kwargs)

    monkeypatch.setattr(software_module, "git_state", counting)

    data = software(_context(tmp_path))["software"]

    assert len(calls) == len(set(calls))
    assert str(tmp_path) in calls
    assert list(data["git"]["editable"]) == sorted(data["git"]["editable"])


def _packaged(*modules: str) -> dict[str, Any]:
    return {
        "registry_layers": [
            {"name": m, "kind": "profile", "path": f"package:{m}.profiles/x.yaml"}
            for m in modules
        ]
    }


def test_layers_do_not_scan_every_distribution_record(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def forbidden() -> Any:
        raise AssertionError("packages_distributions must not run")

    monkeypatch.setattr(software_module.metadata, "packages_distributions", forbidden)

    data = software(_context(tmp_path, _packaged("pytest", "fasthep_toolbench")))

    assert data["software"]["layers"] == ["fasthep-toolbench", "pytest"]
    assert "layer_notes" not in data["software"]


def test_unmatched_layer_package_is_recorded_with_a_note(tmp_path: Path) -> None:
    data = software(_context(tmp_path, _packaged("no_such_pkg_xyz")))["software"]

    assert data["layers"] == ["no_such_pkg_xyz"]
    assert set(data["layer_notes"]) == {"no_such_pkg_xyz"}


def _counting_scan(monkeypatch: pytest.MonkeyPatch) -> list[bool]:
    calls: list[bool] = []
    real = software_module._scan_environment

    def counting(root: Any, *, git: bool) -> dict[str, Any]:
        calls.append(git)
        return real(root, git=git)

    monkeypatch.setattr(software_module, "_scan_environment", counting)
    return calls


def test_scan_is_memoised_per_root_and_git_flag(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = _counting_scan(monkeypatch)

    first = software(_context(tmp_path), git=False)
    second = software(_context(tmp_path), git=False)
    software(_context(tmp_path), git=True)

    assert calls == [False, True]
    assert first == second


def test_cache_is_cleared_and_expires(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = _counting_scan(monkeypatch)
    clock = [100.0]
    monkeypatch.setattr(software_module.time, "monotonic", lambda: clock[0])

    software(_context(tmp_path), git=False)
    software_module.clear_software_cache()
    software(_context(tmp_path), git=False)
    clock[0] += software_module.SOFTWARE_CACHE_TTL_SECONDS + 1
    software(_context(tmp_path), git=False)

    assert calls == [False, False, False]


def test_cached_result_is_not_shared_mutable_state(tmp_path: Path) -> None:
    first = software(_context(tmp_path), git=False)["software"]
    first["distributions"].clear()

    second = software(_context(tmp_path), git=False)["software"]

    assert second["distributions"]
