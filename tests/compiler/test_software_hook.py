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
