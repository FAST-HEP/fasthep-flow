"""One resolver for registry, execution and hooks; each profile is read once per compile."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from hepflow.compiler import profiles as profile_module
from hepflow.compiler.context import CompileContext
from hepflow.compiler.workflow_config import resolve_workflow_config


def _project(tmp_path: Path) -> Path:
    profiles = tmp_path / ".hepflow" / "profiles"
    profiles.mkdir(parents=True)
    (profiles / "base.yaml").write_text(
        yaml.safe_dump(
            {
                "execution": {"config": {"a": 1}},
                "execution_hooks": [{"kind": "x", "events": ["run_end"]}],
            }
        )
    )
    (profiles / "outer.yaml").write_text(
        yaml.safe_dump(
            {"includes": ["base", {"profile": "missing", "optional": True}]}
        )
    )
    return tmp_path


def _count_reads(monkeypatch: pytest.MonkeyPatch) -> dict[str, int]:
    reads: dict[str, int] = {}
    real = profile_module._load_profile_with_provenance

    def counting(name: str, **kwargs: Any) -> Any:
        reads[name] = reads.get(name, 0) + 1
        return real(name, **kwargs)

    monkeypatch.setattr(profile_module, "_load_profile_with_provenance", counting)
    return reads


def test_every_profile_is_read_once_per_compile(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _project(tmp_path)
    reads = _count_reads(monkeypatch)
    context = CompileContext.from_workflow_path(root / "wf.yaml", root)

    config = resolve_workflow_config({"use": {"profiles": ["outer"]}}, context=context)

    assert reads == {"outer": 1, "base": 1, "missing": 1}
    assert config.execution["execution"]["config"]["a"] == 1
    assert config.execution_hooks["execution_hooks"][0]["kind"] == "x"
    skipped = config.registry.provenance["optional_profiles_skipped"]
    assert [item["profile"] for item in skipped] == ["missing"]
    for key, layers in (
        ("execution_layers", config.execution["provenance"]),
        ("execution_hook_layers", config.execution_hooks["provenance"]),
    ):
        assert [layer["name"] for layer in layers[key]] == ["builtin", "base", "workflow"]


def test_use_must_be_a_mapping(tmp_path: Path) -> None:
    context = CompileContext.from_workflow_path(tmp_path / "wf.yaml", tmp_path)

    with pytest.raises(ValueError, match="use must be a mapping"):
        resolve_workflow_config({"use": ["basic"]}, context=context)


def test_a_deprecated_entry_directory_profile_warns_once(tmp_path: Path) -> None:
    project = tmp_path / "proj"
    project.mkdir()
    entry = tmp_path / "entry"
    (entry / ".hepflow" / "profiles").mkdir(parents=True)
    (entry / ".hepflow" / "profiles" / "p.yaml").write_text(
        yaml.safe_dump({"execution": {"config": {"b": 2}}})
    )
    context = CompileContext.from_workflow_path(entry / "wf.yaml", project)

    with pytest.warns(FutureWarning) as caught:
        resolve_workflow_config({"use": {"profiles": ["p"]}}, context=context)

    assert len(caught) == 1
