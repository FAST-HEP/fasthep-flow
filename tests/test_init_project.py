from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest
import yaml

from hepflow.api import init_project


def test_init_project_scaffolds_flow_profiles_under_fasthep(tmp_path: Path) -> None:
    result = init_project(target_dir=tmp_path)

    profile_dir = tmp_path / ".fasthep" / "profiles" / "hepflow"
    assert result.profile_dir == profile_dir
    assert result.created_profile_dir
    assert (profile_dir / "registry.yaml").exists()
    assert (profile_dir / "dask_local.yaml").exists()
    assert (profile_dir / "basic.yaml").exists()
    assert (profile_dir / "hep.yaml").exists()
    assert (profile_dir / "hep_debug.yaml").exists()
    assert not (tmp_path / ".hepflow").exists()


def test_init_project_skips_existing_flow_profiles(tmp_path: Path) -> None:
    first = init_project(target_dir=tmp_path)
    second = init_project(target_dir=tmp_path)

    assert first.written
    assert not second.created_profile_dir
    assert not second.written
    assert {
        path.relative_to(second.profile_dir).as_posix()
        for path in second.skipped_existing
    } == {
        "basic.yaml",
        "dask_local.yaml",
        "hep.yaml",
        "hep_debug.yaml",
        "registry.yaml",
    }


def test_init_project_includes_package_profile(tmp_path: Path) -> None:
    result = init_project(
        target_dir=tmp_path,
        include=["tests.toy_components:registry"],
    )

    destination = (
        tmp_path
        / ".fasthep"
        / "profiles"
        / "tests.toy_components"
        / "registry.yaml"
    )
    assert destination in result.written
    assert destination.exists()
    assert yaml.safe_load(destination.read_text(encoding="utf-8"))["registry"]


def test_init_project_includes_local_profile_path(tmp_path: Path) -> None:
    source = tmp_path / "profiles" / "custom.yaml"
    source.parent.mkdir()
    source.write_text("registry:\n  sources: {}\n", encoding="utf-8")

    result = init_project(target_dir=tmp_path, include=["./profiles/custom.yaml"])

    destination = tmp_path / ".fasthep" / "profiles" / "local" / "custom.yaml"
    assert destination in result.written
    assert destination.read_text(encoding="utf-8") == source.read_text(encoding="utf-8")


def test_init_project_hep_profile_copies_importable_package_profiles(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    package_name = "fake_" + "fasthep_" + "carpenter"
    _write_profile_package(
        tmp_path,
        package_name,
        {
            "registry.yaml": "name: registry\n",
            "custom-profile.yaml": "name: custom\n",
        },
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setattr("hepflow.profiles.init.HEP_PROFILE_PACKAGES", [package_name])
    importlib.invalidate_caches()

    result = init_project(target_dir=tmp_path, profiles=["HEP"])

    profile_dir = tmp_path / ".fasthep" / "profiles" / package_name
    registry = profile_dir / "registry.yaml"
    custom = profile_dir / "custom-profile.yaml"
    assert registry in result.written
    assert custom in result.written
    assert registry.read_text(encoding="utf-8") == "name: registry\n"
    assert custom.read_text(encoding="utf-8") == "name: custom\n"


def test_init_project_hep_profile_warns_for_missing_package(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    missing_package = "missing_" + "fasthep_" + "render"
    monkeypatch.setattr("hepflow.profiles.init.HEP_PROFILE_PACKAGES", [missing_package])

    result = init_project(target_dir=tmp_path, profiles=["HEP"])

    assert result.warnings == [f"profile package not found: {missing_package}"]
    assert (tmp_path / ".fasthep" / "profiles" / "hepflow" / "registry.yaml").exists()


def test_init_project_hep_profile_is_case_insensitive(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    package_name = "fake_" + "fasthep_" + "curator"
    _write_profile_package(
        tmp_path,
        package_name,
        {"registry.yaml": "name: registry\n"},
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setattr("hepflow.profiles.init.HEP_PROFILE_PACKAGES", [package_name])
    importlib.invalidate_caches()

    result = init_project(target_dir=tmp_path, profiles=["hep"])

    destination = tmp_path / ".fasthep" / "profiles" / package_name / "registry.yaml"
    assert destination in result.written
    assert destination.exists()


def test_init_project_reports_missing_package_profile(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="does_not_exist:registry"):
        init_project(target_dir=tmp_path, include=["does_not_exist:registry"])


def test_init_project_reports_missing_profile_resource(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match=r"tests\.toy_components:missing"):
        init_project(target_dir=tmp_path, include=["tests.toy_components:missing"])


def test_init_project_reports_missing_local_profile(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match=r"\./profiles/missing\.yaml"):
        init_project(target_dir=tmp_path, include=["./profiles/missing.yaml"])


def _write_profile_package(
    tmp_path: Path,
    package_name: str,
    profiles: dict[str, str],
) -> None:
    package_dir = tmp_path / package_name
    profiles_dir = package_dir / "profiles"
    profiles_dir.mkdir(parents=True)
    (package_dir / "__init__.py").write_text("", encoding="utf-8")
    for filename, content in profiles.items():
        (profiles_dir / filename).write_text(content, encoding="utf-8")
    sys.modules.pop(package_name, None)
