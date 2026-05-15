from __future__ import annotations

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
    } == {"registry.yaml", "dask_local.yaml"}


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


def test_init_project_reports_missing_package_profile(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="does_not_exist:registry"):
        init_project(target_dir=tmp_path, include=["does_not_exist:registry"])


def test_init_project_reports_missing_profile_resource(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match=r"tests\.toy_components:missing"):
        init_project(target_dir=tmp_path, include=["tests.toy_components:missing"])


def test_init_project_reports_missing_local_profile(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match=r"\./profiles/missing\.yaml"):
        init_project(target_dir=tmp_path, include=["./profiles/missing.yaml"])
