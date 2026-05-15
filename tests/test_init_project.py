from __future__ import annotations

from pathlib import Path

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
