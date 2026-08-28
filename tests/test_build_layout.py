from __future__ import annotations

from pathlib import Path

import pytest

from hepflow.build_layout import (
    BuildPaths,
    ensure_build_layout,
    validate_build_relative_path,
)


def test_build_paths_artifact_without_variation() -> None:
    paths = BuildPaths(root=Path("build"))

    assert paths.artifacts_root() == Path("build/artifacts")
    assert paths.artifact("histograms", "MuonPt.pkl") == Path(
        "build/artifacts/histograms/MuonPt.pkl"
    )


def test_build_paths_artifact_with_variation() -> None:
    paths = BuildPaths(root=Path("build"), variation="nominal")

    assert paths.artifacts_root() == Path("build/artifacts/nominal")
    assert paths.artifact("histograms", "MuonPt.pkl") == Path(
        "build/artifacts/nominal/histograms/MuonPt.pkl"
    )


def test_build_paths_provenance_execution() -> None:
    paths = BuildPaths(root=Path("build"))
    varied = BuildPaths(root=Path("build"), variation="nominal")

    assert paths.provenance_execution() == Path(
        "build/artifacts/provenance/execution.json"
    )
    assert varied.provenance_execution() == Path(
        "build/artifacts/nominal/provenance/execution.json"
    )


def test_build_paths_report_without_variation() -> None:
    paths = BuildPaths(root=Path("build"))

    assert paths.report("schema", "source.json") == Path(
        "build/reports/schema/source.json"
    )


def test_build_paths_report_with_variation() -> None:
    paths = BuildPaths(root=Path("build"), variation="trigger_eff_up")

    assert paths.report("schema", "source.json") == Path(
        "build/reports/trigger_eff_up/schema/source.json"
    )


def test_build_paths_render_spec_without_variation() -> None:
    paths = BuildPaths(root=Path("build"))

    assert paths.render_spec("render_MuonPt_0.yaml") == Path(
        "build/render/specs/render_MuonPt_0.yaml"
    )


def test_build_paths_render_spec_with_variation() -> None:
    paths = BuildPaths(root=Path("build"), variation="trigger_eff_up")

    assert paths.render_spec("render_MuonPt_0.yaml") == Path(
        "build/render/specs/trigger_eff_up/render_MuonPt_0.yaml"
    )


def test_build_paths_debug_path() -> None:
    paths = BuildPaths(root=Path("build"), variation="nominal")

    assert paths.debug("logs", "run.log") == Path(
        "build/debug/nominal/logs/run.log"
    )


def test_build_paths_execution_path() -> None:
    paths = BuildPaths(root=Path("build"))

    assert paths.execution_dir() == Path("build/execution")
    assert paths.worker_environments_dir() == Path(
        "build/execution/worker-environments"
    )
    assert paths.execution_backend_dir("dask", "htcondor", "submit") == Path(
        "build/execution/dask/htcondor/submit"
    )
    assert BuildPaths(root=Path("build/execution")).root == Path("build")


def test_build_paths_run_summary_without_variation() -> None:
    paths = BuildPaths(root=Path("build"))

    assert paths.run_summary() == Path("build/run_summary.yaml")


def test_build_paths_run_summary_with_variation() -> None:
    paths = BuildPaths(root=Path("build"), variation="trigger_eff_down")

    assert paths.run_summary() == Path(
        "build/reports/trigger_eff_down/run_summary.yaml"
    )


def test_ensure_build_layout_creates_variation_parents(tmp_path: Path) -> None:
    ensure_build_layout(tmp_path / "build", variation="nominal")

    assert (tmp_path / "build" / "artifacts" / "nominal" / "histograms").is_dir()
    assert (tmp_path / "build" / "reports" / "nominal" / "schema").is_dir()
    assert (tmp_path / "build" / "render" / "specs" / "nominal").is_dir()


def test_ensure_build_layout_creates_backend_declared_directories(
    tmp_path: Path,
) -> None:
    ensure_build_layout(
        tmp_path / "build",
        backend_directories=[
            "execution/dask/htcondor/submit",
            "execution/dask/htcondor/logs",
            "debug/dask",
        ],
    )

    assert (tmp_path / "build" / "execution" / "dask" / "htcondor" / "submit").is_dir()
    assert (tmp_path / "build" / "execution" / "dask" / "htcondor" / "logs").is_dir()
    assert (tmp_path / "build" / "debug" / "dask").is_dir()


@pytest.mark.parametrize("path", ["/tmp/outside", "../outside", "execution/../x"])
def test_backend_declared_build_directories_reject_unsafe_paths(path: str) -> None:
    with pytest.raises(ValueError, match="build directory"):
        validate_build_relative_path(path)
