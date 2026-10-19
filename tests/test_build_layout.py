from __future__ import annotations

from pathlib import Path

import pytest

from hepflow.build_layout import (
    BuildPaths,
    ensure_build_layout,
    output_files_root_from_context,
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


def test_build_paths_files_root_moves_only_the_files_family() -> None:
    paths = BuildPaths(root=Path("build"), files_root=Path("/job/dir"))

    assert paths.artifact("files", "ds/0_0.root") == Path("/job/dir/ds/0_0.root")
    assert paths.artifact_dir("files") == Path("/job/dir")
    assert paths.artifact("cutflows", "x.csv") == Path("build/artifacts/cutflows/x.csv")
    assert paths.provenance_dir() == Path("build/artifacts/provenance")


def test_build_paths_without_files_root_is_unchanged() -> None:
    paths = BuildPaths(root=Path("build"))

    assert paths.files_root is None
    assert paths.artifact("files", "ds/0_0.root") == Path(
        "build/artifacts/files/ds/0_0.root"
    )


def test_build_paths_from_ctx_reads_output_files_root() -> None:
    paths = BuildPaths.from_ctx({"outdir": "build", "output_files_root": "."})

    assert paths.root == Path("build")
    assert paths.artifact("files", "ds/0_0.root") == Path("ds/0_0.root")


def test_build_paths_from_ctx_adds_files_root_to_existing_build_paths() -> None:
    existing = BuildPaths(root=Path("build"), variation="nominal")

    paths = BuildPaths.from_ctx(
        {"build_paths": existing, "output_files_root": "/job/dir"}
    )

    assert paths.variation == "nominal"
    assert paths.artifact("files", "a.root") == Path("/job/dir/a.root")
    assert existing.files_root is None


@pytest.mark.parametrize("value", [None, "", "   ", 3])
def test_output_files_root_from_context_ignores_blank_values(value: object) -> None:
    assert output_files_root_from_context({"output_files_root": value}) is None
    assert output_files_root_from_context(None) is None


def test_ensure_build_layout_ignores_files_root_context(tmp_path: Path) -> None:
    ensure_build_layout(tmp_path)

    assert (tmp_path / "artifacts" / "files").is_dir()
