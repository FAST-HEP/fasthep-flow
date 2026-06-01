from __future__ import annotations

from pathlib import Path

from hepflow.build_layout import BuildPaths, ensure_build_layout


def test_build_paths_artifact_without_variation() -> None:
    paths = BuildPaths(root=Path("build"))

    assert paths.artifact("histograms", "MuonPt.pkl") == Path(
        "build/artifacts/histograms/MuonPt.pkl"
    )


def test_build_paths_artifact_with_variation() -> None:
    paths = BuildPaths(root=Path("build"), variation="nominal")

    assert paths.artifact("histograms", "MuonPt.pkl") == Path(
        "build/artifacts/nominal/histograms/MuonPt.pkl"
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


def test_ensure_build_layout_creates_variation_parents(tmp_path: Path) -> None:
    ensure_build_layout(tmp_path / "build", variation="nominal")

    assert (tmp_path / "build" / "artifacts" / "nominal" / "histograms").is_dir()
    assert (tmp_path / "build" / "reports" / "nominal" / "schema").is_dir()
    assert (tmp_path / "build" / "render" / "specs" / "nominal").is_dir()
    assert (tmp_path / "build" / "debug" / "nominal" / "logs").is_dir()
