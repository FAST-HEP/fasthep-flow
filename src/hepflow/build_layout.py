from __future__ import annotations

from pathlib import Path


def build_root(path: str | Path) -> Path:
    candidate = Path(path)
    if candidate.name in {"compile", "graph", "render", "reports", "debug"}:
        return candidate.parent
    return candidate


def compile_dir(root: str | Path) -> Path:
    return build_root(root) / "compile"


def graph_dir(root: str | Path) -> Path:
    return build_root(root) / "graph"


def render_dir(root: str | Path) -> Path:
    return build_root(root) / "render"


def render_specs_dir(root: str | Path, *, variation: str | None = None) -> Path:
    path = render_dir(root) / "specs"
    if variation:
        return path / variation
    return path


def reports_dir(root: str | Path, *, variation: str | None = None) -> Path:
    path = build_root(root) / "reports"
    if variation:
        return path / variation
    return path


def debug_dir(root: str | Path, *, variation: str | None = None) -> Path:
    path = build_root(root) / "debug"
    if variation:
        return path / variation
    return path


def artifacts_dir(root: str | Path) -> Path:
    return build_root(root) / "artifacts"


def artifact_family_dir(
    root: str | Path,
    family: str,
    *,
    variation: str | None = None,
) -> Path:
    path = artifacts_dir(root)
    if variation:
        path = path / variation
    return path / family


def cutflows_dir(root: str | Path, *, variation: str | None = None) -> Path:
    return artifact_family_dir(root, "cutflows", variation=variation)


def tables_dir(root: str | Path, *, variation: str | None = None) -> Path:
    return artifact_family_dir(root, "tables", variation=variation)


def plan_path(root: str | Path) -> Path:
    return compile_dir(root) / "plan.yaml"


def normalized_path(root: str | Path) -> Path:
    return compile_dir(root) / "normalized.yaml"


def run_summary_path(root: str | Path) -> Path:
    return build_root(root) / "run_summary.yaml"


def ensure_build_layout(root: str | Path, *, variation: str | None = None) -> None:
    for path in [
        artifact_family_dir(root, "plots", variation=variation),
        artifact_family_dir(root, "histograms", variation=variation),
        artifact_family_dir(root, "cutflows", variation=variation),
        artifact_family_dir(root, "tables", variation=variation),
        artifact_family_dir(root, "files", variation=variation),
        compile_dir(root),
        graph_dir(root),
        render_specs_dir(root, variation=variation),
        reports_dir(root, variation=variation) / "schema",
        reports_dir(root, variation=variation) / "diagnostics",
        reports_dir(root, variation=variation) / "provenance",
        debug_dir(root, variation=variation) / "dask",
        debug_dir(root, variation=variation) / "performance",
        debug_dir(root, variation=variation) / "logs",
    ]:
        path.mkdir(parents=True, exist_ok=True)


def resolve_plan_path(path: str | Path) -> Path:
    candidate = Path(path)
    if candidate.exists():
        return candidate
    if candidate.name == "plan.yaml":
        nested = candidate.parent / "compile" / "plan.yaml"
        if nested.exists():
            return nested
    return candidate


def resolve_normalized_path(path: str | Path) -> Path:
    candidate = Path(path)
    if candidate.exists():
        return candidate
    if candidate.name == "normalized.yaml":
        nested = candidate.parent / "compile" / "normalized.yaml"
        if nested.exists():
            return nested
    return candidate
