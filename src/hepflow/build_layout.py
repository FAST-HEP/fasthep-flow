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


def render_specs_dir(root: str | Path) -> Path:
    return render_dir(root) / "specs"


def reports_dir(root: str | Path) -> Path:
    return build_root(root) / "reports"


def debug_dir(root: str | Path) -> Path:
    return build_root(root) / "debug"


def artifacts_dir(root: str | Path) -> Path:
    return build_root(root) / "artifacts"


def artifact_family_dir(root: str | Path, family: str) -> Path:
    return artifacts_dir(root) / family


def cutflows_dir(root: str | Path) -> Path:
    return artifact_family_dir(root, "cutflows")


def tables_dir(root: str | Path) -> Path:
    return artifact_family_dir(root, "tables")


def plan_path(root: str | Path) -> Path:
    return compile_dir(root) / "plan.yaml"


def normalized_path(root: str | Path) -> Path:
    return compile_dir(root) / "normalized.yaml"


def run_summary_path(root: str | Path) -> Path:
    return build_root(root) / "run_summary.yaml"


def ensure_build_layout(root: str | Path) -> None:
    for path in [
        artifact_family_dir(root, "plots"),
        artifact_family_dir(root, "histograms"),
        artifact_family_dir(root, "cutflows"),
        artifact_family_dir(root, "tables"),
        artifact_family_dir(root, "files"),
        compile_dir(root),
        graph_dir(root),
        render_specs_dir(root),
        reports_dir(root) / "schema",
        reports_dir(root) / "diagnostics",
        reports_dir(root) / "provenance",
        debug_dir(root) / "dask",
        debug_dir(root) / "performance",
        debug_dir(root) / "logs",
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
