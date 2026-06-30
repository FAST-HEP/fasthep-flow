from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from hepflow.utils import write_yaml


@dataclass(frozen=True, slots=True)
class BuildPaths:
    root: Path
    variation: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "root", build_root(self.root))
        if isinstance(self.variation, str):
            variation = self.variation.strip() or None
            object.__setattr__(self, "variation", variation)

    @classmethod
    def from_ctx(cls, ctx: Mapping[str, Any] | None) -> BuildPaths:
        context = dict(ctx or {})
        existing = context.get("build_paths")
        if isinstance(existing, BuildPaths):
            return existing
        return cls(
            root=Path(str(context.get("outdir") or ".")),
            variation=output_variation_from_context(context),
        )

    @classmethod
    def from_plan(cls, plan: Any, *, outdir: str | Path) -> BuildPaths:
        return cls(
            root=Path(outdir),
            variation=output_variation_from_context(
                getattr(plan, "context", {}) or {}
            ),
        )

    def artifact_dir(self, kind: str) -> Path:
        return self.artifacts_root() / kind

    def artifacts_root(self) -> Path:
        path = self.root / "artifacts"
        if self.variation:
            path = path / self.variation
        return path

    def artifact(self, kind: str, filename: str | Path) -> Path:
        return self.artifact_dir(kind) / filename

    def provenance_dir(self) -> Path:
        return self.artifact_dir("provenance")

    def provenance_records_dir(self) -> Path:
        return self.provenance_dir() / "records"

    def provenance_manifest(self) -> Path:
        return self.provenance_dir() / "manifest.json"

    def provenance_execution(self) -> Path:
        return self.provenance_dir() / "execution.json"

    def report_dir(self, kind: str | None = None) -> Path:
        path = self.root / "reports"
        if self.variation:
            path = path / self.variation
        if kind:
            path = path / kind
        return path

    def report(self, kind: str, filename: str | Path) -> Path:
        return self.report_dir(kind) / filename

    def render_dir(self) -> Path:
        return self.root / "render"

    def render_specs_dir(self) -> Path:
        path = self.render_dir() / "specs"
        if self.variation:
            path = path / self.variation
        return path

    def render_spec(self, filename: str | Path) -> Path:
        return self.render_specs_dir() / filename

    def debug_dir(self, kind: str | None = None) -> Path:
        path = self.root / "debug"
        if self.variation:
            path = path / self.variation
        if kind:
            path = path / kind
        return path

    def debug(self, kind: str, filename: str | Path) -> Path:
        return self.debug_dir(kind) / filename

    def compile_dir(self) -> Path:
        return self.root / "compile"

    def compile_file(self, filename: str | Path) -> Path:
        return self.compile_dir() / filename

    def graph_dir(self) -> Path:
        return self.root / "graph"

    def graph_file(self, filename: str | Path) -> Path:
        return self.graph_dir() / filename

    def relative_to_root(self, path: str | Path) -> Path:
        return Path(path).relative_to(self.root)

    def run_summary(self) -> Path:
        if self.variation:
            return self.report_dir() / "run_summary.yaml"
        return self.root / "run_summary.yaml"


def output_variation_from_context(context: Mapping[str, Any] | None) -> str | None:
    if context is None:
        return None
    existing = context.get("build_paths")
    if isinstance(existing, BuildPaths):
        return existing.variation
    variation = context.get("variation")
    if not isinstance(variation, Mapping):
        return None
    name = variation.get("name")
    if not isinstance(name, str) or not name.strip():
        return None
    return name.strip()


def build_root(path: str | Path) -> Path:
    candidate = Path(path)
    if candidate.name in {"compile", "graph", "render", "reports", "debug"}:
        return candidate.parent
    return candidate


def compile_dir(root: str | Path) -> Path:
    return BuildPaths(root=Path(root)).compile_dir()


def graph_dir(root: str | Path) -> Path:
    return BuildPaths(root=Path(root)).graph_dir()


def render_dir(root: str | Path) -> Path:
    return BuildPaths(root=Path(root)).render_dir()


def render_specs_dir(root: str | Path, *, variation: str | None = None) -> Path:
    return BuildPaths(root=Path(root), variation=variation).render_specs_dir()


def reports_dir(root: str | Path, *, variation: str | None = None) -> Path:
    return BuildPaths(root=Path(root), variation=variation).report_dir()


def debug_dir(root: str | Path, *, variation: str | None = None) -> Path:
    return BuildPaths(root=Path(root), variation=variation).debug_dir()


def artifacts_dir(root: str | Path) -> Path:
    return build_root(root) / "artifacts"


def artifact_family_dir(
    root: str | Path,
    family: str,
    *,
    variation: str | None = None,
) -> Path:
    return BuildPaths(root=Path(root), variation=variation).artifact_dir(family)


def cutflows_dir(root: str | Path, *, variation: str | None = None) -> Path:
    return artifact_family_dir(root, "cutflows", variation=variation)


def tables_dir(root: str | Path, *, variation: str | None = None) -> Path:
    return artifact_family_dir(root, "tables", variation=variation)


def plan_path(root: str | Path) -> Path:
    return BuildPaths(root=Path(root)).compile_file("plan.yaml")


def normalized_path(root: str | Path) -> Path:
    return BuildPaths(root=Path(root)).compile_file("normalized.yaml")


def run_summary_path(root: str | Path) -> Path:
    return BuildPaths(root=Path(root)).run_summary()


def write_run_summary(
    root: str | Path,
    summary: dict[str, Any],
    *,
    variation_name: str | None = None,
) -> None:
    paths = BuildPaths(root=Path(root), variation=variation_name)
    paths.run_summary().parent.mkdir(parents=True, exist_ok=True)
    write_yaml(deepcopy(summary), str(paths.run_summary()))


def ensure_build_layout(root: str | Path, *, variation: str | None = None) -> None:
    paths = BuildPaths(root=Path(root), variation=variation)
    for path in [
        paths.artifact_dir("plots"),
        paths.artifact_dir("histograms"),
        paths.artifact_dir("cutflows"),
        paths.artifact_dir("tables"),
        paths.artifact_dir("files"),
        paths.provenance_records_dir(),
        paths.compile_dir(),
        paths.graph_dir(),
        paths.render_specs_dir(),
        paths.report_dir("schema"),
        paths.report_dir("diagnostics"),
        paths.report_dir("provenance"),
        paths.debug_dir("dask"),
        paths.debug_dir("performance"),
        paths.debug_dir("logs"),
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
