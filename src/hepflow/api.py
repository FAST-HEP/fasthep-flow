from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Any

from hepflow.backends.loaders import load_backend
from hepflow.backends.model import BackendResult
from hepflow.build_layout import (
    BuildPaths,
    compile_dir,
    ensure_build_layout,
    graph_dir,
    normalized_path,
    plan_path,
    resolve_normalized_path,
    resolve_plan_path,
    write_run_summary,
)
from hepflow.compiler.artifacts import write_compile_artifacts
from hepflow.compiler.execution import (
    resolve_author_execution,
    resolve_author_execution_hooks,
)
from hepflow.compiler.graph_artifacts import write_graph_artifacts
from hepflow.compiler.includes import load_author_with_includes
from hepflow.compiler.normalize import normalize_author
from hepflow.compiler.plan import build_plan_from_normalized
from hepflow.compiler.plan_diff import (
    diff_plans,
    format_plan_diff,
    load_plan_yaml,
)
from hepflow.compiler.registry_resolution import resolve_author_registry
from hepflow.compiler.systematics import make_systematic_plan_files
from hepflow.model.plan import (
    ExecutionNode,
    ExecutionPartition,
    ExecutionPlan,
    PartitionSpec,
    PlanInputRef,
)
from hepflow.profiles.init import InitResult
from hepflow.profiles.init import init_project as _init_project
from hepflow.runtime.config import (
    _runtime_execution_with_overrides,
    default_run_outdir_for_plan,
)
from hepflow.utils import read_yaml, write_yaml

__all__ = [
    "InitResult",
    "compile_author_file",
    "diff_plan_files",
    "init_project",
    "load_author_yaml",
    "load_plan_file",
    "make_plan_file",
    "normalise_author_file",
    "normalize_author_file",
    "run_author_file",
    "run_plan_file",
]


def load_author_yaml(path: str | Path) -> dict[str, Any]:
    return load_author_with_includes(str(path)).doc


def init_project(
    *,
    target_dir: str | Path,
    force: bool = False,
    include: Iterable[str] | None = None,
    profiles: Iterable[str] | None = None,
) -> InitResult:
    """Create project-local profile templates."""
    return _init_project(
        target_dir=target_dir,
        force=force,
        include=include,
        profiles=profiles,
    )


def normalise_author_file(
    author_path: str | Path,
    *,
    outdir: str | Path,
) -> dict[str, Any]:
    """Normalise an author YAML file and write ``compile/normalized.yaml``."""
    author_file = Path(author_path)
    out_path = Path(outdir)
    compile_dir(out_path).mkdir(parents=True, exist_ok=True)

    author = load_author_yaml(str(author_file))
    normalized = normalize_author(author)

    registry_result = resolve_author_registry(author, author_path=author_file)
    execution_result = resolve_author_execution(author, author_path=author_file)
    hooks_result = resolve_author_execution_hooks(author, author_path=author_file)

    normalized["registry"] = registry_result.registry
    normalized["execution"] = execution_result["execution"]
    normalized["execution_hooks"] = hooks_result["execution_hooks"]
    normalized.setdefault("provenance", {}).update(registry_result.provenance)
    normalized.setdefault("provenance", {}).update(execution_result["provenance"])
    normalized.setdefault("provenance", {}).update(hooks_result["provenance"])

    write_yaml(normalized, str(normalized_path(out_path)))
    return normalized


normalize_author_file = normalise_author_file


def make_plan_file(
    normalized_path: str | Path,
    *,
    outdir: str | Path,
    chunk_size: int | None = None,
) -> ExecutionPlan:
    """Lower a normalized YAML file and write plan/graph artifacts."""
    normalized_file = resolve_normalized_path(normalized_path)
    out_path = Path(outdir)
    normalized = read_yaml(str(normalized_file)) or {}
    if "systematics" in normalized:
        compile_dir(out_path).mkdir(parents=True, exist_ok=True)
        return make_systematic_plan_files(
            normalized,
            outdir=out_path,
            chunk_size=chunk_size,
        )

    ensure_build_layout(out_path)
    graph, plan = build_plan_from_normalized(normalized, chunk_size=chunk_size)

    write_compile_artifacts(
        plan=plan,
        graph=graph,
        outdir=out_path,
        normalized=normalized,
    )
    write_graph_artifacts(graph, graph_dir(out_path))
    write_yaml(plan.to_dict(), str(plan_path(out_path)))
    return plan


def compile_author_file(
    author_path: str | Path,
    *,
    outdir: str | Path,
    chunk_size: int | None = None,
) -> ExecutionPlan:
    """Normalise an author YAML file, lower it, and write compile artifacts."""
    out_path = Path(outdir)
    normalise_author_file(author_path, outdir=out_path)
    return make_plan_file(
        normalized_path(out_path),
        outdir=out_path,
        chunk_size=chunk_size,
    )


def load_plan_file(plan_path: str | Path) -> ExecutionPlan:
    """Load an ``ExecutionPlan`` from a compiled ``plan.yaml`` file."""
    doc = read_yaml(str(plan_path)) or {}
    plan = ExecutionPlan(
        context=dict(doc.get("context") or {}),
        registry=dict(doc.get("registry") or {}),
        provenance=dict(doc.get("provenance") or {}),
        execution=dict(doc.get("execution") or {}),
        execution_hooks=list(doc.get("execution_hooks") or []),
        data_flow=dict(doc.get("data_flow") or {}),
    )
    plan.partitions = [
        ExecutionPartition(
            id=str(item["id"]),
            dataset=str(item["dataset"]),
            file=str(item["file"]),
            source=str(item["source"]),
            part=str(item["part"]),
            start=item.get("start"),
            stop=item.get("stop"),
        )
        for item in list(doc.get("partitions") or [])
    ]

    for item in list(doc.get("nodes") or []):
        partitioning = dict(item.get("partitioning") or {})
        node = ExecutionNode(
            id=str(item["id"]),
            graph_node_id=str(item.get("graph_node_id") or item["id"]),
            role=item["role"],
            impl=str(item["impl"]),
            inputs=[
                PlanInputRef(
                    node_id=str(ref["node_id"]),
                    output_name=str(ref["output_name"]),
                    input_name=str(ref["input_name"]),
                )
                for ref in list(item.get("inputs") or [])
            ],
            params=dict(item.get("params") or {}),
            outputs=dict(item.get("outputs") or {}),
            input_scope=item.get("input_scope", "global"),
            output_scope=item.get("output_scope", "global"),
            partitioning=PartitionSpec(
                mode=partitioning.get("mode", "none"),
                chunk_size=partitioning.get("chunk_size"),
            ),
            materialize=item.get("materialize", "never"),
            meta=dict(item.get("meta") or {}),
        )
        plan.add_node(node)

    return plan


def run_plan_file(
    plan_path: str | Path,
    *,
    outdir: str | Path | None = None,
    backend: str | None = None,
    strategy: str | None = None,
    scheduler: str | None = None,
    workers: int | None = None,
) -> BackendResult:
    """Run a compiled plan file and write ``run_summary.yaml``."""
    plan_file = resolve_plan_path(plan_path)
    plan = load_plan_file(plan_file)
    out_path = (
        Path(outdir) if outdir is not None else default_run_outdir_for_plan(plan_file)
    )
    build_paths = BuildPaths.from_plan(plan, outdir=out_path)
    ensure_build_layout(build_paths.root, variation=build_paths.variation)
    runtime_execution = _runtime_execution_with_overrides(
        plan.execution,
        backend=backend,
        strategy=strategy,
        scheduler=scheduler,
        workers=workers,
    )
    plan.execution = runtime_execution

    backend_impl = load_backend(plan)
    run_ctx: dict[str, Any] = {
        "outdir": str(build_paths.root.resolve()),
        "build_paths": build_paths,
    }
    result = backend_impl.run(plan, ctx=run_ctx)

    summary = {
        "backend": result.backend,
        "strategy": result.strategy,
        "success": result.success,
        "execution": runtime_execution,
        **result.summary,
    }
    summary["summary_path"] = str(build_paths.run_summary())
    summary["artifacts_path"] = str(build_paths.artifacts_root())
    if build_paths.variation is not None:
        summary["variation"] = plan.context.get("variation") or {
            "name": build_paths.variation
        }
    write_run_summary(
        build_paths.root,
        summary,
        variation_name=build_paths.variation,
    )
    result.summary = summary
    return result


def run_author_file(
    author_path: str | Path,
    *,
    outdir: str | Path,
    backend: str | None = None,
    strategy: str | None = None,
    scheduler: str | None = None,
    workers: int | None = None,
    chunk_size: int | None = None,
) -> BackendResult:
    """Compile and run an author YAML file in one call."""
    out_path = Path(outdir)
    compile_author_file(author_path, outdir=out_path, chunk_size=chunk_size)
    normalized = read_yaml(str(normalized_path(out_path))) or {}
    if "systematics" in normalized:
        nominal_plan = out_path / "compile" / "nominal" / "plan.yaml"
        if not nominal_plan.exists():
            raise ValueError(
                "Systematics are present but no nominal variation was generated. "
                "Run a specific variation plan with `fasthep run-plan "
                "build/compile/<variation>/plan.yaml`."
            )
        return run_plan_file(
            nominal_plan,
            outdir=out_path,
            backend=backend,
            strategy=strategy,
            scheduler=scheduler,
            workers=workers,
        )
    return run_plan_file(
        plan_path(out_path),
        outdir=out_path,
        backend=backend,
        strategy=strategy,
        scheduler=scheduler,
        workers=workers,
    )


def diff_plan_files(
    old_plan: str | Path,
    new_plan: str | Path,
) -> tuple[str, bool]:
    """Return a formatted structural diff and equality flag for two plan files."""
    report = diff_plans(
        load_plan_yaml(old_plan),
        load_plan_yaml(new_plan),
    )
    return format_plan_diff(report), report.equal
