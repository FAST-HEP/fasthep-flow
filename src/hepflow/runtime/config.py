from __future__ import annotations

from pathlib import Path
from typing import Any

from hepflow.backends.loaders import normalize_backend_override


def _runtime_execution_with_overrides(
    execution: dict[str, Any] | None,
    *,
    backend: str | None,
    strategy: str | None,
    scheduler: str | None,
    workers: int | None,
) -> dict[str, Any]:
    runtime_execution = dict(execution or {})
    override = normalize_backend_override(backend, strategy)
    if override:
        runtime_execution.update(override)

    runtime_execution["backend"] = str(runtime_execution.get("backend") or "local")
    runtime_execution["strategy"] = str(runtime_execution.get("strategy") or "default")
    runtime_execution["config"] = dict(runtime_execution.get("config") or {})
    if scheduler is not None:
        runtime_execution["config"]["scheduler"] = scheduler
    if workers is not None:
        runtime_execution["config"]["n_workers"] = workers
    return runtime_execution


def default_run_outdir_for_plan(plan_file: Path) -> Path:
    plan_file = Path(plan_file)
    if plan_file.name == "plan.yaml" and plan_file.parent.parent.name == "compile":
        build_root = plan_file.parent.parent.parent
        return build_root / plan_file.parent.name
    if plan_file.parent.name == "compile":
        return plan_file.parent.parent
    return plan_file.parent


_default_run_outdir = default_run_outdir_for_plan
