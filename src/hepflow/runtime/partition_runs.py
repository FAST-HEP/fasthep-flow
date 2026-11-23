"""Run the partitions of a per-dataset plan one at a time, each with its own records.

Production jobs run one partition each; an invocation that selects several still produces one
record set per partition under ``runs/[<variation>/]<dataset>/<N>/``, so the merge step reads the
same thing either way. Every dataset's binding is checked before any partition runs.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from hepflow.backends.model import BackendResult
from hepflow.build_layout import BuildPaths
from hepflow.compiler.dataset_units import MANIFEST_NAME, read_manifest, unit_dir
from hepflow.model.plan import ExecutionPartition, ExecutionPlan
from hepflow.progress import ProgressReporter, ProgressSink
from hepflow.runtime.run_records import (
    Binding,
    build_binding,
    claim_attempt_dir,
    partition_summary,
    publish_binding,
    write_partition_summary,
)


def run_partition_records(
    plan_file: Path,
    plan: ExecutionPlan,
    *,
    all_partitions: Sequence[ExecutionPartition],
    backend: Any,
    build_paths: BuildPaths,
    run_ctx: dict[str, Any],
    progress_sinks: list[ProgressSink] | None = None,
) -> BackendResult:
    """Run ``plan.partitions`` one by one; ``plan`` already holds only the selected ones.

    ``all_partitions`` are those of the loaded plan, which give each dataset's partition count K.
    """
    datasets = list(dict.fromkeys(p.dataset for p in plan.partitions))
    bindings = _publish_bindings(plan_file, datasets, build_paths)
    totals = {d: sum(1 for p in all_partitions if p.dataset == d) for d in datasets}

    results: list[BackendResult] = []
    runs: list[str] = []
    for partition in plan.partitions:
        results.append(
            _run_one(
                plan,
                partition,
                backend=backend,
                build_paths=build_paths,
                run_ctx=run_ctx,
                binding=bindings[partition.dataset],
                total=totals[partition.dataset],
                progress_sinks=progress_sinks,
            )
        )
        runs.append(str(build_paths.partition_run_dir(partition.dataset, partition.part)))
    merged = _merge(results)
    merged.summary["partition_runs"] = runs
    return merged


def _publish_bindings(
    plan_file: Path, datasets: Sequence[str], build_paths: BuildPaths
) -> dict[str, Binding]:
    bindings: dict[str, Binding] = {}
    for dataset in datasets:
        manifest = read_manifest(unit_dir(plan_file.parent, dataset) / MANIFEST_NAME)
        bindings[dataset] = build_binding(
            plan_file, manifest=manifest, variation=build_paths.variation
        )
    # Checked for every dataset before any partition writes a file.
    for dataset, binding in bindings.items():
        publish_binding(build_paths.binding_file(dataset), binding)
    return bindings


def _run_one(
    plan: ExecutionPlan,
    partition: ExecutionPartition,
    *,
    backend: Any,
    build_paths: BuildPaths,
    run_ctx: dict[str, Any],
    binding: Binding,
    total: int,
    progress_sinks: list[ProgressSink] | None,
) -> BackendResult:
    run_dir = build_paths.partition_run_dir(partition.dataset, partition.part)
    attempt_id, attempt_dir = claim_attempt_dir(build_paths.attempts_dir(partition.dataset, partition.part))
    (attempt_dir / "attempt.json").write_text(
        json.dumps({"attempt": attempt_id, "started": datetime.now(UTC).isoformat()}) + "\n",
        encoding="utf-8",
    )
    paths = replace(build_paths, run_dir=run_dir)
    ctx = {**run_ctx, "build_paths": paths, "attempt_id": attempt_id}
    one = replace(plan, partitions=[partition])
    reporter = ProgressReporter([partition], sinks=progress_sinks) if progress_sinks else None
    result = backend.run(one, ctx=ctx, progress=reporter)
    write_partition_summary(
        paths,
        partition_summary(
            dataset=partition.dataset,
            partition=partition.part,
            partitions=total,
            binding=binding,
            attempt_id=attempt_id,
            run={
                "backend": result.backend,
                "strategy": result.strategy,
                "success": result.success,
                **result.summary,
            },
        ),
    )
    return result


def _merge(results: Sequence[BackendResult]) -> BackendResult:
    last = results[-1]
    summary: dict[str, Any] = dict(last.summary)
    for key in ("partitions", "warnings"):
        merged = [item for r in results for item in r.summary.get(key) or []]
        if merged or key in summary:
            summary[key] = merged
    counts: dict[str, int] = {}
    for result in results:
        for key, value in ((result.summary.get("progress") or {}).get("counts") or {}).items():
            counts[key] = counts.get(key, 0) + int(value)
    if counts:
        summary["progress"] = {**dict(summary.get("progress") or {}), "counts": counts}
    return BackendResult(
        backend=last.backend,
        strategy=last.strategy,
        success=all(r.success for r in results),
        outputs=dict(last.outputs),
        summary=summary,
    )
