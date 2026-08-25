from __future__ import annotations

from typing import Any

from hepflow.backends.model import BackendResult
from hepflow.model.plan import ExecutionPlan
from hepflow.progress import ProgressReporter
from hepflow.runtime.boundary import PartitionBoundaryResult
from hepflow.runtime.engine import execute_plan_locally


class LocalBackend:
    name = "local.default"

    def run(
        self,
        plan: ExecutionPlan,
        *,
        ctx: dict[str, Any] | None = None,
        progress: ProgressReporter | None = None,
    ) -> BackendResult:
        run_ctx = dict(ctx or {})
        warnings: list[dict[str, Any]] = []
        run_ctx["_warnings"] = warnings
        reporter = progress or ProgressReporter(plan.partitions)
        try:
            value = execute_plan_locally(
                plan,
                registry_cfg=plan.registry,
                ctx=run_ctx,
                partitions=plan.partitions or None,
                progress=reporter,
            )
        finally:
            warnings.extend(
                {"kind": "progress_sink", **warning}
                for warning in reporter.close()
            )
        summary = _value_store_summary(value, plan=plan)
        summary["warnings"] = warnings
        summary["hooks"] = run_ctx.get("_hook_summary") or {"enabled": []}
        summary["progress"] = {
            "run_id": reporter.run_id,
            "counts": reporter.counts.to_dict(),
        }
        return BackendResult(
            backend="local",
            strategy="default",
            success=True,
            outputs={"value_store": value},
            summary=summary,
        )


def _value_store_summary(value: Any, *, plan: ExecutionPlan) -> dict[str, Any]:
    if isinstance(value, list):
        return {
            "partitions": [
                _partition_summary(item, index=index, plan=plan)
                for index, item in enumerate(value)
            ]
        }
    if isinstance(value, dict):
        return {
            "outputs": _store_outputs_summary(value),
        }
    return {
        "value_type": type(value).__name__,
    }


def _partition_summary(
    item: Any,
    *,
    index: int,
    plan: ExecutionPlan,
) -> dict[str, Any]:
    if isinstance(item, PartitionBoundaryResult):
        return {
            "partition": item.partition.to_dict(),
            "outputs": [
                {
                    "node": product.node_id,
                    "port": product.output_name,
                    "kind": product.kind,
                    "representation": product.representation,
                    "type": type(product.value).__name__,
                }
                for product in item.products
            ],
        }
    return {
        "partition": plan.partitions[index].to_dict()
        if index < len(plan.partitions)
        else {"index": index},
        "outputs": _store_outputs_summary(item),
    }


def _store_outputs_summary(store: dict[Any, Any]) -> list[dict[str, str]]:
    return [
        {
            "node": str(node_id),
            "port": str(output_name),
            "type": type(value).__name__,
        }
        for (node_id, output_name), value in sorted(store.items())
    ]
