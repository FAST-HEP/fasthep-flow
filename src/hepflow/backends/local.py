from __future__ import annotations

from typing import Any

from hepflow.backends.model import BackendResult
from hepflow.model.plan import ExecutionPlan
from hepflow.runtime.engine import execute_plan_locally


class LocalBackend:
    name = "local.default"

    def run(
        self,
        plan: ExecutionPlan,
        *,
        ctx: dict[str, Any] | None = None,
    ) -> BackendResult:
        run_ctx = dict(ctx or {})
        warnings: list[dict[str, Any]] = []
        run_ctx["_warnings"] = warnings
        value = execute_plan_locally(
            plan,
            registry_cfg=plan.registry,
            ctx=run_ctx,
            partitions=plan.partitions or None,
        )
        summary = _value_store_summary(value, plan=plan)
        summary["warnings"] = warnings
        summary["hooks"] = run_ctx.get("_hook_summary") or {"enabled": []}
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
                {
                    "partition": plan.partitions[index].to_dict()
                    if index < len(plan.partitions)
                    else {"index": index},
                    "outputs": _store_outputs_summary(store),
                }
                for index, store in enumerate(value)
            ]
        }
    if isinstance(value, dict):
        return {
            "outputs": _store_outputs_summary(value),
        }
    return {
        "value_type": type(value).__name__,
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
