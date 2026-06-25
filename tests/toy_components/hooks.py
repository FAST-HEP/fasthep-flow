from __future__ import annotations

from contextlib import contextmanager
from typing import Any

TOY_CONTEXT_HOOK_SPEC = {
    "name": "toy.context",
    "kind": "hook",
    "lifecycle": {
        "events": [
            "partition_start",
            "around_node",
            "before_node",
            "after_node",
            "run_end",
        ],
    },
    "context_outputs": ["toy_context"],
}

TOY_ORDER_HOOK_SPEC = {
    "name": "toy.order",
    "kind": "hook",
    "lifecycle": {"events": ["around_node", "before_node", "after_node"]},
    "context_outputs": [],
}

TOY_OLD_SHAPE_HOOK_SPEC = {
    "name": "toy.old_shape",
    "kind": "hook",
    "events": ["partition_start"],
    "context_outputs": ["toy_context"],
}


class ToyContextHook:
    def __init__(self, value: str = "ready") -> None:
        self.value = value
        self.events: list[str] = []

    def partition_start(self, *, partition: Any, ctx: dict[str, Any]) -> None:
        self.events.append("partition_start")
        ctx["toy_context"] = self.value

    @contextmanager
    def around_node(self, *, node: Any, inputs: dict[str, Any], ctx: dict[str, Any]):
        self.events.append(f"around:{node.id}")
        yield

    def before_node(self, *, node: Any, inputs: dict[str, Any], ctx: dict[str, Any]) -> None:
        self.events.append(f"before:{node.id}")

    def after_node(
        self,
        *,
        node: Any,
        inputs: dict[str, Any],
        outputs: Any,
        ctx: dict[str, Any],
    ) -> None:
        self.events.append(f"after:{node.id}")

    def run_end(self, *, plan: Any, ctx: dict[str, Any], summary: dict[str, Any]) -> None:
        self.events.append("run_end")
        summary["toy_hook_events"] = list(self.events)


class ToyOrderHook:
    @contextmanager
    def around_node(self, *, node: Any, ctx: dict[str, Any], **_: Any):
        if node.id == "stage.Scale":
            ctx.setdefault("_modifier_events", []).append("global.around.enter")
        try:
            yield
        finally:
            if node.id == "stage.Scale":
                ctx.setdefault("_modifier_events", []).append("global.around.exit")

    def before_node(self, *, node: Any, ctx: dict[str, Any], **_: Any) -> None:
        if node.id == "stage.Scale":
            ctx.setdefault("_modifier_events", []).append("global.before")

    def after_node(self, *, node: Any, ctx: dict[str, Any], **_: Any) -> None:
        if node.id == "stage.Scale":
            ctx.setdefault("_modifier_events", []).append("global.after")
