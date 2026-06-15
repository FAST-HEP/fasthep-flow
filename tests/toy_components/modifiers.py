from __future__ import annotations

from contextlib import contextmanager
from typing import Any


class ToyExecutionModifier:
    def __init__(self, label: str) -> None:
        self.label = label

    def before_node(
        self,
        *,
        inputs: dict[str, Any],
        ctx: dict[str, Any],
        **_: Any,
    ) -> None:
        _record(ctx, f"{self.label}.before")
        stream = dict(inputs["stream"])
        field = ctx.get(f"{self.label}_field")
        if field:
            stream[str(field)] = [value + 1 for value in stream["pt"]]
        inputs["stream"] = stream

    @contextmanager
    def around_node(self, *, ctx: dict[str, Any], **_: Any) -> Any:
        _record(ctx, f"{self.label}.around.enter")
        try:
            yield
        finally:
            _record(ctx, f"{self.label}.around.exit")

    def after_node(
        self,
        *,
        outputs: dict[str, Any],
        ctx: dict[str, Any],
        **_: Any,
    ) -> None:
        _record(ctx, f"{self.label}.after")
        stream = dict(outputs.get("stream") or {})
        stream[f"{self.label}_after"] = True
        outputs["stream"] = stream


class FailingBeforeModifier:
    def before_node(self, **_: Any) -> None:
        raise ValueError("before boom")


class FailingAroundModifier:
    @contextmanager
    def around_node(self, **_: Any) -> Any:
        raise ValueError("around boom")
        yield


class FailingAfterModifier:
    def after_node(self, **_: Any) -> None:
        raise ValueError("after boom")


MODIFIER_A = ToyExecutionModifier("A")
MODIFIER_B = ToyExecutionModifier("B")
FAILING_BEFORE = FailingBeforeModifier()
FAILING_AROUND = FailingAroundModifier()
FAILING_AFTER = FailingAfterModifier()
INVALID_MODIFIER = object()


def _record(ctx: dict[str, Any], event: str) -> None:
    ctx.setdefault("_modifier_events", []).append(event)
