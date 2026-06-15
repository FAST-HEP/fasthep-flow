from __future__ import annotations

from typing import Any


class ToyExecutionModifier:
    def __init__(self, label: str) -> None:
        self.label = label

    def before(
        self,
        *,
        stream: dict[str, Any],
        params: dict[str, Any],
        ctx: dict[str, Any],
        **_: Any,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        _record(ctx, f"{self.label}.before")
        next_stream = dict(stream)
        next_params = dict(params)
        field = next_params.pop(f"{self.label}_field", None)
        if field:
            next_stream[str(field)] = [value + 1 for value in next_stream["pt"]]
        factor = next_params.pop(f"{self.label}_factor", None)
        if factor is not None:
            next_params["factor"] = factor
        return next_stream, next_params

    def wrap(self, *, func: Any, ctx: dict[str, Any], **_: Any) -> Any:
        def wrapped(**kwargs: Any) -> Any:
            _record(ctx, f"{self.label}.wrap.enter")
            result = func(**kwargs)
            _record(ctx, f"{self.label}.wrap.exit")
            return result

        return wrapped

    def after(
        self,
        *,
        result: dict[str, Any],
        ctx: dict[str, Any],
        **_: Any,
    ) -> dict[str, Any]:
        _record(ctx, f"{self.label}.after")
        stream = dict(result.get("stream") or {})
        stream[f"{self.label}_after"] = True
        return {**result, "stream": stream}


class FailingBeforeModifier:
    def before(self, **_: Any) -> None:
        raise ValueError("before boom")


class FailingWrapModifier:
    def wrap(self, **_: Any) -> None:
        raise ValueError("wrap boom")


class FailingAfterModifier:
    def after(self, **_: Any) -> None:
        raise ValueError("after boom")


MODIFIER_A = ToyExecutionModifier("A")
MODIFIER_B = ToyExecutionModifier("B")
FAILING_BEFORE = FailingBeforeModifier()
FAILING_WRAP = FailingWrapModifier()
FAILING_AFTER = FailingAfterModifier()
INVALID_MODIFIER = object()


def _record(ctx: dict[str, Any], event: str) -> None:
    ctx.setdefault("_modifier_events", []).append(event)
