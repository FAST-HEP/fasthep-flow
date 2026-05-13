from __future__ import annotations

from typing import Any

TOY_OBSERVER_SPEC = {
    "name": "toy.observe",
    "kind": "observer",
    "input": {"name": "target", "required": True},
    "params": {},
    "result": {"report": "report"},
}


def run_toy_observer(
    *,
    target: Any,
    ctx: dict[str, Any] | None = None,
    **params: Any,
) -> dict[str, Any]:
    return {
        "type": type(target).__name__,
        "keys": sorted(target) if isinstance(target, dict) else [],
    }
