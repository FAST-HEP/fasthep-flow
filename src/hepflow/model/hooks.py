from __future__ import annotations

from contextlib import contextmanager
from typing import Any


class ExecutionHook:
    """Runtime callback base class for lifecycle hook implementations."""

    name: str | None = None

    def __init__(self, **params: Any) -> None:
        self.params = dict(params)

    def partition_start(self, *, partition, ctx: dict[str, Any]) -> None:
        pass

    def before_node(self, *, node, inputs: dict[str, Any], ctx: dict[str, Any]) -> None:
        pass

    def after_node(
        self,
        *,
        node,
        inputs: dict[str, Any],
        outputs: Any,
        ctx: dict[str, Any],
    ) -> None:
        pass

    @contextmanager
    def around_node(self, *, node, inputs: dict[str, Any], ctx: dict[str, Any]):
        pass

    def on_node_error(
        self,
        *,
        node,
        inputs: dict[str, Any],
        ctx: dict[str, Any],
        exc: BaseException,
    ) -> None:
        pass

    def partition_end(self, *, partition, ctx: dict[str, Any], value_store) -> None:
        pass

    def dataset_end(
        self,
        *,
        dataset_name: str,
        ctx: dict[str, Any],
        value_store,
    ) -> None:
        pass

    def run_end(self, *, plan, ctx: dict[str, Any], summary: dict[str, Any]) -> None:
        pass
