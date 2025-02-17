from __future__ import annotations

from collections.abc import Callable
from functools import wraps
from typing import Any, Protocol


class PluginInterface(Protocol):
    """Interface for defining a plugin."""

    def before(self, func: Callable[..., Any], *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        """Function to run before the task."""

    def after(  # type: ignore[no-untyped-def]
        self,
        func: Callable[..., Any],
        result: Any,
        *args,
        **kwargs,
    ) -> None:
        """Function to run after the task."""


def task_wrapper(  # type: ignore[no-untyped-def]
    func: Callable[..., Any], plugins: list[PluginInterface] | None = None
):
    """Decorator to wrap a task function with plugins."""

    @wraps(func)
    def _wrapper(*args, **kwargs):  # type: ignore[no-untyped-def]
        if plugins:
            for plugin in plugins:
                plugin.before(func, *args, **kwargs)
        result = func(*args, **kwargs)
        if plugins:
            for plugin in plugins:
                plugin.after(func, result, *args, **kwargs)
        return result

    return _wrapper
