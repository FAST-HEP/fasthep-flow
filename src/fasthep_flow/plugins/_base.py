from __future__ import annotations

from collections.abc import Callable
from functools import wraps
from typing import Any, Protocol


class PluginInterface(Protocol):
    def before_run(self, func: Callable[..., Any], *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        pass

    def after_run(  # type: ignore[no-untyped-def]
        self,
        func: Callable[..., Any],
        result: Any,
        *args,
        **kwargs,
    ) -> None:
        pass


def task_wrapper(  # type: ignore[no-untyped-def]
    func: Callable[..., Any], plugins: list[PluginInterface] | None = None
):
    @wraps(func)
    def _wrapper(*args, **kwargs):  # type: ignore[no-untyped-def]
        if plugins:
            for plugin in plugins:
                plugin.before_run(func, *args, **kwargs)
        result = func(*args, **kwargs)
        if plugins:
            for plugin in plugins:
                plugin.after_run(func, result, *args, **kwargs)
        return result

    return _wrapper
