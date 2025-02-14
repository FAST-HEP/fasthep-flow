from __future__ import annotations

from collections.abc import Callable
from typing import Any

from ._base import PluginInterface


class LoggingPlugin(PluginInterface):
    def before(self, func: Callable[..., Any], *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        print(  # noqa: T201
            f"Running {func.__name__} with args {args} and kwargs {kwargs}"
        )

    def after(self, func: Callable[..., Any], result: Any, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        print(f"Finished {func.__name__} with result {result}")  # noqa: T201
