from __future__ import annotations

from collections.abc import Callable
from typing import Any

from loguru import logger

from ._base import PluginInterface


class LoggingPlugin(PluginInterface):
    """Plugin to log task execution."""

    def before(self, func: Callable[..., Any], *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        logger.info(f"Running {func.__name__} with args {args} and kwargs {kwargs}")

    def after(self, func: Callable[..., Any], result: Any, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        if result:
            logger.success(f"Finished {func.__name__} with result {result}")
        logger.warning(f"Finished {func.__name__} with no result")
