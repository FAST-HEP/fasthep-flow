from __future__ import annotations

from collections.abc import Callable
from functools import wraps
from typing import Any, Protocol

from fasthep_flow.config import PluginConfig


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
    func: Callable[..., Any], plugins: dict[str, list[PluginInterface]] | None = None
):
    """Decorator to wrap a task function with plugins."""
    local_plugins = plugins.get(func.__name__, []) if plugins else []

    @wraps(func)
    def _wrapper(*args, **kwargs):  # type: ignore[no-untyped-def]
        for plugin in local_plugins:
            plugin.before(func, *args, **kwargs)
        result = func(*args, **kwargs)
        if plugins:
            for plugin in local_plugins:
                plugin.after(func, result, *args, **kwargs)
        return result

    return _wrapper


def _load_plugin(plugin_config: PluginConfig) -> PluginInterface:
    """Load a plugin from a PluginConfig."""
    module_path, class_name = plugin_config.name.rsplit(".", 1)
    module = __import__(module_path, fromlist=[class_name])
    return getattr(module, class_name)(**plugin_config.kwargs)  # type: ignore[no-any-return]


def init_plugins(
    plugins_by_task: dict[str, list[PluginConfig]],
) -> dict[str, list[PluginInterface]]:
    """Initialize a dictionary of plugins."""
    plugins = {}
    for name, plugin_list in plugins_by_task.items():
        plugins[name] = [_load_plugin(plugin) for plugin in plugin_list]

    return plugins
