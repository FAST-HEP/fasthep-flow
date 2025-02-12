from __future__ import annotations

from ._base import PluginInterface


class PrintPlugin(PluginInterface):
    def before_run(self, func, *args, **kwargs):  # type: ignore[no-untyped-def]
        print(  # noqa: T201
            f"Running {func.__name__} with args {args} and kwargs {kwargs}"
        )

    def after_run(self, func, result, *args, **kwargs):  # type: ignore[no-untyped-def]
        print(f"Finished running {func.__name__} with result {result}")  # noqa: T201
