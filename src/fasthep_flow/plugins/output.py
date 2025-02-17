from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

from ._base import PluginInterface


class LocalOutputPlugin(PluginInterface):
    """Plugin for writing output to a local file.
    Output will be stored with the function hash as the filename.

    @output_path: path to write the output to.
    @func_hash: hash of the function that generated the output
    """

    output_path: Path
    func_hash: str

    def before(self, func: Callable[..., Any], *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        pass

    def after(self, func: Callable[..., Any], result: Any, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        pass
