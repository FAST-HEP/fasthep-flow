"""
Core collection of plugins for the fasthep-flow package.
This might be moved to a separate package in the future (e.g. fasthep-core/carpenter).
"""

from __future__ import annotations

from ._base import PluginInterface, init_plugins, task_wrapper
from .logging import LoggingPlugin
from .output import LocalOutputPlugin
from .tests import PrintPlugin

__all__ = [
    "LocalOutputPlugin",
    "LoggingPlugin",
    "PluginInterface",
    "PrintPlugin",
    "init_plugins",
    "task_wrapper",
]
