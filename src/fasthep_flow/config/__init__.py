from __future__ import annotations

import importlib
import pathlib
from typing import Any

from omegaconf import OmegaConf

from .v0 import FlowConfig, PluginConfig, TaskConfig

DEFAULT_VERSION = "v0"


def load_config(config_file: pathlib.Path) -> Any:
    """Load a config file and return the structured config."""
    conf = OmegaConf.load(config_file)
    # read version from config
    version = conf.get("version", DEFAULT_VERSION)
    cfg_class = importlib.import_module(f".{version}", __package__).FlowConfig

    flow = cfg_class.from_dictconfig(conf)
    flow.metadata = {"config_file": str(config_file), "name": config_file.stem}
    return flow


def plugins_by_task(config: FlowConfig) -> dict[str, list[PluginConfig]]:
    """Return a dictionary of plugins by task name."""
    plugins = {}
    global_plugins = config.plugins
    for task in config.tasks:
        plugins[task.name] = global_plugins.copy() if global_plugins else []
        if task.plugins:
            plugins[task.name] += task.plugins
    return plugins


__all__ = ["FlowConfig", "PluginConfig", "TaskConfig", "load_config"]
