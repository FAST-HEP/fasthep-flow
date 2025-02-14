from __future__ import annotations

import importlib
import pathlib
from typing import Any

from omegaconf import OmegaConf

DEFAULT_VERSION = "v0"
default_module = importlib.import_module(f".{DEFAULT_VERSION}", __package__)
FlowConfig = default_module.FlowConfig
PluginConfig = default_module.PluginConfig
TaskConfig = default_module.TaskConfig


def load_config(config_file: pathlib.Path) -> Any:
    """Load a config file and return the structured config."""
    conf = OmegaConf.load(config_file)
    # read version from config
    version = conf.get("version", DEFAULT_VERSION)
    FlowConfig = importlib.import_module(f".{version}", __package__).FlowConfig

    flow = FlowConfig.from_dictconfig(conf)
    flow.metadata = {"config_file": str(config_file), "name": config_file.stem}
    return flow


__all__ = ["FlowConfig", "PluginConfig", "TaskConfig", "load_config"]
