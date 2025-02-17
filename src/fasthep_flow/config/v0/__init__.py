"""Definitions for the configuration for describing the workflow."""

from __future__ import annotations

import importlib
from dataclasses import field
from typing import Any

from omegaconf import DictConfig, ListConfig, OmegaConf
from pydantic import field_validator
from pydantic.dataclasses import dataclass

ALIASES = {
    "fasthep_flow.operators.BashOperator": "fasthep_flow.operators.bash.LocalBashOperator",
}


def _validate_class_import(value: str) -> str:
    """Validate the type field
    Any specified type needs to be a Python class that can be imported"""
    # Split the string to separate the module from the class name
    value = ALIASES.get(value, value)
    module_path, class_name = value.rsplit(".", 1)
    try:
        # Import the module
        mod = importlib.import_module(module_path)
        # this must be a class
        getattr(mod, class_name)
    except ImportError as previous_error:
        msg = f"Could not import {module_path}.{class_name}"
        raise ValueError(msg) from previous_error
    # Return the original string if everything is fine
    return value


def _rename_keys(
    cfg: dict[Any, Any] | list[Any] | Any, key_map: dict[str, str]
) -> dict[str, Any] | list[Any] | Any:
    """
    Recursively rename keys in a dictionary based on key_map.
    """
    if isinstance(cfg, dict):
        new_cfg = {}
        for key, value in cfg.items():
            new_key = key_map.get(key, key)
            new_cfg[new_key] = _rename_keys(value, key_map)
        return new_cfg
    if isinstance(cfg, list):
        return [_rename_keys(item, key_map) for item in cfg]
    return cfg


def _preprocess_config(cfg: DictConfig | ListConfig) -> DictConfig | ListConfig:
    """
    Preprocess the config to rename keys and validate the config.
    """
    key_map = {
        "with": "kwargs",
    }
    if not isinstance(cfg, DictConfig):
        return cfg
    cfg_dict = OmegaConf.to_container(cfg, resolve=True)
    renamed_cfg_dict = _rename_keys(cfg_dict, key_map)
    return OmegaConf.create(renamed_cfg_dict)


@dataclass
class PluginConfig:
    """A plugin in the workflow."""

    name: str
    kwargs: dict[str, Any] | None = field(default_factory=dict[str, Any])

    @field_validator("name")
    @classmethod
    def validate_type(cls, value: str) -> str:
        """Validate the name field
        Any specified name needs to be a Python class that can be imported"""
        return _validate_class_import(value)

    def resolve(self) -> Any:
        """Resolve the plugin to a class."""


@dataclass
class TaskConfig:
    """A task in the workflow."""

    name: str
    type: str
    needs: list[Any] | None = field(default_factory=list)
    args: list[Any] | None = field(default_factory=list)
    kwargs: dict[Any, Any] | None = field(default_factory=dict)
    plugins: list[PluginConfig] | None = field(default_factory=list[PluginConfig])

    @field_validator("type")
    @classmethod
    def validate_type(cls, value: str) -> str:
        """Validate the type field
        Any specified type needs to be a Python class that can be imported"""

        return _validate_class_import(value)

    def resolve(self) -> Any:
        """Resolve the task to a class."""
        module_path, class_name = self.type.rsplit(".", 1)
        mod = importlib.import_module(module_path)
        class_ = getattr(mod, class_name)
        return class_(*self.args, **self.kwargs)


@dataclass
class FlowConfig:
    """The workflow."""

    tasks: list[TaskConfig]
    # optional metadata
    metadata: dict[str, Any] = field(default_factory=dict[str, Any])
    # optional plugins
    plugins: list[PluginConfig] | None = field(default_factory=list[PluginConfig])

    @staticmethod
    def from_dictconfig(config: DictConfig | ListConfig) -> FlowConfig:
        """Create a FlowConfig from a dictionary."""
        schema = OmegaConf.structured(FlowConfig)
        processed_cfg = _preprocess_config(config)
        merged_conf = OmegaConf.merge(schema, processed_cfg)
        return FlowConfig(**OmegaConf.to_container(merged_conf))

    def __post_init__(self) -> None:
        """Post init function to set metadata."""
        self.metadata = {"config_file": "", "name": "fasthep-flow"}

    @property
    def config_file(self) -> str:
        """Return the path to the config file."""
        if "config_file" not in self.metadata:
            msg = "config_file is not set in metadata for FlowConfig"
            raise ValueError(msg)
        return str(self.metadata["config_file"])

    @property
    def name(self) -> str:
        """Return the name of the config."""
        if "name" not in self.metadata:
            msg = "name is not set in metadata for FlowConfig"
            raise ValueError(msg)
        return str(self.metadata["name"])
