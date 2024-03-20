"""Definitions for the configuration for describing the workflow."""

from __future__ import annotations

import importlib
import pathlib
from dataclasses import field
from typing import Any

from omegaconf import DictConfig, ListConfig, OmegaConf
from pydantic import field_validator
from pydantic.dataclasses import dataclass


@dataclass
class StageConfig:
    """A stage in the workflow."""

    name: str
    type: str
    needs: list[Any] | None = field(default_factory=list)
    args: list[Any] | None = field(default_factory=list)
    kwargs: dict[Any, Any] | None = field(default_factory=dict)

    @field_validator("type")
    @classmethod
    def validate_type(cls, value: str) -> str:
        """Validate the type field
        Any specified type needs to be a Python class that can be imported"""
        # Split the string to separate the module from the class name
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

    def resolve(self) -> Any:
        """Resolve the stage to a class."""
        module_path, class_name = self.type.rsplit(".", 1)
        mod = importlib.import_module(module_path)
        class_ = getattr(mod, class_name)
        return class_(*self.args, **self.kwargs)


@dataclass
class FlowConfig:
    """The workflow."""

    stages: list[StageConfig]

    @staticmethod
    def from_dictconfig(config: DictConfig | ListConfig) -> FlowConfig:
        """Create a FlowConfig from a dictionary."""
        schema = OmegaConf.structured(FlowConfig)
        merged_conf = OmegaConf.merge(schema, config)
        return FlowConfig(**OmegaConf.to_container(merged_conf))


def load_config(config_file: pathlib.Path) -> Any:
    """Load a config file and return the structured config."""
    conf = OmegaConf.load(config_file)
    return FlowConfig.from_dictconfig(conf)
