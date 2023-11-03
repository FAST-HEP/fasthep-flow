from __future__ import annotations

import importlib
import pathlib
from typing import Any

from omegaconf import OmegaConf
from pydantic import Field, validator
from pydantic.dataclasses import dataclass


@dataclass
class StageConfig:
    """A stage in the workflow."""

    name: str
    type: str
    needs: list[str] | None = Field(default_factory=list)
    args: list[Any] | None = Field(default_factory=list)
    kwargs: dict[str, Any] | None = Field(default_factory=dict)

    @validator("type")
    def validate_type(cls, v: str) -> str:
        # Split the string to separate the module from the class name
        module_path, class_name = v.rsplit(".", 1)
        try:
            # Import the module
            mod = importlib.import_module(module_path)
            # this must be a class
            getattr(mod, class_name)
        except ImportError as e:
            msg = f"Could not import {module_path}.{class_name}"
            raise ValueError(msg) from e
        # Return the original string if everything is fine
        return v


@dataclass
class FlowConfig:
    """The workflow."""

    stages: list[StageConfig]


def load_config(config_file: pathlib.Path) -> Any:
    """Load a config file and return the structured config."""
    schema = OmegaConf.structured(FlowConfig)
    conf = OmegaConf.load(config_file)
    merged_conf = OmegaConf.merge(schema, conf)
    return OmegaConf.to_object(merged_conf)
