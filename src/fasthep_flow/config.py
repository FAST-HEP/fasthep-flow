from __future__ import annotations

import pathlib
from dataclasses import dataclass
from typing import Any

from omegaconf import OmegaConf


@dataclass
class StageConfig:
    """A stage in the workflow."""

    name: str
    type: str
    needs: list[str]
    args: list[Any]
    kwargs: dict[str, Any]


@dataclass
class FlowConfig:
    """The workflow."""

    stages: list[StageConfig]


def load_config(config_file: pathlib.Path) -> Any:
    """Load a config file and return the structured config."""
    schema = OmegaConf.structured(FlowConfig)
    conf = OmegaConf.load(config_file)
    return OmegaConf.merge(schema, conf)
