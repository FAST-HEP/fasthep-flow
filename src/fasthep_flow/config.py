from __future__ import annotations

import pathlib
from dataclasses import dataclass
from typing import Any

from omegaconf import OmegaConf


@dataclass
class StageConfig:
    name: str
    type: str
    needs: list[str]
    args: list[Any]
    kwargs: dict[str, Any]


@dataclass
class FlowConfig:
    stages: list[StageConfig]


def load_config(config_file: pathlib.Path) -> Any:
    schema = OmegaConf.structured(FlowConfig)
    conf = OmegaConf.load(config_file)
    return OmegaConf.merge(schema, conf)
