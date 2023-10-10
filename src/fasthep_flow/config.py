from dataclasses import dataclass
from omegaconf import OmegaConf
import pathlib
from typing import Any


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


def load_config(config_file: pathlib.Path) -> FlowConfig:
    schema = OmegaConf.structured(FlowConfig)
    conf = OmegaConf.load(config_file)
    merged_conf = OmegaConf.merge(schema, conf)
    return merged_conf
