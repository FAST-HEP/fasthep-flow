from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

current_dir = Path(__file__).parent


@pytest.fixture()
def simple_config_yaml() -> Path:
    return current_dir / "simple_config.yaml"


@pytest.fixture()
def simple_config(simple_config_yaml) -> str:
    with Path.open(simple_config_yaml, "r") as file:
        return file.read()


@pytest.fixture()
def simple_dict_config(simple_config) -> Any:
    return yaml.safe_load(simple_config)
