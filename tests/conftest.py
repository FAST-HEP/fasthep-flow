from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

current_dir = Path(__file__).parent


@pytest.fixture()
def simple_config_yaml() -> Path:
    return current_dir / "data" / "simple_config.yaml"


@pytest.fixture()
def simple_config(simple_config_yaml) -> str:
    with Path.open(simple_config_yaml, "r") as file:
        return file.read()


@pytest.fixture()
def simple_dict_config(simple_config) -> Any:
    return yaml.safe_load(simple_config)


@pytest.fixture()
def parallel_config_yaml() -> Path:
    return current_dir / "data" / "parallel.yaml"


@pytest.fixture()
def parallel_config(parallel_config_yaml) -> str:
    with Path.open(parallel_config_yaml, "r") as file:
        return file.read()


@pytest.fixture()
def parallel_dict_config(parallel_config) -> Any:
    return yaml.safe_load(parallel_config)


@pytest.fixture()
def plugin_config_yaml() -> Path:
    return current_dir / "data" / "plugins.yaml"


@pytest.fixture()
def plugin_config(plugin_config_yaml) -> str:
    with Path.open(plugin_config_yaml, "r") as file:
        return file.read()


@pytest.fixture()
def plugin_dict_config(plugin_config) -> Any:
    return yaml.safe_load(plugin_config)
