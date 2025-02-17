from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from fasthep_flow.config import (
    FlowConfig,
    PluginConfig,
    TaskConfig,
    load_config,
    plugins_by_task,
)


@pytest.fixture()
def just_login_plugin() -> PluginConfig:
    return PluginConfig(
        name="fasthep_flow.plugins.LoggingPlugin",
        kwargs={"level": "TRACE"},
    )


@pytest.fixture()
def just_output_plugin() -> PluginConfig:
    return PluginConfig(
        name="fasthep_flow.plugins.LocalOutputPlugin",
        kwargs={"checksum": "adler32", "output_path": "/tmp/fasthep_flow"},
    )


@pytest.fixture()
def all_plugins(just_login_plugin, just_output_plugin) -> list[PluginConfig]:
    return [just_login_plugin, just_output_plugin]


def test_load_config_parses_correctly(simple_config_yaml: Path):
    parsed_config = load_config(simple_config_yaml)
    assert parsed_config is not None
    assert isinstance(parsed_config, FlowConfig)


def test_invalid_type():
    with pytest.raises(ValidationError) as excinfo:
        TaskConfig(name="test", type="invalid.type")

    assert "Unable to import invalid.type" in str(excinfo.value)


def test_first_task(simple_config_yaml: Path):
    # Now call load_config with the path to the temporary file
    parsed_config = load_config(simple_config_yaml)
    first_task = parsed_config.tasks[0]
    assert first_task.name == "printEcho"
    assert first_task.type == "fasthep_flow.operators.bash.LocalBashOperator"
    assert first_task.kwargs == {"bash_command": "echo", "arguments": ["Hello World!"]}

    resolved = first_task.resolve()
    assert resolved.bash_command == "echo"
    assert resolved.arguments == ["Hello World!"]


def test_plugins_by_task(
    plugin_config_yaml: Path,
    just_login_plugin: PluginConfig,
    just_output_plugin: PluginConfig,
):
    parsed_config = load_config(plugin_config_yaml)
    plugins = plugins_by_task(parsed_config)

    assert plugins["A"] == [just_login_plugin]
    assert plugins["B"] == [just_login_plugin]
    assert plugins["Y"] == [just_login_plugin, just_output_plugin]


def test_plugins_by_task_no_plugins(simple_config_yaml: Path):
    parsed_config = load_config(simple_config_yaml)
    plugins = plugins_by_task(parsed_config)
    assert plugins["printEcho"] == []
