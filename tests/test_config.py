from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from fasthep_flow.config import FlowConfig, StageConfig, load_config


# This would be a fixture providing various YAML configurations
@pytest.fixture()
def valid_config_yaml():
    return """
stages:
  - name: printEcho
    type: "fasthep_flow.operators.BashOperator"
    kwargs:
      bash_command: echo
      arguments: ["Hello World!"]
"""


def test_load_config_parses_correctly(valid_config_yaml: str, tmp_path: Path):
    # Create a temporary YAML config file
    temp_config_file = tmp_path / "config.yaml"
    temp_config_file.write_text(valid_config_yaml)

    # Now call load_config with the path to the temporary file
    parsed_config = load_config(temp_config_file)
    assert parsed_config is not None
    assert isinstance(parsed_config, FlowConfig)


def test_invalid_type():
    with pytest.raises(ValidationError) as excinfo:
        StageConfig(name="test", type="invalid.type")

    assert "Could not import invalid.type" in str(excinfo.value)


def test_first_stage(valid_config_yaml: str, tmp_path: Path):
    # Create a temporary YAML config file
    temp_config_file = tmp_path / "config.yaml"
    temp_config_file.write_text(valid_config_yaml)

    # Now call load_config with the path to the temporary file
    parsed_config = load_config(temp_config_file)
    first_stage = parsed_config.stages[0]
    assert first_stage.name == "printEcho"
    assert first_stage.type == "fasthep_flow.operators.BashOperator"
    assert first_stage.kwargs == {"bash_command": "echo", "arguments": ["Hello World!"]}

    resolved = first_stage.resolve()
    assert resolved.bash_command == "echo"
    assert resolved.arguments == ["Hello World!"]
