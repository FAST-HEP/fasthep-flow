from __future__ import annotations

from pathlib import Path

import pytest

from fasthep_flow.config import FlowConfig, load_config


# This would be a fixture providing various YAML configurations
@pytest.fixture()
def valid_config_yaml():
    return """
stages:
  - name: printEcho
    type: "airflow.operators.bash.BashOperator"
    kwargs:
      bash_command: echo "Hello World!"
"""


def test_load_config_parses_correctly(valid_config_yaml: str, tmp_path: Path):
    # Create a temporary YAML config file
    temp_config_file = tmp_path / "config.yaml"
    temp_config_file.write_text(valid_config_yaml)

    # Now call load_config with the path to the temporary file
    parsed_config = load_config(temp_config_file)
    assert parsed_config is not None
    assert isinstance(parsed_config, FlowConfig)
