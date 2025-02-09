from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from fasthep_flow.config import FlowConfig, TaskConfig, load_config


def test_load_config_parses_correctly(simple_config_yaml: Path):
    parsed_config = load_config(simple_config_yaml)
    assert parsed_config is not None
    assert isinstance(parsed_config, FlowConfig)


def test_invalid_type():
    with pytest.raises(ValidationError) as excinfo:
        TaskConfig(name="test", type="invalid.type")

    assert "Could not import invalid.type" in str(excinfo.value)


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
