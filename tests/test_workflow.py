from __future__ import annotations

import pytest

from fasthep_flow import FlowConfig
from fasthep_flow.workflow import create_workflow


@pytest.fixture()
def config(simple_dict_config):
    return FlowConfig.from_dictconfig(simple_dict_config)


def test_create_workflow(config):
    workflow = create_workflow(config)
    assert workflow.tasks
    assert len(workflow.tasks) == 1
    task = workflow.tasks[0]
    assert task.name == "printEcho"
    assert task.type == "fasthep_flow.operators.bash.LocalBashOperator"
    params = task.kwargs
    assert params["bash_command"] == "echo"
    assert params["arguments"] == ["Hello World!"]


def test_run_workflow(config):
    workflow = create_workflow(config)
    results = workflow.run()
    assert results
    assert len(results) == 1
    result = results[0]
    assert result["stdout"] == "Hello World!\n"


def test_task_names(config):
    workflow = create_workflow(config)
    assert workflow.task_names
    assert workflow.task_names == ["printEcho"]
