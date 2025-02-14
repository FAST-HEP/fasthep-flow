from __future__ import annotations

import pytest

from fasthep_flow import FlowConfig
from fasthep_flow.workflow import create_workflow, get_task_source


@pytest.fixture()
def workflow(simple_dict_config):
    return create_workflow(FlowConfig.from_dictconfig(simple_dict_config))


@pytest.fixture()
def parallel_workflow(parallel_dict_config):
    return create_workflow(FlowConfig.from_dictconfig(parallel_dict_config))


def test_create_workflow(workflow):
    assert workflow.tasks
    assert len(workflow.tasks) == 1
    task = workflow.tasks[0]
    assert task.name == "printEcho"
    assert task.type == "fasthep_flow.operators.bash.LocalBashOperator"
    params = task.kwargs
    assert params["bash_command"] == "echo"
    assert params["arguments"] == ["Hello World!"]


def test_run_workflow(workflow):
    results = workflow.run()
    assert results
    assert len(results) == 1
    result = results[0]
    assert result["stdout"] == "Hello World!"


def test_task_names(workflow):
    assert workflow.task_names
    assert workflow.task_names == ["printEcho"]


def test_get_task_source(workflow):
    source = get_task_source(workflow.tasks[0], "printEcho")
    assert "def printEcho" in source


def test_parallel(parallel_workflow):
    assert parallel_workflow.tasks
    assert len(parallel_workflow.tasks) == 5
    assert parallel_workflow.tasks[0].name == "A"
    assert parallel_workflow.tasks[2].name == "C"
    assert parallel_workflow.tasks[4].name == "Y"
    assert parallel_workflow.tasks[0].needs == []
    assert parallel_workflow.tasks[2].needs == ["A"]
    assert parallel_workflow.tasks[4].needs == ["C", "D"]
