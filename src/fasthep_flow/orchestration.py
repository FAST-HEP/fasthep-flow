"""Orchestration module for the fasthep-flow package.
This module contains functions to convert a fasthep-flow workflow into various other workflows,
e.g. Prefect, NetworkX, etc., to either execute them or visualize them.
"""

from __future__ import annotations

from typing import Any

from .workflow import Workflow


def get_runner(runner: str) -> Any:
    """Get the task runner for the given name."""
    from prefect.task_runners import ConcurrentTaskRunner, SequentialTaskRunner
    from prefect_dask import DaskTaskRunner

    runners: dict[str, Any] = {
        "Dask": DaskTaskRunner,
        "Sequential": SequentialTaskRunner,
        "Concurrent": ConcurrentTaskRunner,
    }

    return runners[runner]


def prefect_workflow(workflow: Workflow) -> Any:
    """Convert a workflow into a Prefect flow."""
    from prefect import Flow, Task

    def prefect_wrapper() -> Any:
        """Function to execute all tasks in the workflow."""
        for task in workflow.tasks:
            # TODO: add subflows
            prefect_task = Task(task.payload, name=task.name)
            prefect_task()

    return Flow(
        prefect_wrapper,
        name="config name",
        flow_run_name="fasthep-flow",
        version="0.0.1",
    )


# def gitlab_ci_workflow(workflow: Workflow):
#     """Convert a workflow into a GitLab CI workflow."""

# def github_actions_workflow(workflow: Workflow):
#     """Convert a workflow into a GitHub Actions workflow."""

# def coffea_workflow(workflow: Workflow):
#     """Convert a workflow into a coffea processor."""

# def dask_workflow(workflow: Workflow):
#     """Convert a workflow into a dask graph."""

# def luigi_workflow(workflow: Workflow):
#     """Convert a workflow into a luigi graph."""

# def parsl_workflow(workflow: Workflow):
#     """Convert a workflow into a parsl graph."""

# def networkx_workflow(workflow: Workflow):
#     """Convert a workflow into a networkx directed graph."""
