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
            pt = Task(task.payload, name=task.name)
            pt()

    return Flow(
        prefect_wrapper,
        name="config name",
        flow_run_name="fasthep-flow",
        version="0.0.1",
    )


# def networkx_workflow(workflow: Workflow):
#     """Convert a workflow into a networkx directed graph."""
