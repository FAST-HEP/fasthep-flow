from __future__ import annotations

from prefect import flow, task

from .config import FlowConfig


class Workflow:
    """Wrapper for any compute graph implementation we want to support. Currently using Prefect."""

    tasks: list[task]

    def __init__(self, config: FlowConfig) -> None:
        stages = config.stages
        self.tasks = []
        for stage in stages:
            self.tasks.append(
                task(
                    stage.resolve(),
                    name=stage.name,
                )
            )

    def __call__(self) -> None:
        """Function to execute all tasks in the workflow."""
        for t in self.tasks:
            t()

    def run(self) -> None:
        """Function to execute the workflow. Wraps __call__ to convert the workflow into a Prefect flow."""
        f = flow(
            self.__call__,
            name="config name",
            flow_run_name="fasthep-flow",
            version="0.0.1",
        )
        f()
