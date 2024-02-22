from __future__ import annotations

from typing import Any

from .config import FlowConfig


class Workflow:
    """Wrapper for any compute graph implementation we want to support. Currently using Prefect."""

    tasks: list[
        Any
    ]  # this should be prefect.Task, but that's not working with pydantic v2 for now

    def __init__(self, config: FlowConfig) -> None:
        from prefect import Task

        stages = config.stages
        self.tasks = []
        for stage in stages:
            self.tasks.append(
                Task(
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
        from prefect import Flow

        f = Flow(
            self.__call__,
            name="config name",
            flow_run_name="fasthep-flow",
            version="0.0.1",
        )
        f()
