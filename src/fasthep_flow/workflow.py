"""Workflow and Task classes to define and execute a compute graph."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .config import FlowConfig


@dataclass
class Task:
    """Wrapper for any compute task implementation we want to support."""

    name: str
    type: str
    kwargs: dict[str, Any]
    payload: Any

    def __call__(self) -> Any:
        return self.payload()


@dataclass
class Workflow:
    """Wrapper for any compute graph implementation we want to support."""

    tasks: list[Task]  # TODO: Maybe this should be an OrderedDict

    def __init__(self, config: FlowConfig) -> None:
        stages = config.stages
        self.tasks = []
        for stage in stages:
            self.tasks.append(
                Task(
                    name=stage.name,
                    type=stage.type,
                    kwargs=stage.kwargs if stage.kwargs is not None else {},
                    payload=stage.resolve() if hasattr(stage, "resolve") else None,
                    # TODO: pass information about the task's dependencies and execution environment
                )
            )

    def __call__(self) -> Any:
        """Function to execute all tasks in the workflow."""
        for task in self.tasks:
            yield task.payload()

    def run(self) -> Any:
        """Function to execute all tasks in the workflow."""
        return list(self())
