"""Workflow and Task classes to define and execute a compute graph."""

from __future__ import annotations

import importlib
import importlib.machinery
import importlib.util
import inspect
import pickle
import shutil
import string
import sys
from dataclasses import dataclass, field
from pathlib import Path
from types import ModuleType
from typing import Any

import dill

from .config import FlowConfig
from .templates import template_environment
from .utils import generate_save_path

REPLACE_DICT = {ord(c): "_" for c in string.whitespace + string.punctuation}
REPLACE_TABLE = str.maketrans(REPLACE_DICT)


@dataclass
class Task:
    """Wrapper for any compute task implementation we want to support."""

    name: str
    type: str
    kwargs: dict[str, Any]
    payload: Any
    needs: list[str] = field(default_factory=list)

    def __call__(self, data: dict[str, Any]) -> Any:
        return self.payload(data=data, **self.kwargs)

    @property
    def safe_name(self) -> str:
        """Return a safe name for the task."""
        return self.name.translate(REPLACE_TABLE)

    @property
    def __name__(self) -> str:
        return self.safe_name

    @property
    def safe_needs(self) -> list[str]:
        """Return a safe name for the task."""
        return [name.translate(REPLACE_TABLE) for name in self.needs]


def get_task_source(obj: Any) -> str:
    """Retrieve the source code of a task object and return a function definition."""
    # Capture the object definition
    obj_attrs = {}

    for attr_name, attr_value in inspect.getmembers(obj):
        if not attr_name.startswith("__") and not inspect.isroutine(attr_value):
            obj_attrs[attr_name] = attr_value

    # Return the object definition as source code string
    task_base_source = dill.source.getsource(Task)
    task_source = dill.source.getsource(obj)
    return str(task_source.replace(task_base_source, "").strip())


@dataclass
class Workflow:
    """Wrapper for any compute graph implementation we want to support."""

    # config: FlowConfig
    metadata: dict[str, Any] = field(default_factory=dict[str, Any])
    tasks: list[Task] = field(default_factory=list)
    task_names: list[str] = field(default_factory=list)
    name: str = "fasthep-flow"
    save_path: str = "~/.fasthep/flow/"

    def __call__(self) -> Any:
        """Function to execute all tasks in the workflow."""
        for task in self.tasks:
            yield task.payload()

    def run(self) -> Any:
        """Function to execute all tasks in the workflow."""
        return list(self())

    def _save_tasks(self, task_file: Path) -> None:
        imports = {}
        task_definitions = {}
        for task in self.tasks:
            task_definitions[task.safe_name] = (get_task_source(task), task.safe_needs)
            module_path, class_name = task.type.rsplit(".", 1)
            imports[module_path] = class_name
        templates = template_environment()
        with task_file.open("w") as f:
            template = templates.get_template("hamilton/task_source.py.j2")
            content = template.render(
                dynamic_imports=imports,
                task_sources=task_definitions,
                enable_cache=False,
                task_cache_format="parquet",
                config_file=self.metadata["config_file"],
            )
            f.write(content)

    def save(self, base_path: Path = Path("~/.fasthep/flow")) -> str:
        """
        Save the workflow to a file.
        Automatic path is ~/.fasthep/flow/{workflow_name}/{datetime}/{config_hash}/

        Returns the save path.
        """
        base_path = Path(base_path).expanduser().resolve()

        config_file = Path(self.metadata["config_file"])
        path = generate_save_path(base_path, self.name, config_file)
        path.mkdir(parents=True, exist_ok=True)
        # TODO: check if things exist and skip if they do
        # copy the config file to the path
        shutil.copy(config_file, path / config_file.name)
        # save the workflow to the path
        workflow_file = path / "workflow.pkl"
        if not workflow_file.exists():
            with workflow_file.open("wb") as f:
                pickle.dump(self, f)
        # store the python code for each task in tasks.py
        task_file = path / "tasks.py"
        if not task_file.exists():
            self._save_tasks(task_file)
        self.save_path = str(path)
        # TODO: save external modules
        return self.save_path

    @staticmethod
    def load(path: Path | str) -> Workflow:
        """
        Load a workflow from a file.
        @param path: Path to the directory containing the workflow file.
        """
        path = Path(path)
        workflow_file = path / "workflow.pkl"
        with workflow_file.open("rb") as f:
            workflow: Workflow = pickle.load(f)
        workflow.save_path = str(path)
        return workflow


def load_tasks_module(workflow: Workflow) -> ModuleType:
    """Load tasks from a tasks.py file in the workflow save path."""
    task_location = workflow.save_path
    task_spec = importlib.machinery.PathFinder().find_spec("tasks", [task_location])
    if task_spec is None:
        msg = f"No tasks module found in {task_location}"
        raise FileNotFoundError(msg)
    task_functions = importlib.util.module_from_spec(task_spec)
    if task_spec.loader is None:
        msg = f"Loader is None for {task_spec}"
        raise ValueError(msg)
    task_spec.loader.exec_module(task_functions)
    sys.modules["tasks"] = task_functions
    return task_functions


def create_workflow(config: FlowConfig) -> Workflow:
    """Create a workflow from a configuration."""
    name = config.metadata.get("name", Workflow.name)
    tasks = []
    for task in config.tasks:
        # TODO: set ouput_path for each task
        tasks.append(
            Task(
                name=task.name,
                type=task.type,
                kwargs=task.kwargs if task.kwargs is not None else {},
                payload=task.resolve() if hasattr(task, "resolve") else None,
                needs=task.needs if task.needs else [],
                # TODO: pass information about the task's dependencies and execution environment
            )
        )
    task_names = [task.safe_name for task in tasks]

    return Workflow(
        metadata=config.metadata,
        tasks=tasks,
        task_names=task_names,
        name=name,
    )
