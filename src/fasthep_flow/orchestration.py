"""Orchestration module for the fasthep-flow package.
This module contains functions to convert a fasthep-flow workflow into various other workflows,
e.g. Prefect, NetworkX, etc., to either execute them or visualize them.
"""

from __future__ import annotations

import logging
from typing import Any

from hamilton import driver, telemetry

from .workflow import Workflow, load_tasks_module

telemetry.disable_telemetry()  # we don't want to send telemetry data for this example

logger = logging.getLogger(__name__)

# def get_runner(runner: str) -> Any:
#     """Get the task runner for the given name."""
#     from prefect.task_runners import ConcurrentTaskRunner, SequentialTaskRunner
#     from prefect_dask import DaskTaskRunner

#     runners: dict[str, Any] = {
#         "Dask": DaskTaskRunner,
#         "Sequential": SequentialTaskRunner,
#         "Concurrent": ConcurrentTaskRunner,
#     }

#     return runners[runner]


# def create_dask_cluster() -> Any:
#     cluster = LocalCluster()
#     client = Client(cluster)
#     logger.info(client.cluster)
#     logging.basicConfig(stream=sys.stdout, level=logging.INFO)

#     return client


def workflow_to_hamilton_dag(workflow: Workflow) -> Any:
    """Convert a workflow into a Hamilton flow."""
    task_functions = load_tasks_module(workflow)
    return driver.Builder().with_modules(task_functions).with_cache().build()


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
