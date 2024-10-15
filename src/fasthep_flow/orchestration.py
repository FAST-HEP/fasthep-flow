"""Orchestration module for the fasthep-flow package.
This module contains functions to convert a fasthep-flow workflow into various other workflows,
e.g. Prefect, NetworkX, etc., to either execute them or visualize them.
"""

from __future__ import annotations

import logging
from functools import partial
from pathlib import Path
from typing import Any

from dask.distributed import Client, LocalCluster
from hamilton import base, driver, telemetry

from .workflow import Workflow, load_tasks_module

telemetry.disable_telemetry()  # we don't want to send telemetry data for this example

logger = logging.getLogger(__name__)


def create_dask_cluster() -> Any:
    cluster = LocalCluster()
    client = Client(cluster)
    logger.info(client.cluster)

    return client


DASK_CLIENTS = {
    "local": create_dask_cluster,
}


def create_dask_adapter(client_type: str) -> Any:
    from hamilton.plugins import h_dask

    client = DASK_CLIENTS[client_type]()

    return h_dask.DaskGraphAdapter(
        client,
        base.DictResult(),
        visualize_kwargs={"filename": "run_with_delayed", "format": "png"},
        use_delayed=True,
        compute_at_end=True,
    )


def create_local_adapter() -> Any:
    return base.SimplePythonGraphAdapter(base.DictResult())


PRECONFIGURED_ADAPTERS = {
    "dask:local": partial(create_dask_adapter, client_type="local"),
    "local": create_local_adapter,
}


def workflow_to_hamilton_dag(
    workflow: Workflow,
    output_path: str,
    # method: str = "local"
) -> Any:
    """Convert a workflow into a Hamilton flow."""
    task_functions = load_tasks_module(workflow)
    # adapter = PRECONFIGURED_ADAPTERS[method]()
    cache_dir = Path(output_path) / ".hamilton_cache"

    return driver.Builder().with_modules(task_functions).with_cache(cache_dir).build()


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
