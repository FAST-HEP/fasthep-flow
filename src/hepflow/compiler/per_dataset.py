"""Opt-in per-dataset mode: dataset selection, metadata stubs and safety rules.

In per-dataset mode a job selects one dataset of a multi-dataset workflow. Partition
numbers are 1-based and local to the dataset, and the file data of every other dataset is
not resolved: those datasets stay in the workflow as metadata stubs so the plan still sees
every dataset record.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from copy import deepcopy
from typing import Any

from hepflow.compiler.output_claims import _sink_when
from hepflow.model.plan import ExecutionPlan

#: Keys a non-selected dataset keeps; everything else is file data.
STUB_KEYS = ("name", "group", "eventtype", "meta")


def per_dataset_enabled(
    execution: Mapping[str, Any] | None, override: bool | None = None
) -> bool:
    """The mode of a workflow: the override wins over ``execution.partitioning``."""
    if override is not None:
        return bool(override)
    partitioning = (execution or {}).get("partitioning") or {}
    return bool(partitioning.get("per_dataset"))


def with_per_dataset(execution: dict[str, Any], enabled: bool) -> dict[str, Any]:
    """Copy of ``execution`` whose ``partitioning.per_dataset`` records ``enabled``."""
    result = deepcopy(execution)
    partitioning = dict(result.get("partitioning") or {})
    if enabled:
        partitioning["per_dataset"] = True
    else:
        partitioning.pop("per_dataset", None)
    result["partitioning"] = partitioning
    return result


def validate_dataset_selection(
    dataset: str | None, *, enabled: bool, names: Iterable[str]
) -> None:
    """Reject ``--dataset`` outside per-dataset mode and names the workflow lacks."""
    if dataset is None:
        return
    if not enabled:
        raise ValueError(
            "workflow is not per-dataset; set `execution.partitioning.per_dataset` "
            "or pass `--per-dataset`"
        )
    available = [str(name) for name in names]
    if dataset not in available:
        raise ValueError(
            f"Unknown dataset {dataset!r}; available datasets: {', '.join(available)}."
        )


def stub_unselected_datasets(
    normalized: Mapping[str, Any], dataset: str
) -> dict[str, Any]:
    """Copy of a normalized workflow in which only ``dataset`` keeps its file data."""
    result = dict(normalized)
    data = dict(result.get("data") or {})
    data["datasets"] = [
        deepcopy(record)
        if record.get("name") == dataset
        else {key: deepcopy(record[key]) for key in STUB_KEYS if key in record}
        for record in list(data.get("datasets") or [])
    ]
    result["data"] = data
    return result


def validate_per_dataset_plan(plan: ExecutionPlan) -> None:
    """Reject run-level work, which concurrent per-partition jobs cannot share."""
    for node in plan.nodes:
        if node.role == "observer":
            raise ValueError(
                f"per-dataset mode does not support observer node {node.id!r}: "
                "aggregation belongs to the merge step"
            )
        if node.role == "sink" and _sink_when(node) == "run_end":
            raise ValueError(
                f"per-dataset mode does not support the run_end sink {node.id!r}: "
                "aggregation belongs to the merge step"
            )
