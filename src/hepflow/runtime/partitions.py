from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from hepflow.compiler.per_dataset import validate_dataset_selection
from hepflow.model.plan import ExecutionPartition


def select_partitions(
    partitions: Sequence[ExecutionPartition],
    *,
    numbers: Iterable[int] | None = None,
    dataset: str | None = None,
    chunk_size: int | None = None,
) -> list[ExecutionPartition]:
    if numbers is None:
        return list(partitions)

    requested = list(numbers)
    if not requested:
        raise ValueError("At least one partition number must be requested.")

    total = len(partitions)
    unique_numbers = set()
    for number in requested:
        if isinstance(number, bool) or not isinstance(number, int):
            raise TypeError(
                f"Partition numbers must be integers, got {type(number).__name__}."
            )
        if number <= 0:
            raise ValueError(
                f"Partition numbers are 1-based; requested partition {number}."
            )
        if number > total and dataset is not None:
            raise ValueError(
                f"Requested partition {number}, but dataset {dataset!r} "
                f"(chunk size {chunk_size}) has {total} partitions; "
                f"valid range 1..{total}."
            )
        if number > total:
            raise ValueError(
                f"Requested partition {number}, but this plan contains "
                f"{total} partitions."
            )
        unique_numbers.add(number)

    return [
        partition
        for index, partition in enumerate(partitions, start=1)
        if index in unique_numbers
    ]


def select_dataset_partitions(
    partitions: Sequence[ExecutionPartition],
    *,
    context: Mapping[str, Any],
    per_dataset: bool,
    dataset: str | None,
    chunk_size: int | None,
    numbers: Iterable[int] | None,
) -> list[ExecutionPartition]:
    """Restrict a plan's partitions to one dataset; numbers are then local to it.

    ``chunk_size`` is the caller's expectation of the chunk size the plan was compiled
    with; the plan is not re-chunked, so a different value is an error.
    """
    validate_dataset_selection(
        dataset,
        enabled=per_dataset,
        names=list(context.get("dataset_names") or []),
    )
    if not per_dataset:
        return select_partitions(partitions, numbers=numbers)

    datasets = list(dict.fromkeys(p.dataset for p in partitions))
    if dataset is None and numbers is not None and len(datasets) > 1:
        raise ValueError(
            "Partition numbers are ambiguous on a workflow with several datasets "
            f"({', '.join(datasets)}); pass --dataset."
        )
    chunk_sizes = dict(context.get("chunk_sizes") or {})
    if chunk_size is not None:
        for checked in [dataset] if dataset is not None else datasets:
            compiled = chunk_sizes.get(checked)
            if compiled != chunk_size:
                raise ValueError(
                    f"Dataset {checked!r} was compiled with chunk size {compiled}, "
                    f"not {chunk_size}; recompile into a new out-dir."
                )
    selected = [
        partition
        for partition in partitions
        if dataset is None or partition.dataset == dataset
    ]
    name = dataset or (datasets[0] if len(datasets) == 1 else None)
    return select_partitions(
        selected,
        numbers=numbers,
        dataset=name,
        chunk_size=chunk_sizes.get(name) if name is not None else None,
    )


def describe_partition_selection(
    partitions: Sequence[ExecutionPartition],
    *,
    selected_partitions: Sequence[ExecutionPartition],
    numbers: Iterable[int],
) -> dict[str, object]:
    selected_ids = {partition.id for partition in selected_partitions}
    return {
        "requested": sorted(set(numbers)),
        "total_partitions": len(partitions),
        "selected_partitions": len(selected_partitions),
        "partitions": [
            {
                "number": index,
                "id": partition.id,
                "dataset": partition.dataset,
            }
            for index, partition in enumerate(partitions, start=1)
            if partition.id in selected_ids
        ],
    }
