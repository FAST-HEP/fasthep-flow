from __future__ import annotations

from collections.abc import Iterable, Sequence

from hepflow.model.plan import ExecutionPartition


def select_partitions(
    partitions: Sequence[ExecutionPartition],
    *,
    numbers: Iterable[int] | None = None,
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
