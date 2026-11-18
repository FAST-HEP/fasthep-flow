from __future__ import annotations

import warnings
from collections.abc import Iterable, Mapping
from dataclasses import replace
from typing import Any

from hepflow.model.plan import ExecutionPartition


def resolve_chunk_size(
    cli_chunk_size: int | None,
    dataset: Mapping[str, Any],
    execution: Mapping[str, Any],
) -> int | None:
    """Resolve CLI, dataset and workflow chunk sizes in precedence order."""
    partitioning = execution.get("partitioning") or {}
    workflow_chunk_size = (
        partitioning.get("chunk_size") if isinstance(partitioning, Mapping) else None
    )
    for value in (cli_chunk_size, dataset.get("chunk_size"), workflow_chunk_size):
        if value is None:
            continue
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError("chunk size must be a positive integer")
        return value
    return None


def derive_dataset_partitions(
    dataset: Mapping[str, Any],
    *,
    source_name: str,
    chunk_size: int | None,
    local_numbers: bool = False,
) -> list[ExecutionPartition]:
    """Derive one dataset's partitions in file order, then chunk order.

    With ``local_numbers`` (per-dataset mode) ``part`` is the 1-based position inside the
    dataset; otherwise it is ``<file_index>_<chunk_index>``.
    """
    dataset_name = str(dataset.get("name") or "")
    files = list(dataset.get("files") or [])
    if not files:
        raise ValueError(
            f"Source {source_name!r} dataset {dataset_name!r} has no files"
        )

    file_counts = _file_event_counts(dataset, len(files))
    if chunk_size is not None and file_counts is None:
        _require_file_counts_for_chunking(dataset_name, dataset, files)
        warnings.warn(
            f"Dataset {dataset_name!r} has no per-file event counts; "
            "using one whole-file partition per file.",
            UserWarning,
            stacklevel=2,
        )

    partitions: list[ExecutionPartition] = []
    for file_index, file_path in enumerate(files):
        nevents = file_counts[file_index] if file_counts is not None else None
        if chunk_size is not None and nevents is not None:
            for chunk_index, start in enumerate(range(0, nevents, chunk_size)):
                stop = min(start + chunk_size, nevents)
                partitions.append(
                    ExecutionPartition(
                        id=(
                            f"{source_name}__{dataset_name}__"
                            f"{file_index}_{chunk_index}"
                        ),
                        dataset=dataset_name,
                        file=str(file_path),
                        source=source_name,
                        part=f"{file_index}_{chunk_index}",
                        start=start,
                        stop=stop,
                    )
                )
            continue

        partitions.append(
            ExecutionPartition(
                id=f"{source_name}__{dataset_name}__{file_index}",
                dataset=dataset_name,
                file=str(file_path),
                source=source_name,
                part=f"{file_index}_0",
                start=0 if nevents is not None else None,
                stop=nevents,
            )
        )
    if local_numbers:
        partitions = [
            replace(
                partition,
                id=f"{source_name}__{dataset_name}__{number}",
                part=str(number),
            )
            for number, partition in enumerate(partitions, start=1)
        ]
    return partitions


def uncounted_file_count(dataset: Mapping[str, Any]) -> int:
    files = list(dataset.get("files") or [])
    return len(files) if _file_event_counts(dataset, len(files)) is None else 0


def list_dataset_partitions(
    datasets: Iterable[Mapping[str, Any]],
    *,
    execution: Mapping[str, Any],
    dataset: str | None = None,
    chunk_size: int | None = None,
) -> list[dict[str, object]]:
    """Build partition summaries for normalized workflow datasets."""
    dataset_records = list(datasets)
    names = [str(item.get("name")) for item in dataset_records]
    if dataset is not None and dataset not in names:
        available = ", ".join(names)
        raise ValueError(
            f"Unknown dataset {dataset!r}; available datasets: {available}."
        )

    result: list[dict[str, object]] = []
    for item in dataset_records:
        dataset_record = dict(item)
        dataset_name = str(dataset_record.get("name"))
        if dataset is not None and dataset_name != dataset:
            continue
        effective_chunk_size = resolve_chunk_size(
            chunk_size, dataset_record, execution
        )
        partitions = derive_dataset_partitions(
            dataset_record,
            source_name="events",
            chunk_size=effective_chunk_size,
        )
        result.append(
            {
                "dataset": dataset_name,
                "chunk_size": effective_chunk_size,
                "n_partitions": len(partitions),
                "n_files": len(list(dataset_record.get("files") or [])),
                "n_uncounted": uncounted_file_count(dataset_record),
            }
        )
    return result


def _file_event_counts(
    dataset: Mapping[str, Any], n_files: int
) -> list[int | None] | None:
    per_file = dataset.get("file_nevents")
    if per_file is not None:
        return [int(n) for n in per_file]
    total = _coerce_nevents(dataset.get("nevents"))
    if total is not None and n_files == 1:
        return [total]
    return None


def _require_file_counts_for_chunking(
    dataset_name: str, dataset: Mapping[str, Any], files: list[Any]
) -> None:
    if len(files) > 1 and _coerce_nevents(dataset.get("nevents")) is not None:
        raise ValueError(
            f"Dataset {dataset_name!r} has {len(files)} files and only a dataset-level "
            "'nevents' total, which cannot be used to chunk individual files. "
            "Add 'file_nevents' (one event count per file, in the order of 'files')."
        )


def _coerce_nevents(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
    return int(value)
