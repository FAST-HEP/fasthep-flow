from __future__ import annotations

from collections.abc import Mapping
from typing import Any


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
