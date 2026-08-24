from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from types import MappingProxyType
from typing import Any, Literal

PartitionState = Literal["pending", "running", "completed", "failed"]
RunState = Literal["pending", "running", "completed", "failed"]
ProgressEventKind = Literal[
    "run_started",
    "phase_started",
    "phase_completed",
    "partition_state",
    "run_completed",
    "run_failed",
]

SCHEMA_VERSION = 1


@dataclass(frozen=True, slots=True)
class ProgressCounts:
    total: int
    pending: int
    running: int
    completed: int
    failed: int

    def __post_init__(self) -> None:
        if self.total != self.pending + self.running + self.completed + self.failed:
            raise ValueError("ProgressCounts invariant violated")
        if min(self.total, self.pending, self.running, self.completed, self.failed) < 0:
            raise ValueError("ProgressCounts values must be non-negative")

    @property
    def finished(self) -> int:
        return self.completed + self.failed

    def to_dict(self) -> dict[str, int]:
        return {
            "total": self.total,
            "pending": self.pending,
            "running": self.running,
            "completed": self.completed,
            "failed": self.failed,
            "finished": self.finished,
        }


@dataclass(frozen=True, slots=True)
class ProgressEvent:
    run_id: str
    sequence: int
    kind: ProgressEventKind
    timestamp: datetime
    elapsed_seconds: float
    partition_id: str | None = None
    dataset: str | None = None
    from_state: PartitionState | None = None
    to_state: PartitionState | None = None
    attempt: int | None = None
    state_duration_seconds: float | None = None
    phase: str | None = None
    detail: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.timestamp.tzinfo is None:
            raise ValueError("ProgressEvent timestamp must be timezone-aware")
        object.__setattr__(self, "detail", MappingProxyType(dict(self.detail)))

    def to_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "run_id": self.run_id,
            "sequence": self.sequence,
            "kind": self.kind,
            "timestamp": _format_utc(self.timestamp),
            "elapsed_seconds": self.elapsed_seconds,
        }
        for key in (
            "partition_id",
            "dataset",
            "from_state",
            "to_state",
            "attempt",
            "state_duration_seconds",
            "phase",
        ):
            value = getattr(self, key)
            if value is not None:
                data[key] = value
        if self.detail:
            data["detail"] = dict(self.detail)
        return data


@dataclass(frozen=True, slots=True)
class ProgressSnapshot:
    run_id: str
    run_state: RunState
    phase: str | None
    counts: ProgressCounts
    elapsed_seconds: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "run_id": self.run_id,
            "run_state": self.run_state,
            "phase": self.phase,
            "counts": self.counts.to_dict(),
            "elapsed_seconds": self.elapsed_seconds,
        }


@dataclass(frozen=True, slots=True)
class ProgressUpdate:
    event: ProgressEvent
    snapshot: ProgressSnapshot

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "event": self.event.to_dict(),
            "snapshot": self.snapshot.to_dict(),
        }


def _format_utc(value: datetime) -> str:
    utc = value.astimezone(UTC)
    return utc.isoformat(timespec="microseconds").replace("+00:00", "Z")
