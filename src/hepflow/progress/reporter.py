from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from threading import Lock
from time import monotonic
from typing import Any
from uuid import uuid4

from hepflow.model.plan import ExecutionPartition
from hepflow.progress.model import (
    PartitionState,
    ProgressCounts,
    ProgressEvent,
    ProgressEventKind,
    ProgressSnapshot,
    ProgressUpdate,
    RunState,
)
from hepflow.progress.sink import BufferedProgressSink, ProgressSink

_TERMINAL_STATES: set[PartitionState] = {"completed", "failed"}
_ALLOWED_TRANSITIONS: dict[PartitionState, set[PartitionState]] = {
    "pending": {"pending", "running", "completed", "failed"},
    "running": {"pending", "running", "completed", "failed"},
    "completed": {"completed"},
    "failed": {"failed"},
}


@dataclass(slots=True)
class _PartitionProgress:
    id: str
    dataset: str | None
    state: PartitionState = "pending"
    attempt: int = 0
    entered_monotonic: float = 0.0


class ProgressReporter:
    def __init__(
        self,
        partitions: Iterable[ExecutionPartition | dict[str, Any] | str] = (),
        *,
        run_id: str | None = None,
        sinks: Iterable[ProgressSink] = (),
        wall_clock: Callable[[], datetime] | None = None,
        monotonic_clock: Callable[[], float] | None = None,
    ) -> None:
        self.run_id = run_id or str(uuid4())
        self._wall_clock = wall_clock or (lambda: datetime.now(UTC))
        self._monotonic_clock = monotonic_clock or monotonic
        now = self._monotonic_clock()
        self._run_started_monotonic = now
        self._sequence = 0
        self._phase: str | None = None
        self._run_state: RunState = "pending"
        self._lock = Lock()
        self._partitions = {
            item.id: _PartitionProgress(
                id=item.id,
                dataset=item.dataset,
                entered_monotonic=now,
            )
            for item in (_partition_like(partition) for partition in partitions)
        }
        self._sinks = [
            sink if isinstance(sink, BufferedProgressSink) else BufferedProgressSink(sink)
            for sink in sinks
        ]

    @property
    def counts(self) -> ProgressCounts:
        with self._lock:
            return self._counts()

    @property
    def warnings(self) -> list[dict[str, str]]:
        warnings: list[dict[str, str]] = []
        for sink in self._sinks:
            warnings.extend(item.to_dict() for item in sink.warnings)
        return warnings

    def run_started(self, *, detail: dict[str, Any] | None = None) -> ProgressUpdate:
        with self._lock:
            now = self._monotonic_clock()
            self._run_started_monotonic = now
            for partition in self._partitions.values():
                if partition.state == "pending":
                    partition.entered_monotonic = now
            self._run_state = "running"
            update = self._update("run_started", detail=detail, now_monotonic=now)
        self._dispatch(update)
        return update

    def phase_started(
        self,
        phase: str,
        *,
        detail: dict[str, Any] | None = None,
    ) -> ProgressUpdate:
        phase = _phase(phase)
        with self._lock:
            self._phase = phase
            update = self._update("phase_started", phase=phase, detail=detail)
        self._dispatch(update)
        return update

    def phase_completed(
        self,
        phase: str,
        *,
        detail: dict[str, Any] | None = None,
    ) -> ProgressUpdate:
        phase = _phase(phase)
        with self._lock:
            update = self._update("phase_completed", phase=phase, detail=detail)
            if self._phase == phase:
                self._phase = None
        self._dispatch(update)
        return update

    def transition(
        self,
        partition_id: str,
        to_state: PartitionState,
        *,
        detail: dict[str, Any] | None = None,
    ) -> ProgressUpdate | None:
        with self._lock:
            partition = self._partition(partition_id)
            from_state = partition.state
            if to_state == from_state:
                return None
            if to_state not in _ALLOWED_TRANSITIONS[from_state]:
                raise ValueError(
                    f"Invalid partition state transition for {partition_id!r}: "
                    f"{from_state} -> {to_state}"
                )
            now = self._monotonic_clock()
            state_duration = now - partition.entered_monotonic
            partition.state = to_state
            partition.entered_monotonic = now
            if to_state == "running":
                partition.attempt += 1
            update = self._update(
                "partition_state",
                partition_id=partition.id,
                dataset=partition.dataset,
                from_state=from_state,
                to_state=to_state,
                attempt=partition.attempt,
                state_duration_seconds=state_duration,
                detail=detail,
                now_monotonic=now,
            )
        self._dispatch(update)
        return update

    def pending(self, partition_id: str, *, detail: dict[str, Any] | None = None):
        return self.transition(partition_id, "pending", detail=detail)

    def running(self, partition_id: str, *, detail: dict[str, Any] | None = None):
        return self.transition(partition_id, "running", detail=detail)

    def completed(self, partition_id: str, *, detail: dict[str, Any] | None = None):
        return self.transition(partition_id, "completed", detail=detail)

    def failed(self, partition_id: str, *, detail: dict[str, Any] | None = None):
        return self.transition(partition_id, "failed", detail=detail)

    def run_completed(self, *, detail: dict[str, Any] | None = None) -> ProgressUpdate:
        with self._lock:
            self._run_state = "completed"
            update = self._update("run_completed", detail=detail)
        self._dispatch(update)
        return update

    def run_failed(self, exc: BaseException | None = None) -> ProgressUpdate:
        detail = {}
        if exc is not None:
            detail = {"exception_type": type(exc).__name__, "message": str(exc)}
        with self._lock:
            self._run_state = "failed"
            update = self._update("run_failed", detail=detail)
        self._dispatch(update)
        return update

    def close(self, *, timeout: float = 2.0) -> list[dict[str, str]]:
        for sink in self._sinks:
            sink.close(timeout=timeout)
        return self.warnings

    def _partition(self, partition_id: str) -> _PartitionProgress:
        try:
            return self._partitions[partition_id]
        except KeyError as exc:
            raise KeyError(f"Unknown progress partition id: {partition_id}") from exc

    def _update(
        self,
        kind: ProgressEventKind,
        *,
        partition_id: str | None = None,
        dataset: str | None = None,
        from_state: PartitionState | None = None,
        to_state: PartitionState | None = None,
        attempt: int | None = None,
        state_duration_seconds: float | None = None,
        phase: str | None = None,
        detail: dict[str, Any] | None = None,
        now_monotonic: float | None = None,
    ) -> ProgressUpdate:
        now_monotonic = self._monotonic_clock() if now_monotonic is None else now_monotonic
        elapsed = now_monotonic - self._run_started_monotonic
        self._sequence += 1
        event = ProgressEvent(
            run_id=self.run_id,
            sequence=self._sequence,
            kind=kind,
            timestamp=self._wall_clock().astimezone(UTC),
            elapsed_seconds=elapsed,
            partition_id=partition_id,
            dataset=dataset,
            from_state=from_state,
            to_state=to_state,
            attempt=attempt,
            state_duration_seconds=state_duration_seconds,
            phase=phase,
            detail=detail or {},
        )
        snapshot = ProgressSnapshot(
            run_id=self.run_id,
            run_state=self._run_state,
            phase=self._phase,
            counts=self._counts(),
            elapsed_seconds=elapsed,
        )
        return ProgressUpdate(event=event, snapshot=snapshot)

    def _counts(self) -> ProgressCounts:
        states = [partition.state for partition in self._partitions.values()]
        return ProgressCounts(
            total=len(states),
            pending=states.count("pending"),
            running=states.count("running"),
            completed=states.count("completed"),
            failed=states.count("failed"),
        )

    def _dispatch(self, update: ProgressUpdate) -> None:
        for sink in self._sinks:
            sink.enqueue(update)


def _partition_like(value: ExecutionPartition | dict[str, Any] | str) -> _PartitionProgress:
    if isinstance(value, ExecutionPartition):
        return _PartitionProgress(id=value.id, dataset=value.dataset)
    if isinstance(value, dict):
        return _PartitionProgress(
            id=str(value["id"]),
            dataset=str(value["dataset"]) if value.get("dataset") is not None else None,
        )
    return _PartitionProgress(id=str(value), dataset=None)


def _phase(value: str) -> str:
    phase = str(value).strip()
    if not phase:
        raise ValueError("Progress phase must be a non-empty string")
    return phase
