from __future__ import annotations

import json
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pytest
import yaml

from hepflow.api import compile_workflow_file, run_plan_file
from hepflow.model.plan import ExecutionPartition
from hepflow.progress import PartitionState, ProgressReporter, ProgressUpdate


def test_reporter_state_machine_timing_and_serialization() -> None:
    clock = _Clock()
    sink = _CollectSink()
    reporter = ProgressReporter(
        [_partition("a")],
        run_id="run-1",
        sinks=[sink],
        wall_clock=clock.wall,
        monotonic_clock=clock.monotonic,
    )

    started = reporter.run_started()
    clock.advance(monotonic=10.0, wall=timedelta(days=1))
    running = reporter.running("a")
    clock.advance(monotonic=5.0, wall=timedelta(seconds=-60))
    completed = reporter.completed("a")
    reporter.run_completed()
    reporter.close()

    assert started.event.timestamp.tzinfo is UTC
    assert started.event.elapsed_seconds == 0.0
    assert running is not None
    assert completed is not None
    assert running.event.elapsed_seconds == 10.0
    assert running.event.state_duration_seconds == 10.0
    assert running.event.attempt == 1
    assert completed.event.elapsed_seconds == 15.0
    assert completed.event.state_duration_seconds == 5.0
    assert completed.snapshot.counts.completed == 1
    payload = completed.to_dict()
    assert payload["schema_version"] == 1
    assert payload["event"]["timestamp"].endswith("Z")
    assert payload["event"]["sequence"] == completed.event.sequence
    json.dumps(payload)
    _assert_counts_invariant(completed)


def test_reporter_requeue_attempts_and_idempotent_same_state() -> None:
    reporter = ProgressReporter(["p"])
    reporter.run_started()

    first = reporter.running("p")
    assert reporter.running("p") is None
    reporter.pending("p", detail={"reason": "preempted"})
    second = reporter.running("p")
    reporter.completed("p")

    assert first is not None
    assert second is not None
    assert first.event.attempt == 1
    assert second.event.attempt == 2
    assert reporter.counts.completed == 1
    reporter.close()


@pytest.mark.parametrize("terminal", ["completed", "failed"])
def test_reporter_rejects_transitions_out_of_terminal(terminal: str) -> None:
    reporter = ProgressReporter(["p"])
    reporter.transition("p", cast(PartitionState, terminal))

    with pytest.raises(ValueError, match="Invalid partition state transition"):
        reporter.running("p")

    reporter.close()


def test_reporter_rejects_unknown_partition() -> None:
    reporter = ProgressReporter(["known"])

    with pytest.raises(KeyError, match="Unknown progress partition id"):
        reporter.running("missing")

    reporter.close()


def test_reporter_nonblocking_and_sink_failure_isolated() -> None:
    slow = _SlowSink(delay=0.25)
    fast = _CollectSink()
    failing = _FailingSink()
    reporter = ProgressReporter(["p"], sinks=[slow, fast, failing])
    reporter.run_started()

    start = time.monotonic()
    reporter.running("p")
    duration = time.monotonic() - start

    assert duration < 0.1
    deadline = time.monotonic() + 2
    while len(fast.updates) < 2 and time.monotonic() < deadline:
        time.sleep(0.01)
    warnings = reporter.close()
    assert len(fast.updates) >= 2
    assert any("progress sink failed" in item["message"] for item in warnings)


def test_reporter_concurrent_transitions_have_unique_sequences() -> None:
    sink = _CollectSink()
    partitions = [_partition(f"p{i}") for i in range(20)]
    reporter = ProgressReporter(partitions, sinks=[sink])
    reporter.run_started()

    def worker(partition: ExecutionPartition) -> None:
        reporter.running(partition.id)
        reporter.completed(partition.id)

    threads = [threading.Thread(target=worker, args=(partition,)) for partition in partitions]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    reporter.run_completed()
    reporter.close()

    sequences = [update.event.sequence for update in sink.updates]
    assert len(sequences) == len(set(sequences))
    assert sequences == sorted(sequences)
    assert reporter.counts.completed == 20
    assert reporter.counts.pending == 0


def test_local_backend_emits_partition_and_finalization_progress(
    tmp_path: Path,
    toy_workflow: dict[str, Any],
) -> None:
    workflow = {
        **toy_workflow,
        "data": {"datasets": [{"name": "toydata", "files": ["toy://events"], "nevents": 4}]},
    }
    workflow_path = tmp_path / "workflow.yaml"
    workflow_path.write_text(yaml.safe_dump(workflow, sort_keys=False), encoding="utf-8")
    build_dir = tmp_path / "build"
    compile_workflow_file(workflow_path, outdir=build_dir, chunk_size=2)
    sink = _CollectSink()

    result = run_plan_file(
        build_dir / "compile" / "plan.yaml",
        outdir=build_dir,
        progress_sinks=[sink],
    )

    assert result.success is True
    kinds = [update.event.kind for update in sink.updates]
    assert kinds[:2] == ["run_started", "phase_started"]
    assert [update.event.to_state for update in sink.updates if update.event.kind == "partition_state"] == [
        "running",
        "completed",
        "running",
        "completed",
    ]
    assert ("phase_completed", "executing") in [
        (update.event.kind, update.event.phase) for update in sink.updates
    ]
    assert ("phase_started", "finalizing") in [
        (update.event.kind, update.event.phase) for update in sink.updates
    ]
    assert kinds[-1] == "run_completed"
    assert result.summary["progress"]["counts"]["completed"] == 2


def test_local_backend_reports_failed_partition_and_propagates_error(
    tmp_path: Path,
    toy_workflow: dict[str, Any],
) -> None:
    workflow = {
        **toy_workflow,
        "data": {"datasets": [{"name": "toydata", "files": ["toy://events"], "nevents": 4}]},
    }
    workflow["analysis"]["stages"][0]["params"]["source"] = "missing"
    workflow_path = tmp_path / "workflow.yaml"
    workflow_path.write_text(yaml.safe_dump(workflow, sort_keys=False), encoding="utf-8")
    build_dir = tmp_path / "build"
    compile_workflow_file(workflow_path, outdir=build_dir, chunk_size=2)
    sink = _CollectSink()

    with pytest.raises(KeyError, match="missing"):
        run_plan_file(
            build_dir / "compile" / "plan.yaml",
            outdir=build_dir,
            progress_sinks=[sink],
        )

    states = [update.event.to_state for update in sink.updates if update.event.kind == "partition_state"]
    assert states == ["running", "failed"]
    assert sink.updates[-1].event.kind == "run_failed"


class _Clock:
    def __init__(self) -> None:
        self._wall = datetime(2026, 8, 24, 14, 31, tzinfo=UTC)
        self._monotonic = 100.0

    def wall(self) -> datetime:
        return self._wall

    def monotonic(self) -> float:
        return self._monotonic

    def advance(self, *, monotonic: float, wall: timedelta) -> None:
        self._monotonic += monotonic
        self._wall += wall


class _CollectSink:
    def __init__(self) -> None:
        self.updates: list[ProgressUpdate] = []
        self._lock = threading.Lock()

    def handle(self, update: ProgressUpdate) -> None:
        with self._lock:
            self.updates.append(update)


class _SlowSink:
    def __init__(self, *, delay: float) -> None:
        self.delay = delay

    def handle(self, update: ProgressUpdate) -> None:
        del update
        time.sleep(self.delay)


class _FailingSink:
    def handle(self, update: ProgressUpdate) -> None:
        del update
        raise RuntimeError("boom")


def _partition(partition_id: str) -> ExecutionPartition:
    return ExecutionPartition(
        id=partition_id,
        dataset="dataset",
        file="file.root",
        source="events",
        part=partition_id,
    )


def _assert_counts_invariant(update: ProgressUpdate) -> None:
    counts = update.snapshot.counts
    assert counts.total == counts.pending + counts.running + counts.completed + counts.failed
