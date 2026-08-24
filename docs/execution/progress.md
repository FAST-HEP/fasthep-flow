# Progress reporting

Flow owns a backend-neutral progress contract for execution-state reporting.
Backends translate their native scheduler observations into Flow states, while
progress sinks own presentation and transport.

```text
Backend
    |
    v
ProgressReporter
    |
    +-> buffered TerminalSink
    +-> buffered LoggingSink
    +-> buffered JsonlSink
    +-> buffered Queue/WebSocket/etc
```

The core model reports partition-granular progress. Every execution partition
starts as `pending` and can move through:

```text
pending -> running
pending -> completed
pending -> failed
running -> pending
running -> completed
running -> failed
```

The `running -> pending` transition represents a requeue, preemption, eviction,
or retry without adding backend-specific states such as `held` or `idle` to the
core model. Native scheduler details belong in event `detail`, for example:

```yaml
reason: preempted
backend: htcondor
backend_state: held
```

Completed and failed partitions are terminal. A backend should report `failed`
only after retries are exhausted.

## Events and snapshots

The reporter emits immutable progress updates. Each update contains:

- one event, such as `run_started`, `phase_started`, `partition_state`,
  `run_completed`, or `run_failed`
- one Flow-derived snapshot containing current run state, phase, elapsed time,
  and pending/running/completed/failed counts

Sinks receive the snapshot with every event so terminal renderers, JSONL logs,
and dashboards do not reconstruct counts independently. A terminal sink can show:

```text
Executing: 52 complete / 8 running / 60 pending
```

During output merging or materialization, Flow reports the `finalizing` phase
instead of pretending the run is done just because every partition has finished:

```text
Finalizing outputs...
```

## Timing

Every event records a timezone-aware UTC `timestamp`. This is the time at which
the Flow reporter observed the event on the driver, not necessarily an
authoritative remote scheduler timestamp.

Durations use a monotonic clock:

- `elapsed_seconds` is measured from `run_started`
- `state_duration_seconds` is the time spent in the partition state being left

Wall-clock movement never affects duration calculation. If a backend later
provides scheduler timestamps, preserve them in `detail` rather than replacing
the Flow timestamp.

## Sink isolation

Progress sinks are synchronous objects with a small `handle(update)` method, but
the reporter does not call them on the execution hot path. Each sink is wrapped
by a buffered worker. Reporter calls update state, allocate a sequence number,
construct an immutable update, release the state lock, and enqueue the update.

A slow sink cannot block execution and cannot delay other sinks. If a sink
raises, Flow disables that sink, stores a warning, and continues the workflow.
A broken dashboard must not fail a physics run.

Reporter shutdown is best-effort with a finite timeout. At run completion or
failure, backend summary code can retrieve sink warnings and include them with
normal run warnings.

## Extension

Progress sinks are registered in their own registry section:

```yaml
registry:
  progress_sinks:
    terminal:
      impl: my_package.progress:TerminalSink
    jsonl:
      impl: my_package.progress:JsonlSink
```

This keeps progress presentation separate from rendering, execution hooks, and
physics semantics. Execution hooks instrument work inside nodes, while progress
reporting describes driver-observable run and partition state.
