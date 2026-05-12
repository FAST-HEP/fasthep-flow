from __future__ import annotations


VALID_LIFECYCLE_EVENTS = {
    "partition_start",
    "partition_end",
    "dataset_end",
    "run_end",
    "around_node",
    "on_node_error",
}

WHEN_ALIASES = {
    "partition": "partition_end",
    "dataset": "dataset_end",
    "final": "run_end",
}


def normalize_lifecycle_event(value: str) -> str:
    event = str(value).strip()
    event = WHEN_ALIASES.get(event, event)

    if event not in VALID_LIFECYCLE_EVENTS:
        expected = sorted(VALID_LIFECYCLE_EVENTS | set(WHEN_ALIASES))
        raise ValueError(
            f"Unsupported lifecycle event {event!r}. "
            f"Expected one of {expected}"
        )

    return event
