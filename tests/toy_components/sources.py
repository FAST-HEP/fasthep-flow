from __future__ import annotations

from typing import Any

TOY_SOURCE_SPEC = {
    "name": "toy.source",
    "kind": "source",
    "params": {},
    "result": {"stream": "event_stream"},
}


def run_toy_source(*, ctx: dict[str, Any] | None = None, **params: Any) -> dict[str, Any]:
    partition = dict((ctx or {}).get("partition") or {})
    start = partition.get("start")
    stop = partition.get("stop")
    values = [12, 18, 21, 28]
    if start is not None and stop is not None:
        values = values[int(start) : int(stop)]
    return {
        "pt": values,
        "dataset": (ctx or {}).get("dataset_name"),
        "params": {
            key: value
            for key, value in params.items()
            if key not in {"datasets", "defaults", "branches"}
        },
    }
