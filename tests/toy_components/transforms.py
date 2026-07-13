from __future__ import annotations

from typing import Any

TOY_SCALE_SPEC = {
    "name": "toy.scale",
    "kind": "transform",
    "input": {"name": "stream", "required": True},
    "params": {
        "factor": {"required": False},
        "source": {"required": False, "default": "pt"},
        "output": {"required": False, "default": "scaled_pt"},
    },
    "result": {"stream": "event_stream"},
    "requires": {
        "symbols": [
            {"from": "params.source", "kind": "expr_or_field"},
        ],
    },
    "provides": {
        "symbols": [
            {"from": "params.output", "kind": "field_list"},
        ],
    },
}

TOY_RECORD_SPEC = {
    "name": "toy.record",
    "kind": "transform",
    "input": {"name": "stream", "required": True},
    "params": {
        "source": {"required": False, "default": "pt"},
        "output": {"required": False, "default": "recorded_pt"},
    },
    "result": {"stream": "event_stream"},
    "requires": {
        "symbols": [
            {"from": "params.source", "kind": "field_list"},
        ],
    },
    "provides": {
        "symbols": [
            {"from": "params.output", "kind": "field_list"},
        ],
    },
}


def run_toy_scale(
    *,
    stream: dict[str, Any],
    factor: int | float = 1,
    source: str = "pt",
    output: str = "scaled_pt",
    ctx: dict[str, Any] | None = None,
    **params: Any,
) -> dict[str, Any]:
    values = [value * factor for value in stream[source]]
    return {"stream": {**stream, output: values}}


def run_toy_record(
    *,
    stream: dict[str, Any],
    source: str = "pt",
    output: str = "recorded_pt",
    ctx: dict[str, Any] | None = None,
    **params: Any,
) -> dict[str, Any]:
    values = list(stream[source])
    provenance = (ctx or {}).get("provenance")
    if provenance is not None:
        provenance.record_operation(
            inputs={"symbols": [source]},
            outputs={"symbols": [output]},
        )
    return {"stream": {**stream, output: values}}
