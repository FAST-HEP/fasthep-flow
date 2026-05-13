from __future__ import annotations

from typing import Any

from hepflow.model.data_flow import DataDependencyResult

TOY_SCALE_SPEC = {
    "name": "toy.scale",
    "kind": "transform",
    "input": {"name": "stream", "required": True},
    "params": {
        "factor": {"required": False},
        "source": {"required": False},
        "output": {"required": False},
    },
    "result": {"stream": "event_stream"},
    "dependencies": {
        "parser": "tests.toy_components.transforms:parse_toy_scale_dependencies",
    },
}


def parse_toy_scale_dependencies(
    params: dict[str, Any],
    context_symbols: set[str] | None = None,
    **_: Any,
) -> DataDependencyResult:
    source = str(params.get("source") or "pt")
    consumes = set() if source in set(context_symbols or set()) else {source}
    return DataDependencyResult(
        consumes=consumes,
        produces={str(params.get("output") or "scaled_pt")},
    )


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
