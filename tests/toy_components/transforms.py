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

TOY_DEFAULTED_SPEC = {
    "name": "toy.defaulted",
    "kind": "transform",
    "input": {"name": "stream", "required": True},
    "params": {
        "required": {"required": True},
        "mode": {"required": False, "default": "nominal"},
        "sort": {
            "required": False,
            "default": {"by": "pt", "order": "descending"},
        },
    },
    "normalize_params": {"defaults": True},
    "result": {"stream": "event_stream"},
}

TOY_STAGE_ID_OUTPUT_SPEC = {
    "name": "toy.stage_id_output",
    "kind": "transform",
    "input": {"name": "stream", "required": True},
    "params": {
        "output": {"required": False},
    },
    "normalize_params": {"stage_id_defaults": {"output": "id"}},
    "result": {"stream": "event_stream"},
    "provides": {
        "symbols": [
            {"from": "params.output", "kind": "field_list"},
        ],
    },
}

TOY_TEMPLATE_OUTPUT_SPEC = {
    "name": "toy.template_output",
    "kind": "transform",
    "input": {"name": "stream", "required": True},
    "params": {
        "source": {"required": True},
        "output": {"required": False},
    },
    "normalize_params": {"param_templates": {"output": "derived_{source}"}},
    "result": {"stream": "event_stream"},
    "provides": {
        "symbols": [
            {"from": "params.output", "kind": "field_list"},
        ],
    },
}

TOY_PRODUCT_SPEC = {
    "name": "toy.product",
    "kind": "transform",
    "input": None,
    "params": {
        "dataset": {"required": False},
        "value": {"required": False, "default": "product"},
    },
    "result": {"product": "toy_product"},
}

TOY_PRODUCT_PAIR_SPEC = {
    "name": "toy.product_pair",
    "kind": "transform",
    "input": None,
    "params": {},
    "result": {"pair": {"kind": "toy_pair"}},
}

TOY_HIST_SPEC = {
    "name": "hep.hist",
    "kind": "transform",
    "input": {"name": "stream", "kind": "event_stream", "required": True},
    "params": {},
    "result": {"hist": {"kind": "histogram"}},
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


def run_toy_defaulted(
    *,
    stream: dict[str, Any],
    required: str,
    mode: str = "nominal",
    sort: dict[str, Any] | None = None,
    ctx: dict[str, Any] | None = None,
    **params: Any,
) -> dict[str, Any]:
    return {
        "stream": {
            **stream,
            "required": required,
            "mode": mode,
            "sort_by": None if sort is None else sort.get("by"),
        }
    }


def run_toy_stage_id_output(
    *,
    stream: dict[str, Any],
    output: str,
    ctx: dict[str, Any] | None = None,
    **params: Any,
) -> dict[str, Any]:
    del ctx, params
    return {"stream": {**stream, output: [True for _ in stream.get("pt", [])]}}


def run_toy_template_output(
    *,
    stream: dict[str, Any],
    source: str,
    output: str,
    ctx: dict[str, Any] | None = None,
    **params: Any,
) -> dict[str, Any]:
    del ctx, params
    return {"stream": {**stream, output: list(stream.get(source, []))}}


def run_toy_product(
    *,
    dataset: str | None = None,
    value: str = "product",
    ctx: dict[str, Any] | None = None,
) -> dict[str, Any]:
    datasets = dict((ctx or {}).get("datasets") or {})
    dataset_record = datasets.get(dataset or "", {})
    return {
        "product": {
            "value": value,
            "dataset": dataset,
            "dataset_meta": dict(dataset_record.get("meta") or {}),
        }
    }


def run_toy_product_pair(
    *,
    left: dict[str, Any],
    right: dict[str, Any],
    ctx: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "pair": {
            "left": left,
            "right": right,
            "input_products": dict((ctx or {}).get("input_products") or {}),
        }
    }


def run_toy_hist(
    *,
    stream: dict[str, Any],
    ctx: dict[str, Any] | None = None,
    **params: Any,
) -> dict[str, Any]:
    del ctx, params
    return {"hist": {"entries": len(next(iter(stream.values()), []))}}
