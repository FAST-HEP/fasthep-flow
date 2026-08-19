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

TOY_NEW_LINEAGE_SPEC = {
    **TOY_SCALE_SPEC,
    "name": "toy.new_lineage",
    "result": {
        "stream": {
            "kind": "event_stream",
            "lineage": "new",
        }
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

TOY_FIELD_GLOB_SPEC = {
    "name": "toy.field_glob",
    "kind": "transform",
    "input": {"name": "stream", "required": True},
    "params": {
        "fields": {
            "type": "list[string]",
            "required": False,
            "default": [],
            "hooks": [
                {
                    "name": "flow.expand_field_glob",
                    "against": "input.stream",
                }
            ],
        },
        "plain": {
            "type": "list[string]",
            "required": False,
            "default": [],
        },
        "output": {"required": False, "default": "matched_fields"},
    },
    "result": {"stream": "event_stream"},
    "requires": {
        "symbols": [
            {"from": "params.fields", "kind": "field_list"},
        ],
    },
    "provides": {
        "symbols": [
            {"from": "params.output", "kind": "field_list"},
        ],
    },
}

def _toy_field_glob_spec(name: str, hooks: Any) -> dict[str, Any]:
    return {
        "name": name,
        "kind": "transform",
        "input": {"name": "stream", "required": True},
        "params": {
            "fields": {
                "type": "list[string]",
                "required": False,
                "default": [],
                "hooks": hooks,
            },
            "plain": {
                "type": "list[string]",
                "required": False,
                "default": [],
            },
            "output": {"required": False, "default": "matched_fields"},
        },
        "result": {"stream": "event_stream"},
        "requires": {
            "symbols": [
                {"from": "params.fields", "kind": "field_list"},
            ],
        },
        "provides": {
            "symbols": [
                {"from": "params.output", "kind": "field_list"},
            ],
        },
    }


TOY_DOUBLE_FIELD_GLOB_SPEC = _toy_field_glob_spec(
    "toy.double_field_glob",
    [
        {
            "name": "flow.expand_field_glob",
            "against": "input.stream",
        },
        {
            "name": "flow.expand_field_glob",
            "against": "input.stream",
        },
    ],
)

TOY_UNKNOWN_PARAM_HOOK_SPEC = _toy_field_glob_spec(
    "toy.unknown_param_hook",
    [
        {
            "name": "toy.missing_param_hook",
            "against": "input.stream",
        }
    ],
)

TOY_MALFORMED_PARAM_HOOK_SPEC = _toy_field_glob_spec(
    "toy.malformed_param_hook",
    {"name": "flow.expand_field_glob"},
)

TOY_MAPPING_CONFIG_SPEC = {
    "name": "toy.mapping_config",
    "kind": "transform",
    "input": {"name": "stream", "required": True},
    "params": {
        "config": {
            "type": "mapping",
            "required": True,
            "hooks": [
                {
                    "name": "flow.load_mapping",
                    "formats": ["yaml", "yml", "json"],
                }
            ],
        },
        "label": {"type": "string", "required": False},
    },
    "result": {"stream": "event_stream"},
}

TOY_STRING_CONFIG_SPEC = {
    "name": "toy.string_config",
    "kind": "transform",
    "input": {"name": "stream", "required": True},
    "params": {
        "config": {"type": "string", "required": True},
    },
    "result": {"stream": "event_stream"},
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

TOY_PROJECT_FIELDS_SPEC = {
    "name": "hep.project_fields",
    "kind": "transform",
    "input": {"name": "stream", "kind": "event_stream", "required": True},
    "params": {
        "stream_id": {"required": True},
        "aliases": {"required": True},
        "include_existing": {"required": False, "default": True},
    },
    "result": {"stream": "event_stream"},
    "requires": {
        "symbols": [
            {"from": "params.aliases.*", "kind": "field_list"},
        ]
    },
    "provides": {
        "symbols": [
            {"from": "params.aliases", "kind": "field_list"},
        ]
    },
}

TOY_MERGE_FIELDS_SPEC = {
    "name": "hep.merge_fields",
    "kind": "transform",
    "input": {
        "inactive_inputs": "omit",
    },
    "params": {
        "on_conflict": {"required": False, "default": "keep_first"},
    },
    "result": {
        "stream": {
            "kind": "event_stream",
            "field_propagation": "merge",
            "lineage": "require_equal",
        }
    },
}

TOY_PRESERVE_MERGE_FIELDS_SPEC = {
    **TOY_MERGE_FIELDS_SPEC,
    "name": "toy.preserve_merge_fields",
    "result": {
        "stream": {
            "kind": "event_stream",
            "field_propagation": "merge",
            "lineage": "preserve",
        }
    },
}

TOY_STRICT_MERGE_FIELDS_SPEC = {
    **TOY_MERGE_FIELDS_SPEC,
    "name": "toy.strict_merge_fields",
    "input": {},
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


def run_toy_project_fields(
    *,
    stream: dict[str, Any],
    stream_id: str,
    aliases: dict[str, str],
    include_existing: bool = True,
    ctx: dict[str, Any] | None = None,
) -> dict[str, Any]:
    del ctx, stream_id
    projected = {alias: stream[source] for alias, source in aliases.items()}
    return {"stream": {**stream, **projected} if include_existing else projected}


def run_toy_merge_fields(
    *,
    on_conflict: str = "keep_first",
    ctx: dict[str, Any] | None = None,
    **streams: dict[str, Any],
) -> dict[str, Any]:
    del ctx
    merged: dict[str, Any] = {}
    for stream in streams.values():
        for field, values in stream.items():
            if field in merged and on_conflict == "error":
                raise ValueError(f"merge_fields duplicate field: {field}")
            if field in merged and on_conflict == "keep_first":
                continue
            merged[field] = values
    return {"stream": merged}


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


def run_toy_field_glob(
    *,
    stream: dict[str, Any],
    fields: list[str] | None = None,
    plain: list[str] | None = None,
    output: str = "matched_fields",
    ctx: dict[str, Any] | None = None,
    **params: Any,
) -> dict[str, Any]:
    del ctx, params
    selected = list(fields or [])
    return {
        "stream": {
            **stream,
            output: selected,
            "plain_fields": list(plain or []),
        }
    }


def run_toy_mapping_config(
    *,
    stream: dict[str, Any],
    config: dict[str, Any],
    label: str | None = None,
    ctx: dict[str, Any] | None = None,
) -> dict[str, Any]:
    del label, ctx
    return {"stream": {**stream, "config": config}}


def run_toy_string_config(
    *,
    stream: dict[str, Any],
    config: str,
    ctx: dict[str, Any] | None = None,
) -> dict[str, Any]:
    del ctx
    return {"stream": {**stream, "config": config}}


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
