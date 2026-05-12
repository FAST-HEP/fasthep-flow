from __future__ import annotations

import inspect
from pathlib import Path
from typing import Any, Protocol

from hepflow.model.io import OutputResult
from hepflow.model.lifecycle import normalize_lifecycle_event
from hepflow.registry.loaders import load_runtime_spec_and_impl


EXECUTION_ONLY_SINK_PARAMS = {"when"}


class OpHandler(Protocol):
    def __call__(
        self,
        inputs: dict[str, Any],
        params: dict[str, Any],
        ctx: dict[str, Any],
    ) -> dict[str, Any]: ...


OP_HANDLERS: dict[str, OpHandler] = {}


def eval_expr(events: Any, expr: str, ctx: dict[str, Any] | None = None) -> Any:
    from hepflow.runtime.engine import eval_expr as _eval_expr

    return _eval_expr(events, expr, ctx)


def register_op(op_name: str, fn: OpHandler) -> None:
    if not isinstance(op_name, str) or not op_name:
        raise ValueError("op_name must be a non-empty string")
    OP_HANDLERS[op_name] = fn


def get_handler(op: str) -> OpHandler:
    handler = OP_HANDLERS.get(op)
    if handler is None:
        raise KeyError(f"No handler registered for op '{op}'")
    return handler


def run_sink(
    *,
    sink_name: str,
    target: Any,
    params: dict[str, Any],
    ctx: dict[str, Any],
    meta: dict[str, Any] | None = None,
    registry_cfg: dict[str, Any] | None,
) -> OutputResult | Any:
    _spec, impl = load_runtime_spec_and_impl(registry_cfg, "sinks", sink_name)
    return _run_writer_like_sink(
        sink_name=sink_name,
        impl=impl,
        target=target,
        params=params,
        ctx=ctx,
        meta=meta,
    )


def _run_writer_like_sink(
    *,
    sink_name: str,
    impl: Any,
    target: Any,
    params: dict[str, Any],
    ctx: dict[str, Any] | None = None,
    meta: dict[str, Any] | None = None,
) -> OutputResult | Any:
    if target is None:
        raise ValueError(f"Sink '{sink_name}' target is None")

    impl_params = _sink_impl_params(params)
    when = normalize_lifecycle_event(params.get("when") or "run_end")
    if "path" in impl_params:
        impl_params = _resolve_writer_paths_for_context(
            impl_params,
            when=when,
            ctx=dict(ctx or {}),
            meta=dict(meta or {}),
        )
    if "output_path" not in impl_params and (
        "out" in impl_params or "spec" in impl_params
    ):
        output_path, output_dir = _derive_artifact_output_paths(
            impl_params,
            ctx=dict(ctx or {}),
            meta=dict(meta or {}),
        )
        impl_params["output_path"] = output_path
        impl_params["output_dir"] = output_dir

    signature = inspect.signature(impl)
    if "ctx" in signature.parameters:
        impl_params["ctx"] = dict(ctx or {})
    if "meta" in signature.parameters:
        impl_params["meta"] = dict(meta or {})

    return impl(target=target, **impl_params)


def _sink_impl_params(params: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in dict(params or {}).items()
        if key not in EXECUTION_ONLY_SINK_PARAMS
    }


def _resolve_writer_paths_for_context(
    params: dict[str, Any],
    *,
    when: str,
    ctx: dict[str, Any],
    meta: dict[str, Any],
) -> dict[str, Any]:
    path_template = str(params["path"])
    path = Path(path_template)
    partition = dict(ctx.get("partition") or {})
    values = {
        "dataset": ctx.get("dataset_name") or partition.get("dataset") or "dataset",
        "part": partition.get("part") or partition.get("id") or "part",
        "partition_id": partition.get("id") or partition.get("part") or "partition",
        "node_id": meta.get("node_id") or meta.get("stage_id") or "sink",
    }

    if any("{" + key + "}" in path_template for key in values):
        path = Path(path_template.format(**values))
    elif when == "partition_end" and partition:
        path = path.with_suffix("") / str(values["dataset"]) / (
            str(values["part"]) + Path(path_template).suffix
        )

    if not path.is_absolute():
        path = Path(ctx.get("outdir") or ".") / path

    resolved = dict(params)
    resolved["path"] = str(path)
    return resolved


def _derive_artifact_output_paths(
    params: dict[str, Any],
    *,
    ctx: dict[str, Any],
    meta: dict[str, Any],
) -> tuple[str, str]:
    spec = dict(params.get("spec") or {})
    out = (
        params.get("out")
        or spec.get("out")
        or meta.get("stage_id")
        or meta.get("node_id")
        or "artifact"
    )
    out_path = Path(str(out))
    if not out_path.suffix:
        out_path = out_path.with_suffix(".png")
    if not out_path.is_absolute():
        out_path = Path(ctx.get("outdir") or ".") / "artifacts" / out_path
    output_dir = out_path.with_suffix("")
    return str(out_path), str(output_dir)


def run_observer(
    *,
    observer_name: str,
    target: Any,
    params: dict[str, Any],
    registry_cfg: dict[str, Any] | None,
    ctx: dict[str, Any] | None = None,
) -> Any:
    spec, impl = load_runtime_spec_and_impl(registry_cfg, "observers", observer_name)
    _validate_component_call(
        spec=spec,
        target=target,
        params=params,
        component_type="observer",
    )
    return impl(target=target, **params, ctx=dict(ctx or {}))


def run_source(
    *,
    source_name: str,
    params: dict[str, Any],
    registry_cfg: dict[str, Any] | None,
    ctx: dict[str, Any] | None = None,
) -> Any:
    spec, impl = load_runtime_spec_and_impl(registry_cfg, "sources", source_name)
    _validate_required_params(spec=spec, params=params)
    return impl(ctx=dict(ctx or {}), **params)


def run_transform(
    *,
    transform_name: str,
    inputs: dict[str, Any],
    params: dict[str, Any],
    registry_cfg: dict[str, Any] | None,
    ctx: dict[str, Any] | None = None,
) -> Any:
    spec, impl = load_runtime_spec_and_impl(registry_cfg, "transforms", transform_name)
    _validate_transform_call(spec=spec, inputs=inputs, params=params)
    return impl(**inputs, **params, ctx=dict(ctx or {}))


def _validate_component_call(
    *,
    spec: Any,
    target: Any,
    params: dict[str, Any],
    component_type: str,
) -> None:
    if target is None:
        raise ValueError(f"{component_type.capitalize()} target is None")
    _validate_required_params(spec=spec, params=params)


def _validate_required_params(*, spec: Any, params: dict[str, Any]) -> None:
    if not isinstance(spec, dict):
        return
    required = [
        name
        for name, cfg in dict(spec.get("params") or {}).items()
        if isinstance(cfg, dict) and cfg.get("required", False)
    ]
    missing = [name for name in required if name not in params]
    if missing:
        raise ValueError(f"Missing required parameters: {missing}")


def _validate_transform_call(
    *,
    spec: Any,
    inputs: dict[str, Any],
    params: dict[str, Any],
) -> None:
    if not isinstance(spec, dict):
        return

    input_cfg = dict(spec.get("input") or {})
    if input_cfg:
        input_name = str(input_cfg.get("name") or "stream")
        required_input = bool(input_cfg.get("required", False))
        if required_input and input_name not in inputs:
            raise ValueError(
                f"Missing required transform input for {spec.get('name')!r}: "
                f"{input_name!r}"
            )

    _validate_required_params(spec=spec, params=params)
