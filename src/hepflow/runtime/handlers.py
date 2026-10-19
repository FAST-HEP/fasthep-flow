from __future__ import annotations

import inspect
import zlib
from pathlib import Path
from typing import Any

from hepflow.build_layout import BuildPaths
from hepflow.model.io import OutputResult
from hepflow.model.lifecycle import normalize_lifecycle_event
from hepflow.registry.loaders import (
    load_runtime_spec_and_impl,
    runtime_registry_from_config,
)
from hepflow.runtime.context import ComponentContext, component_context

EXECUTION_ONLY_SINK_PARAMS = {"when"}


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
    sink_ctx = _sink_runtime_context(ctx=ctx, registry_cfg=registry_cfg)
    return _run_writer_like_sink(
        sink_name=sink_name,
        impl=impl,
        target=target,
        params=params,
        ctx=sink_ctx,
        meta=meta,
    )


def _sink_runtime_context(
    *,
    ctx: dict[str, Any],
    registry_cfg: dict[str, Any] | None,
) -> dict[str, Any]:
    sink_ctx = dict(ctx or {})
    if registry_cfg is not None:
        plan_ctx = dict(sink_ctx.get("plan") or {})
        plan_ctx.setdefault("registry", registry_cfg)
        sink_ctx["plan"] = plan_ctx
    if registry_cfg is not None and "runtime_registry" not in sink_ctx:
        sink_ctx["runtime_registry"] = runtime_registry_from_config(registry_cfg)
    return sink_ctx


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

    component_ctx = component_context(ctx)
    impl_params = _sink_impl_params(params)
    when = normalize_lifecycle_event(params.get("when") or "run_end")
    if "path" in impl_params:
        meta = dict(meta or {})
        meta.setdefault(
            "writer_name", _writer_name(str(impl_params["path"]), meta=meta)
        )
        impl_params = _resolve_writer_paths_for_context(
            impl_params,
            when=when,
            ctx=component_ctx,
            meta=dict(meta or {}),
        )
    if "output_path" not in impl_params and (
        "out" in impl_params or "spec" in impl_params
    ):
        output_path, output_dir = _derive_artifact_output_paths(
            impl_params,
            ctx=component_ctx,
            meta=dict(meta or {}),
        )
        impl_params["output_path"] = output_path
        impl_params["output_dir"] = output_dir

    signature = inspect.signature(impl)
    accepts_kwargs = any(
        parameter.kind == inspect.Parameter.VAR_KEYWORD
        for parameter in signature.parameters.values()
    )
    if "ctx" in signature.parameters or accepts_kwargs:
        impl_params["ctx"] = component_ctx
    if "meta" in signature.parameters or accepts_kwargs:
        impl_params["meta"] = dict(meta or {})

    result = impl(target=target, **impl_params)
    return _localise_writer_manifest(result, component_ctx)


def _localise_writer_manifest(
    result: Any, ctx: dict[str, Any] | ComponentContext
) -> Any:
    """
    Express a writer's manifest path relative to ``output_files_root``.

    Writers record paths relative to ``outdir``, or absolute. When the files are
    written below ``output_files_root`` (a directory local to the worker), the
    absolute path would be meaningless after the run. The path becomes relative
    to that root, ``path_type`` says so, and the optional ``output_destination``
    context entry is recorded as ``destination``, the place where the backend
    delivers the files.
    """

    files_root = BuildPaths.from_ctx(ctx).files_root
    metadata = getattr(result, "metadata", None)
    manifest = metadata.get("writer_manifest") if isinstance(metadata, dict) else None
    if files_root is None or not isinstance(manifest, dict):
        return result
    written = Path(str(getattr(result, "path", None) or manifest.get("path") or ""))
    try:
        relative = written.resolve().relative_to(files_root.resolve())
    except ValueError:
        return result
    manifest["path"] = relative.as_posix()
    manifest["path_type"] = "relative_to_files_root"
    # A checksum to compare with the delivered copy. adler32 is what EOS keeps.
    manifest["adler32"] = _adler32_of_file(written)
    destination = ctx.get("output_destination")
    if destination:
        manifest["destination"] = str(destination)
    return result


def _sink_impl_params(params: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in dict(params or {}).items()
        if key not in EXECUTION_ONLY_SINK_PARAMS
    }


def _adler32_of_file(path: Path) -> str:
    value = 1
    with path.open("rb") as stream:
        while chunk := stream.read(8 * 1024 * 1024):
            value = zlib.adler32(chunk, value)
    return f"{value & 0xFFFFFFFF:08x}"


def _writer_name(path_template: str, *, meta: dict[str, Any]) -> str:
    """
    Name of a writer, used for its manifest directory and manifest ``name``.

    This is the stem of the path template. A stem made of path tokens, as in
    ``{dataset}/{part}.root``, says nothing about the writer, so the node id
    names it instead (``write.FilterChannel.0`` becomes ``FilterChannel_0``).
    """

    stem = Path(path_template).stem
    if "{" not in stem and "}" not in stem:
        return stem
    node_id = str(meta.get("node_id") or meta.get("stage_id") or "").removeprefix(
        "write."
    )
    return node_id.replace(".", "_") or "writer"


def _resolve_writer_paths_for_context(
    params: dict[str, Any],
    *,
    when: str,
    ctx: dict[str, Any] | ComponentContext,
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
        path = BuildPaths.from_ctx(ctx).artifact("files", path)

    resolved = dict(params)
    resolved["path"] = str(path)
    return resolved


def _derive_artifact_output_paths(
    params: dict[str, Any],
    *,
    ctx: dict[str, Any] | ComponentContext,
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
        spec = dict(params.get("spec") or {})
        family = "tables" if spec.get("op") == "hep.render.cutflow_csv" else "plots"
        out_path = BuildPaths.from_ctx(ctx).artifact(family, out_path)
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
    return impl(target=target, **params, ctx=component_context(ctx))


def run_source(
    *,
    source_name: str,
    params: dict[str, Any],
    registry_cfg: dict[str, Any] | None,
    ctx: dict[str, Any] | None = None,
) -> Any:
    spec, impl = load_runtime_spec_and_impl(registry_cfg, "sources", source_name)
    _validate_required_params(spec=spec, params=params)
    return impl(ctx=component_context(ctx), **params)


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
    return impl(**inputs, **params, ctx=component_context(ctx))


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
