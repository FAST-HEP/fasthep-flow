# normalize.py
from __future__ import annotations

from dataclasses import fields
from typing import Any

from hepflow.compiler.execution import (
    normalize_global_execution,
    normalize_stage_execution,
    validate_stage_execution_resource_references,
)
from hepflow.compiler.profiles import normalize_profile_names
from hepflow.model.author import (
    DataBlock,
    DatasetSpec,
    FieldSpec,
    JoinInputSpec,
    NormalizedAuthor,
    RootTreeSourceSpec,
    SystematicApplicability,
    SystematicsConfig,
    SystematicVariation,
    SystematicWeightRule,
    ZipJoinSpec,
    inject_default_events_source,
)
from hepflow.model.defaults import (
    DEFAULT_DATASET_EVENTTYPE,
    DEFAULT_JOIN_ON_MISMATCH,
    DEFAULT_ROOT_TREE,
    DEFAULT_STREAM_TYPE,
)
from hepflow.registry.defaults import (
    default_expr_registry_config,
    default_runtime_registry_config,
    merge_registry_config,
)


def normalize_author(doc: dict[str, Any]) -> dict[str, Any]:
    doc = _ensure_mapping(doc, "document")
    if "sinks" in doc:
        raise ValueError(
            "Top-level 'sinks' is not supported. Attach a writer to an "
            "analysis stage with 'analysis.stages[].write' instead."
        )
    version = str(doc.get("version", "1.0"))
    data = normalize_data(doc.get("data") or {}, doc.get("datasets"))

    sources = normalize_sources(doc.get("sources"), data.defaults)

    if not sources:
        sources["events"] = inject_default_events_source(data.defaults).to_dict()

    joins = normalize_joins(doc.get("joins"))
    fields = normalize_fields(doc.get("fields"))
    styles = normalize_styles(doc.get("styles"))
    observers = normalize_top_level_observers(doc.get("observers"))

    analysis = doc.get("analysis") or {}
    analysis = _ensure_mapping(analysis, "analysis")

    primary = doc.get("primary_stream")
    primary_stream = str(primary) if primary is not None else None

    use = normalize_use(doc.get("use"))
    execution = normalize_global_execution(doc.get("execution"))
    analysis = _normalize_analysis_execution(analysis)
    validate_stage_execution_resource_references(
        list(analysis.get("stages") or []),
        execution["resources"],
        execution["pools"],
    )
    systematics = normalize_systematics(doc.get("systematics"))

    registry_cfg = normalize_registry(doc.get("registry"))
    merged_registry_cfg = merge_registry_config(
        {
            **default_expr_registry_config(),
            **default_runtime_registry_config(),
        },
        registry_cfg,
    )

    norm = NormalizedAuthor(
        version=version,
        data=data,
        sources=sources,
        joins=joins,
        fields=fields,
        styles=styles,
        observers=observers,
        analysis=analysis,
        primary_stream=primary_stream,
        use=use,
        execution=execution,
        registry=merged_registry_cfg,
        systematics=systematics,
    )
    return norm.to_dict()


def normalize_use(raw: Any) -> dict[str, Any]:
    raw = _ensure_mapping(raw or {}, "use")
    return {
        "profiles": normalize_profile_names(raw.get("profiles")),
    }


def normalize_systematics(raw: Any) -> SystematicsConfig | None:
    if raw is None:
        return None
    raw = _ensure_mapping(raw, "systematics")

    variations_raw = raw.get("variations", [])
    if not isinstance(variations_raw, list):
        raise ValueError("systematics.variations must be a list")

    profiles_raw = raw.get("profiles", [])
    if not isinstance(profiles_raw, list):
        raise ValueError("systematics.profiles must be a list")
    profiles = _list_of_strings(profiles_raw, "systematics.profiles")

    variations: list[SystematicVariation] = []
    seen: set[str] = set()
    for idx, variation_raw in enumerate(variations_raw):
        variation = _ensure_mapping(variation_raw, f"systematics.variations[{idx}]")
        name_raw = variation.get("name")
        if not isinstance(name_raw, str) or not name_raw.strip():
            raise ValueError(f"systematics.variations[{idx}].name is required")
        name = name_raw.strip()
        if name in seen:
            raise ValueError(f"duplicate systematics variation name: {name}")
        seen.add(name)

        variations.append(
            SystematicVariation(
                name=name,
                group=_optional_string(
                    variation.get("group"), f"systematics.variations[{idx}].group"
                ),
                direction=_optional_string(
                    variation.get("direction"),
                    f"systematics.variations[{idx}].direction",
                ),
                applies_to=_normalize_systematic_applicability(
                    variation.get("applies_to"),
                    f"systematics.variations[{idx}].applies_to",
                ),
                requires=_normalize_optional_string_list(
                    variation.get("requires"),
                    f"systematics.variations[{idx}].requires",
                ),
                weight=_normalize_systematic_weight(
                    variation.get("weight"),
                    f"systematics.variations[{idx}].weight",
                ),
                replace=_normalize_string_mapping(
                    variation.get("replace"),
                    f"systematics.variations[{idx}].replace",
                ),
                datasets=_normalize_mapping_or_empty(
                    variation.get("datasets"),
                    f"systematics.variations[{idx}].datasets",
                ),
            )
        )

    include_nominal = raw.get("include_nominal", False)
    if not isinstance(include_nominal, bool):
        raise ValueError("systematics.include_nominal must be a boolean")

    return SystematicsConfig(
        include_nominal=include_nominal,
        profiles=profiles,
        variations=variations,
    )


def normalize_data(data: dict[str, Any], datasets_include: Any = None) -> DataBlock:
    data = _ensure_mapping(data, "data")
    defaults = _ensure_mapping(data.get("defaults") or {}, "data.defaults")
    datasets_raw = _merge_included_datasets(data.get("datasets"), datasets_include)
    datasets = normalize_datasets(datasets_raw, data_defaults=defaults)
    return DataBlock(defaults=defaults, datasets=datasets)


def _merge_included_datasets(data_datasets: Any, included: Any) -> Any:
    included_list = _normalize_dataset_collection(included, "datasets")
    data_list = _normalize_dataset_collection(data_datasets, "data.datasets")
    return included_list + data_list


def _normalize_dataset_collection(raw: Any, where: str) -> list[dict[str, Any]]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return list(raw)
    if isinstance(raw, dict):
        out: list[dict[str, Any]] = []
        for name, spec_raw in raw.items():
            spec = _ensure_mapping(spec_raw, f"{where}.{name}")
            item = dict(spec)
            item.setdefault("name", str(name))
            out.append(item)
        return out
    raise ValueError(f"{where} must be a list or mapping")


def normalize_datasets(
    datasets: Any, data_defaults: dict[str, Any]
) -> list[DatasetSpec]:
    if not isinstance(datasets, list):
        raise ValueError("data.datasets must be a list")
    norm_datasets: list[DatasetSpec] = []
    known = {f.name for f in fields(DatasetSpec)} - {"meta"}

    if not isinstance(datasets, list):
        raise ValueError("data.datasets must be a list")
    for i, ds_raw in enumerate(datasets):
        ds = _ensure_mapping(ds_raw, f"data.datasets[{i}]")
        name = ds.get("name")
        files = ds.get("files")
        if not isinstance(files, list):
            raise ValueError(f"data.datasets[{i}].files must be a list")
        meta = {k: v for k, v in ds.items() if k not in known and v is not None}
        norm_datasets.append(
            DatasetSpec(
                name=str(name) if name else "",
                files=files,
                nevents=ds.get("nevents"),
                eventtype=str(
                    ds.get(
                        "eventtype",
                        data_defaults.get("eventtype", DEFAULT_DATASET_EVENTTYPE),
                    )
                ),
                group=ds.get("group"),
                meta=meta,
            )
        )
    return norm_datasets


def normalize_sources(
    sources: Any, data_defaults: dict[str, Any]
) -> dict[str, dict[str, Any]]:
    # If sources are provided explicitly, preserve them.
    if sources is None:
        sources = {}
    sources = _ensure_mapping(sources, "sources")

    out: dict[str, dict[str, Any]] = {}
    for sid, spec_raw in sources.items():
        spec = _ensure_mapping(spec_raw, f"sources.{sid}")
        kind = str(spec.get("kind", "root_tree")).strip()
        if not kind:
            raise ValueError(f"sources.{sid}.kind must be a non-empty string")
        if kind == "root_tree":
            tree = spec.get("tree")
            if not tree:
                raise ValueError(f"sources.{sid} missing tree")
            source = RootTreeSourceSpec(
                kind="root_tree",
                tree=str(tree),
                stream_type=str(spec.get("stream_type", DEFAULT_STREAM_TYPE)),
            ).to_dict()
            for optional_key in ("branches", "start", "stop"):
                if optional_key in spec:
                    source[optional_key] = spec[optional_key]
            out[str(sid)] = source
            continue

        out[str(sid)] = {
            **dict(spec),
            "kind": kind,
            "stream_type": str(spec.get("stream_type", DEFAULT_STREAM_TYPE)),
        }

    # v1 compatibility: only inject default if NONE provided
    if not out:
        default_tree = data_defaults.get("tree_primary", DEFAULT_ROOT_TREE)
        out["events"] = RootTreeSourceSpec(
            kind="root_tree",
            tree=default_tree,
            stream_type=DEFAULT_STREAM_TYPE,
        ).to_dict()

    return out


def normalize_joins(joins_raw: Any) -> dict[str, ZipJoinSpec]:
    if joins_raw is None:
        return {}
    joins_raw = _ensure_mapping(joins_raw, "joins")
    joins: dict[str, ZipJoinSpec] = {}
    for jid, join_raw in joins_raw.items():
        join = _ensure_mapping(join_raw, f"joins.{jid}")
        kind = join.get("kind", "zip")
        if kind != "zip":
            raise ValueError(f"joins.{jid}.kind must be zip (v2.1)")
        inputs_raw = join.get("inputs")
        if not isinstance(inputs_raw, list) or not inputs_raw:
            raise ValueError(f"joins.{jid}.inputs must be a non-empty list")
        inputs: list[JoinInputSpec] = []
        for item_raw in inputs_raw:
            if isinstance(item_raw, str):
                inputs.append(JoinInputSpec(source=item_raw, prefix=item_raw))
            else:
                item = _ensure_mapping(item_raw, f"joins.{jid}.inputs[]")
                src = item.get("source")
                if not src:
                    raise ValueError(f"joins.{jid}.inputs[] missing source")
                pref = item.get("prefix") or src
                inputs.append(JoinInputSpec(source=str(src), prefix=str(pref)))
        joins[str(jid)] = ZipJoinSpec(
            inputs=inputs,
            on_mismatch=str(join.get("on_mismatch", DEFAULT_JOIN_ON_MISMATCH)),
        )
    return joins


def normalize_fields(fields_raw: Any) -> dict[str, FieldSpec]:
    if fields_raw is None:
        return {}
    fields_raw = _ensure_mapping(fields_raw, "fields")
    fields: dict[str, FieldSpec] = {}
    for alias, field_raw in fields_raw.items():
        field = _ensure_mapping(field_raw, f"fields.{alias}")
        fields[str(alias)] = FieldSpec(
            stream=field.get("stream", ""), branch=field.get("branch", "")
        )
    return fields


def normalize_styles(styles: Any) -> dict[str, dict[str, Any]]:
    if styles is None:
        return {}
    styles = _ensure_mapping(styles, "styles")
    out: dict[str, dict[str, Any]] = {}
    for name, spec_raw in styles.items():
        if not isinstance(name, str) or not name:
            raise ValueError("styles keys must be non-empty strings")
        spec = _ensure_mapping(spec_raw, f"styles.{name}")
        out[name] = dict(spec)
    return out


def normalize_top_level_observers(observers_raw: Any) -> list[dict[str, Any]]:
    if observers_raw is None:
        return []
    if not isinstance(observers_raw, list):
        raise ValueError("observers must be a list")

    out: list[dict[str, Any]] = []
    for idx, observer_raw in enumerate(observers_raw):
        observer = _ensure_mapping(observer_raw, f"observers[{idx}]")
        kind = observer.get("kind")
        if not isinstance(kind, str) or not kind.strip():
            raise ValueError(f"observers[{idx}].kind must be a non-empty string")

        at_raw = observer.get("at")
        if isinstance(at_raw, str):
            at = [at_raw]
        elif isinstance(at_raw, list):
            at = []
            for at_idx, item in enumerate(at_raw):
                if not isinstance(item, str) or not item.strip():
                    raise ValueError(
                        f"observers[{idx}].at[{at_idx}] must be a non-empty string"
                    )
                at.append(item)
        else:
            raise ValueError(f"observers[{idx}].at must be a string or list of strings")

        out.append(
            {
                **dict(observer),
                "kind": kind.strip(),
                "at": at,
            }
        )

    return out


def _normalize_analysis_execution(analysis: dict[str, Any]) -> dict[str, Any]:
    analysis = dict(analysis)
    stages_raw = analysis.get("stages")
    if stages_raw is None:
        return analysis
    if not isinstance(stages_raw, list):
        raise ValueError("analysis.stages must be a list")

    stages: list[Any] = []
    for idx, stage_raw in enumerate(stages_raw):
        if not isinstance(stage_raw, dict):
            raise ValueError(f"analysis.stages[{idx}] must be a mapping")
        stage = dict(stage_raw)
        if "execution" in stage:
            stage_execution = normalize_stage_execution(stage.get("execution"))
            if stage_execution is not None:
                stage["execution"] = stage_execution
            else:
                stage.pop("execution", None)
        stages.append(stage)
    analysis["stages"] = stages
    return analysis


def _ensure_mapping(x: Any, where: str) -> dict[str, Any]:
    if not isinstance(x, dict):
        raise ValueError(f"{where} must be a mapping")
    return x


def _normalize_systematic_applicability(
    raw: Any, where: str
) -> SystematicApplicability:
    if raw is None:
        return SystematicApplicability()
    if isinstance(raw, str):
        if not raw.strip():
            raise ValueError(f"{where} must be a non-empty string")
        return SystematicApplicability(eventtypes=[raw.strip()])
    if not isinstance(raw, dict):
        raise ValueError(f"{where} must be a string or mapping")

    eventtypes = _normalize_optional_string_or_list(
        raw.get("eventtypes"), f"{where}.eventtypes"
    )
    datasets = _normalize_optional_string_or_list(
        raw.get("datasets"), f"{where}.datasets"
    )
    return SystematicApplicability(eventtypes=eventtypes, datasets=datasets)


def _normalize_systematic_weight(raw: Any, where: str) -> SystematicWeightRule:
    if raw is None:
        return SystematicWeightRule()
    raw = _ensure_mapping(raw, where)
    multiply = _normalize_optional_string_or_list(
        raw.get("multiply"), f"{where}.multiply"
    )
    return SystematicWeightRule(multiply=multiply)


def _normalize_mapping_or_empty(raw: Any, where: str) -> dict[str, Any]:
    if raw is None:
        return {}
    raw = _ensure_mapping(raw, where)
    return dict(raw)


def _normalize_string_mapping(raw: Any, where: str) -> dict[str, str]:
    if raw is None:
        return {}
    raw = _ensure_mapping(raw, where)
    out: dict[str, str] = {}
    for key, value in raw.items():
        if not isinstance(key, str) or not key.strip():
            raise ValueError(f"{where} keys must be non-empty strings")
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{where}[{key!r}] must be a non-empty string")
        out[key] = value
    return out


def _normalize_optional_string_list(raw: Any, where: str) -> list[str]:
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise ValueError(f"{where} must be a list")
    return _list_of_strings(raw, where)


def _normalize_optional_string_or_list(raw: Any, where: str) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, str):
        if not raw.strip():
            raise ValueError(f"{where} must be a non-empty string")
        return [raw.strip()]
    if not isinstance(raw, list):
        raise ValueError(f"{where} must be a string or list")
    return _list_of_strings(raw, where)


def _list_of_strings(raw: list[Any], where: str) -> list[str]:
    out: list[str] = []
    for idx, item in enumerate(raw):
        if not isinstance(item, str) or not item.strip():
            raise ValueError(f"{where}[{idx}] must be a non-empty string")
        out.append(item.strip())
    return out


def _optional_string(raw: Any, where: str) -> str | None:
    if raw is None:
        return None
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError(f"{where} must be a non-empty string")
    return raw.strip()


def normalize_registry(raw: dict[str, Any] | None) -> dict[str, Any]:
    raw = _ensure_mapping(raw or {}, "registry")

    functions = _ensure_mapping(raw.get("functions") or {}, "registry.functions")
    constants = _ensure_mapping(raw.get("constants") or {}, "registry.constants")
    ops = _ensure_mapping(raw.get("ops") or {}, "registry.ops")
    renderers = _ensure_mapping(raw.get("renderers") or {}, "registry.renderers")
    sinks = _ensure_mapping(raw.get("sinks") or {}, "registry.sinks")
    sources = _ensure_mapping(raw.get("sources") or {}, "registry.sources")
    transforms = _ensure_mapping(raw.get("transforms") or {}, "registry.transforms")
    observers = _ensure_mapping(raw.get("observers") or {}, "registry.observers")
    backends = _ensure_mapping(raw.get("backends") or {}, "registry.backends")
    hooks = _ensure_mapping(raw.get("hooks") or {}, "registry.hooks")

    for group_name, group in [
        ("registry.functions", functions),
        ("registry.constants", constants),
    ]:
        for name, spec in group.items():
            if not isinstance(name, str) or not name.strip():
                raise ValueError(f"{group_name} keys must be non-empty strings")
            if not isinstance(spec, str) or ":" not in spec:
                raise ValueError(f"{group_name}[{name!r}] must be 'module:object'")

    for group_name, group in [
        ("registry.ops", ops),
        ("registry.renderers", renderers),
        ("registry.sinks", sinks),
        ("registry.sources", sources),
        ("registry.transforms", transforms),
        ("registry.observers", observers),
    ]:
        for name, entry in group.items():
            if not isinstance(name, str) or not name.strip():
                raise ValueError(f"{group_name} keys must be non-empty strings")
            if not isinstance(entry, dict):
                raise ValueError(
                    f"{group_name}[{name!r}] must be a mapping with 'spec' and 'impl'"
                )
            spec_ref = entry.get("spec")
            impl_ref = entry.get("impl")
            if not isinstance(spec_ref, str) or ":" not in spec_ref:
                raise ValueError(f"{group_name}[{name!r}].spec must be 'module:object'")
            if not isinstance(impl_ref, str) or ":" not in impl_ref:
                raise ValueError(f"{group_name}[{name!r}].impl must be 'module:object'")

    for name, entry in backends.items():
        if not isinstance(name, str) or not name.strip():
            raise ValueError("registry.backends keys must be non-empty strings")
        if not isinstance(entry, dict):
            raise ValueError(
                f"registry.backends[{name!r}] must be a mapping with 'impl'"
            )
        impl_ref = entry.get("impl")
        if not isinstance(impl_ref, str) or ":" not in impl_ref:
            raise ValueError(
                f"registry.backends[{name!r}].impl must be 'module:object'"
            )

    for name, entry in hooks.items():
        if not isinstance(name, str) or not name.strip():
            raise ValueError("registry.hooks keys must be non-empty strings")
        if not isinstance(entry, dict):
            raise ValueError(
                f"registry.hooks[{name!r}] must be a mapping with 'spec' and 'impl'"
            )
        spec_ref = entry.get("spec")
        if not isinstance(spec_ref, str) or ":" not in spec_ref:
            raise ValueError(f"registry.hooks[{name!r}].spec must be 'module:object'")
        impl_ref = entry.get("impl")
        if not isinstance(impl_ref, str) or ":" not in impl_ref:
            raise ValueError(f"registry.hooks[{name!r}].impl must be 'module:object'")

    return {
        "functions": dict(functions),
        "constants": dict(constants),
        "ops": {k: dict(v) for k, v in ops.items()},
        "renderers": {k: dict(v) for k, v in renderers.items()},
        "sinks": {k: dict(v) for k, v in sinks.items()},
        "sources": {k: dict(v) for k, v in sources.items()},
        "transforms": {k: dict(v) for k, v in transforms.items()},
        "observers": {k: dict(v) for k, v in observers.items()},
        "backends": {k: dict(v) for k, v in backends.items()},
        "hooks": {k: dict(v) for k, v in hooks.items()},
    }
