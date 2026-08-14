from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from .defaults import (
    DEFAULT_DATASET_EVENTTYPE,
    DEFAULT_JOIN_ON_MISMATCH,
    DEFAULT_ROOT_TREE,
    DEFAULT_STREAM_TYPE,
)


def _nonempty_str(x: Any, where: str) -> str:
    if not isinstance(x, str) or not x.strip():
        raise ValueError(f"{where} must be a non-empty string")
    return x.strip()


def _list_of_str(x: Any, where: str) -> list[str]:
    if not isinstance(x, list) or not x:
        raise ValueError(f"{where} must be a non-empty list")
    out: list[str] = []
    for i, v in enumerate(x):
        if not isinstance(v, str) or not v.strip():
            raise ValueError(f"{where}[{i}] must be a non-empty string")
        out.append(v)
    return out


@dataclass(frozen=True)
class DatasetSpec:
    name: str
    files: list[str]
    nevents: str | None = None
    eventtype: str = DEFAULT_DATASET_EVENTTYPE
    group: str | None = None
    meta: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", _nonempty_str(self.name, "dataset.name"))
        object.__setattr__(
            self, "files", _list_of_str(self.files, f"dataset[{self.name}].files")
        )
        if self.nevents is not None:
            object.__setattr__(self, "nevents", str(self.nevents))
        object.__setattr__(
            self,
            "eventtype",
            _nonempty_str(self.eventtype, f"dataset[{self.name}].eventtype"),
        )

        group = self.group if self.group is not None else self.name
        object.__setattr__(
            self, "group", _nonempty_str(group, f"dataset[{self.name}].group")
        )

        if not isinstance(self.meta, dict):
            raise ValueError(f"dataset[{self.name}].meta must be a mapping")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class DataBlock:
    defaults: dict[str, Any] = field(default_factory=dict)
    datasets: list[DatasetSpec] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not isinstance(self.defaults, dict):
            raise ValueError("data.defaults must be a mapping")
        if not isinstance(self.datasets, list):
            raise ValueError("data.datasets must be a list")
        if not self.datasets:
            # you can decide if empty datasets is allowed; normalization currently allows it
            pass

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
        # dataclasses will already convert DatasetSpec -> dict


@dataclass(frozen=True)
class RootTreeSourceSpec:
    tree: str
    stream_type: str = DEFAULT_STREAM_TYPE
    kind: str = "root_tree"

    def __post_init__(self) -> None:
        k = _nonempty_str(self.kind, "source.kind")
        if k != "root_tree":
            raise ValueError(f"source.kind must be 'root_tree', got {k!r}")
        object.__setattr__(self, "kind", "root_tree")
        object.__setattr__(self, "tree", _nonempty_str(self.tree, "source.tree"))
        object.__setattr__(
            self, "stream_type", _nonempty_str(self.stream_type, "source.stream_type")
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class JoinInputSpec:
    source: str
    prefix: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "source", _nonempty_str(self.source, "join.inputs[].source")
        )
        object.__setattr__(
            self, "prefix", _nonempty_str(self.prefix, "join.inputs[].prefix")
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ZipJoinSpec:
    inputs: list[JoinInputSpec]
    on_mismatch: str = DEFAULT_JOIN_ON_MISMATCH
    kind: str = "zip"

    def __post_init__(self) -> None:
        k = _nonempty_str(self.kind, "join.kind")
        if k != "zip":
            raise ValueError(f"join.kind must be 'zip', got {k!r}")
        object.__setattr__(self, "kind", "zip")

        if not isinstance(self.inputs, list) or not self.inputs:
            raise ValueError("join.inputs must be a non-empty list")

        object.__setattr__(
            self, "on_mismatch", _nonempty_str(self.on_mismatch, "join.on_mismatch")
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class FieldSpec:
    stream: str
    branch: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "stream", _nonempty_str(self.stream, "fields.*.stream")
        )
        object.__setattr__(
            self, "branch", _nonempty_str(self.branch, "fields.*.branch")
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class SystematicApplicability:
    eventtypes: list[str] = field(default_factory=list)
    datasets: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "eventtypes",
            _optional_list_of_str(self.eventtypes, "systematics.applies_to.eventtypes"),
        )
        object.__setattr__(
            self,
            "datasets",
            _optional_list_of_str(self.datasets, "systematics.applies_to.datasets"),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class SystematicWeightRule:
    multiply: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "multiply",
            _optional_list_of_str(self.multiply, "systematics.weight.multiply"),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class SystematicVariation:
    name: str
    mode: str = "plan"
    group: str | None = None
    direction: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    applies_to: SystematicApplicability = field(default_factory=SystematicApplicability)
    requires: list[str] = field(default_factory=list)
    weight: SystematicWeightRule = field(default_factory=SystematicWeightRule)
    replace: dict[str, str] = field(default_factory=dict)
    datasets: dict[str, Any] = field(default_factory=dict)
    anchor: str | None = None
    patch: dict[str, Any] = field(default_factory=dict)
    stop_before: list[str] = field(default_factory=list)
    export: dict[str, str] = field(default_factory=dict)
    matrix_origin: dict[str, Any] = field(default_factory=dict)
    matrix_values: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "name", _nonempty_str(self.name, "systematics.variations[].name")
        )
        mode = _nonempty_str(self.mode, f"systematics.variations[{self.name}].mode")
        if mode not in {"plan", "inline"}:
            raise ValueError(
                f"systematics.variations[{self.name}].mode must be 'plan' or 'inline'"
            )
        object.__setattr__(self, "mode", mode)
        if self.group is not None:
            object.__setattr__(
                self,
                "group",
                _nonempty_str(self.group, f"systematics.variations[{self.name}].group"),
            )
        if self.direction is not None:
            object.__setattr__(
                self,
                "direction",
                _nonempty_str(
                    self.direction, f"systematics.variations[{self.name}].direction"
                ),
            )
        if not isinstance(self.metadata, dict):
            raise ValueError(
                f"systematics.variations[{self.name}].metadata must be a mapping"
            )
        object.__setattr__(
            self,
            "requires",
            _optional_list_of_str(
                self.requires, f"systematics.variations[{self.name}].requires"
            ),
        )
        if not isinstance(self.replace, dict):
            raise ValueError(
                f"systematics.variations[{self.name}].replace must be a mapping"
            )
        for key, value in self.replace.items():
            if not isinstance(key, str) or not key.strip():
                raise ValueError(
                    f"systematics.variations[{self.name}].replace keys must be non-empty strings"
                )
            if not isinstance(value, str) or not value.strip():
                raise ValueError(
                    f"systematics.variations[{self.name}].replace[{key!r}] must be a non-empty string"
                )
        if not isinstance(self.datasets, dict):
            raise ValueError(
                f"systematics.variations[{self.name}].datasets must be a mapping"
            )
        if self.anchor is not None:
            object.__setattr__(
                self,
                "anchor",
                _nonempty_str(self.anchor, f"systematics.variations[{self.name}].anchor"),
            )
        if self.mode == "inline" and self.anchor is None:
            raise ValueError(
                f"systematics.variations[{self.name}].anchor is required for inline variations"
            )
        if not isinstance(self.patch, dict):
            raise ValueError(
                f"systematics.variations[{self.name}].patch must be a mapping"
            )
        if not isinstance(self.export, dict):
            raise ValueError(
                f"systematics.variations[{self.name}].export must be a mapping"
            )
        if not isinstance(self.matrix_origin, dict):
            raise ValueError(
                f"systematics.variations[{self.name}].matrix_origin must be a mapping"
            )
        if not isinstance(self.matrix_values, dict):
            raise ValueError(
                f"systematics.variations[{self.name}].matrix_values must be a mapping"
            )
        for key, value in self.export.items():
            if not isinstance(key, str) or not key.strip():
                raise ValueError(
                    f"systematics.variations[{self.name}].export keys must be non-empty strings"
                )
            if not isinstance(value, str) or not value.strip():
                raise ValueError(
                    f"systematics.variations[{self.name}].export[{key!r}] must be a non-empty string"
                )
        object.__setattr__(
            self,
            "stop_before",
            _optional_list_of_str(
                self.stop_before,
                f"systematics.variations[{self.name}].stop_before",
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {"name": self.name}
        if self.mode != "plan":
            data["mode"] = self.mode
        if self.group is not None:
            data["group"] = self.group
        if self.direction is not None:
            data["direction"] = self.direction
        if self.metadata:
            data["metadata"] = dict(self.metadata)
        data["applies_to"] = self.applies_to.to_dict()
        data["requires"] = list(self.requires)
        data["weight"] = self.weight.to_dict()
        data["replace"] = dict(self.replace)
        data["datasets"] = dict(self.datasets)
        if self.anchor is not None:
            data["anchor"] = self.anchor
        if self.patch:
            data["patch"] = dict(self.patch)
        if self.stop_before:
            data["stop_before"] = list(self.stop_before)
        if self.export:
            data["export"] = dict(self.export)
        if self.matrix_origin:
            data["matrix_origin"] = dict(self.matrix_origin)
        if self.matrix_values:
            data["matrix_values"] = dict(self.matrix_values)
        return data


@dataclass(frozen=True)
class SystematicsConfig:
    include_nominal: bool = False
    profiles: list[str] = field(default_factory=list)
    variations: list[SystematicVariation] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not isinstance(self.include_nominal, bool):
            raise ValueError("systematics.include_nominal must be a boolean")
        object.__setattr__(
            self,
            "profiles",
            _optional_list_of_str(self.profiles, "systematics.profiles"),
        )
        if not isinstance(self.variations, list):
            raise ValueError("systematics.variations must be a list")

    def to_dict(self) -> dict[str, Any]:
        return {
            "include_nominal": self.include_nominal,
            "profiles": list(self.profiles),
            "variations": [variation.to_dict() for variation in self.variations],
        }


@dataclass(frozen=True)
class NormalizedWorkflow:
    version: str
    data: DataBlock
    sources: dict[str, dict[str, Any]] = field(default_factory=dict)
    joins: dict[str, ZipJoinSpec] = field(default_factory=dict)
    styles: dict[str, dict[str, Any]] = field(default_factory=dict)
    outputs: dict[str, dict[str, Any]] = field(default_factory=dict)
    fields: dict[str, FieldSpec] = field(default_factory=dict)
    observers: list[dict[str, Any]] = field(default_factory=list)
    reports: list[dict[str, Any]] = field(default_factory=list)
    analysis: dict[str, Any] = field(default_factory=dict)
    primary_stream: str | None = None
    use: dict[str, Any] = field(default_factory=dict)
    execution: dict[str, Any] = field(default_factory=dict)
    registry: dict[str, Any] = field(default_factory=dict)
    systematics: SystematicsConfig | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "version", _nonempty_str(self.version, "version"))
        if not isinstance(self.analysis, dict):
            raise ValueError("analysis must be a mapping")
        if self.primary_stream is not None:
            object.__setattr__(
                self,
                "primary_stream",
                _nonempty_str(self.primary_stream, "primary_stream"),
            )

        if not isinstance(self.styles, dict):
            raise ValueError("styles must be a mapping")
        if not isinstance(self.outputs, dict):
            raise ValueError("outputs must be a mapping")
        if not isinstance(self.observers, list):
            raise ValueError("observers must be a list")
        if not isinstance(self.reports, list):
            raise ValueError("reports must be a list")
        if not isinstance(self.use, dict):
            raise ValueError("use must be a mapping")
        if not isinstance(self.execution, dict):
            raise ValueError("execution must be a mapping")
        if self.systematics is not None and not isinstance(
            self.systematics, SystematicsConfig
        ):
            raise ValueError("systematics must be a SystematicsConfig")

        for k, v in self.styles.items():
            if not isinstance(k, str) or not k:
                raise ValueError("styles keys must be non-empty strings")
            if not isinstance(v, dict):
                raise ValueError(f"styles.{k} must be a mapping")

        for k, v in self.outputs.items():
            if not isinstance(k, str) or not k.strip():
                raise ValueError("outputs keys must be non-empty strings")
            if not isinstance(v, dict):
                raise ValueError(f"outputs.{k} must be a mapping")

        for idx, observer in enumerate(self.observers):
            if not isinstance(observer, dict):
                raise ValueError(f"observers[{idx}] must be a mapping")
        for idx, report in enumerate(self.reports):
            if not isinstance(report, dict):
                raise ValueError(f"reports[{idx}] must be a mapping")

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        if self.systematics is None:
            data.pop("systematics", None)
        else:
            data["systematics"] = self.systematics.to_dict()
        return data


# --- helpers used by normalize.py ---


def inject_default_events_source(data_defaults: dict[str, Any]) -> RootTreeSourceSpec:
    tree = str(data_defaults.get("tree_primary", DEFAULT_ROOT_TREE))
    return RootTreeSourceSpec(tree=tree, stream_type=DEFAULT_STREAM_TYPE)


def _optional_list_of_str(x: Any, where: str) -> list[str]:
    if not isinstance(x, list):
        raise ValueError(f"{where} must be a list")
    out: list[str] = []
    for i, v in enumerate(x):
        if not isinstance(v, str) or not v.strip():
            raise ValueError(f"{where}[{i}] must be a non-empty string")
        out.append(v)
    return out
