# hepflow/model/author.py
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
class NormalizedAuthor:
    version: str
    data: DataBlock
    sources: dict[str, RootTreeSourceSpec] = field(default_factory=dict)
    joins: dict[str, ZipJoinSpec] = field(default_factory=dict)
    styles: dict[str, dict[str, Any]] = field(default_factory=dict)
    fields: dict[str, FieldSpec] = field(default_factory=dict)
    observers: list[dict[str, Any]] = field(default_factory=list)
    analysis: dict[str, Any] = field(default_factory=dict)
    primary_stream: str | None = None
    use: dict[str, Any] = field(default_factory=dict)
    execution: dict[str, Any] = field(default_factory=dict)
    registry: dict[str, Any] = None

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
        if not isinstance(self.observers, list):
            raise ValueError("observers must be a list")
        if not isinstance(self.use, dict):
            raise ValueError("use must be a mapping")
        if not isinstance(self.execution, dict):
            raise ValueError("execution must be a mapping")

        for k, v in self.styles.items():
            if not isinstance(k, str) or not k:
                raise ValueError("styles keys must be non-empty strings")
            if not isinstance(v, dict):
                raise ValueError(f"styles.{k} must be a mapping")

        for idx, observer in enumerate(self.observers):
            if not isinstance(observer, dict):
                raise ValueError(f"observers[{idx}] must be a mapping")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# --- helpers used by normalize.py ---


def inject_default_events_source(data_defaults: dict[str, Any]) -> RootTreeSourceSpec:
    tree = str(data_defaults.get("tree_primary", DEFAULT_ROOT_TREE))
    return RootTreeSourceSpec(tree=tree, stream_type=DEFAULT_STREAM_TYPE)
