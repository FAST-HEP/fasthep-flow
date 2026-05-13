from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from hepflow.model.render import RenderAttempt


@dataclass(frozen=True)
class ReportMessage:
    level: str  # "warn" | "error" | "info"
    code: str   # stable identifier e.g. "RENDER_MISSING"
    message: str
    meta: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class DatasetReport:
    name: str
    eventtype: str
    files: tuple[str, ...]
    nevents: int | None = None
    nevents_source: str = "missing"  # "author" | "inferred" | "missing"
    total_entries: int | None = None

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["files"] = list(self.files)
        return d


@dataclass(frozen=True)
class StreamReport:
    stream_id: str
    kind: str  # "root_tree" | "zip_join"
    tree: str | None = None
    inputs: tuple[dict[str, str], ...] | None = None  # for joins
    on_mismatch: str | None = None

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        if self.inputs is not None:
            d["inputs"] = list(self.inputs)
        return d


@dataclass(frozen=True)
class RenderExecutionReport:
    # summary
    total: int
    rendered: int
    skipped: int
    failed: int

    # “audit”: planned renders + their outcomes
    attempts: tuple[RenderAttempt, ...] = ()

    # renderable products that had NO render block (helps the “why no png?” UX)
    missing_renders: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

@dataclass(frozen=True)
class RenderReport:
    render_id: str
    kind: str
    when: str
    product: str
    output: str
    style: str | None = None
    spec_path: str | None = None

    # optional: filled by compiler
    status: str = "planned"  # "planned" | "skipped" | "compiled"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CompileReport:
    schema_version: str = "1"
    hepflow_version: str | None = None

    work_dir: str = ""
    results_dir: str = ""

    author_path: str | None = None

    primary_stream: str = ""
    streams: tuple[StreamReport, ...] = ()

    datasets: tuple[DatasetReport, ...] = ()

    # deps summary
    external_symbols: tuple[str, ...] = ()
    unresolved_external_symbols: tuple[str, ...] = ()

    required_inputs: dict[str, dict[str, Any]] = field(default_factory=dict)

    # products / renders (optional but useful)
    products: tuple[dict[str, Any], ...] = ()
    renders: tuple[RenderReport, ...] = ()

    messages: tuple[ReportMessage, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "hepflow_version": self.hepflow_version,
            "author_path": self.author_path,
            "work_dir": self.work_dir,
            "results_dir": self.results_dir,
            "primary_stream": self.primary_stream,
            "streams": [s.to_dict() for s in self.streams],
            "datasets": [d.to_dict() for d in self.datasets],
            "external_symbols": list(self.external_symbols),
            "unresolved_external_symbols": list(self.unresolved_external_symbols),
            "required_inputs": self.required_inputs,
            "products": [dict(p) for p in self.products],
            "renders": [r.to_dict() for r in self.renders],
            "messages": [m.to_dict() for m in self.messages],
        }
