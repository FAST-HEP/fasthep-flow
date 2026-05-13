from __future__ import annotations

from dataclasses import asdict, dataclass, field, replace
from enum import StrEnum
from pathlib import Path
from typing import Any


class SelectionOp(StrEnum):
    ALL = "all"
    ANY = "any"
    CUT = "cut"
    REDUCE = "reduce"


@dataclass(frozen=True)
class SelectionStats:
    """
    Cumulative (in -> out) stats for a selection node.

    For unweighted workflows, set sumw_* == n_* and sumw2_* == n_*.
    """
    n_in: int
    n_out: int
    sumw_in: float
    sumw_out: float
    sumw2_in: float
    sumw2_out: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @staticmethod
    def from_dict(d: dict[str, Any]) -> SelectionStats:
        return SelectionStats(
            n_in=int(d.get("n_in", 0)),
            n_out=int(d.get("n_out", 0)),
            sumw_in=float(d.get("sumw_in", 0.0)),
            sumw_out=float(d.get("sumw_out", 0.0)),
            sumw2_in=float(d.get("sumw2_in", 0.0)),
            sumw2_out=float(d.get("sumw2_out", 0.0)),
        )

    def add(self, other: SelectionStats) -> SelectionStats:
        return SelectionStats(
            n_in=self.n_in + other.n_in,
            n_out=self.n_out + other.n_out,
            sumw_in=self.sumw_in + other.sumw_in,
            sumw_out=self.sumw_out + other.sumw_out,
            sumw2_in=self.sumw2_in + other.sumw2_in,
            sumw2_out=self.sumw2_out + other.sumw2_out,
        )


@dataclass(frozen=True)
class SelectionNode:
    """
    A self-describing, order-preserving tree of selection instructions.

    - op=all/any: has children in `items`
    - op=cut: has `expr`
    - op=reduce: has `reduce` dict (e.g. {"op":"any","over":"Muon_Pt > 25"})
    """
    op: SelectionOp
    stats: SelectionStats

    # Leaf payloads:
    expr: str | None = None
    reduce: dict[str, Any] | None = None

    # Composite payload:
    items: tuple[SelectionNode, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        # Minimal structural validation.
        if self.op in (SelectionOp.ALL, SelectionOp.ANY):
            if not self.items:
                raise ValueError(
                    f"SelectionNode op={self.op} requires non-empty items")
            if self.expr is not None or self.reduce is not None:
                raise ValueError(
                    f"SelectionNode op={self.op} cannot set expr/reduce")
        elif self.op == SelectionOp.CUT:
            if not (isinstance(self.expr, str) and self.expr.strip()):
                raise ValueError(
                    "SelectionNode op=cut requires non-empty expr")
            if self.items:
                raise ValueError("SelectionNode op=cut cannot have items")
            if self.reduce is not None:
                raise ValueError("SelectionNode op=cut cannot set reduce")
        elif self.op == SelectionOp.REDUCE:
            if not isinstance(self.reduce, dict) or not self.reduce:
                raise ValueError(
                    "SelectionNode op=reduce requires a reduce mapping")
            if self.items:
                raise ValueError("SelectionNode op=reduce cannot have items")
            if self.expr is not None:
                raise ValueError("SelectionNode op=reduce cannot set expr")
        else:
            raise ValueError(f"Unknown SelectionOp: {self.op}")

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "op": self.op.value,
            "stats": self.stats.to_dict(),
        }
        if self.expr is not None:
            d["expr"] = self.expr
        if self.reduce is not None:
            d["reduce"] = dict(self.reduce)
        if self.items:
            d["items"] = [x.to_dict() for x in self.items]
        return d

    @staticmethod
    def from_dict(d: dict[str, Any]) -> SelectionNode:
        op = SelectionOp(str(d.get("op", "")).lower())
        stats = SelectionStats.from_dict(d.get("stats") or {})
        expr = d.get("expr")
        reduce_ = d.get("reduce")
        items_raw = d.get("items") or []
        items = tuple(SelectionNode.from_dict(x)
                      for x in items_raw) if items_raw else ()
        return SelectionNode(op=op, stats=stats, expr=expr, reduce=reduce_, items=items)


@dataclass(frozen=True)
class CutflowSummary:
    n_in: int
    n_out: int
    sumw_in: float
    sumw_out: float
    eff: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @staticmethod
    def from_dict(d: dict[str, Any]) -> CutflowSummary:
        eff = d.get("eff")
        return CutflowSummary(
            n_in=int(d.get("n_in", 0)),
            n_out=int(d.get("n_out", 0)),
            sumw_in=float(d.get("sumw_in", 0.0)),
            sumw_out=float(d.get("sumw_out", 0.0)),
            eff=None if eff is None else float(eff),
        )


@dataclass(frozen=True)
class CutflowPaths:

    @staticmethod
    def part_path(results_dir: str, stage: str, dataset: str, part: str) -> str:
        return str(Path(results_dir) / "cutflows" / stage / dataset / f"{part}.cutflow.json")

    @staticmethod
    def dataset_path(results_dir: str, stage: str, dataset: str) -> str:
        return str(Path(results_dir) / "cutflows" / stage / dataset / "cutflow.json")

    @staticmethod
    def collection_path(results_dir: str, stage: str) -> str:
        return str(Path(results_dir) / "cutflows" / stage / "collection.json")


@dataclass(frozen=True)
class CutflowReport:
    """
    One cutflow report for ONE stage and ONE dataset.

    This is the unit of merging (partition -> dataset).
    """
    schema: str = "hepflow.cutflow.v1"
    stage: str = ""
    dataset: str = ""
    weight_expr: str | None = None

    selection: SelectionNode = field(default_factory=lambda: SelectionNode(
        op=SelectionOp.ALL,
        stats=SelectionStats(0, 0, 0.0, 0.0, 0.0, 0.0),
        items=(SelectionNode(op=SelectionOp.CUT, expr="__dummy__", reduce=None,
                             stats=SelectionStats(0, 0, 0.0, 0.0, 0.0, 0.0),
                             items=()),)
    ))  # overridden by from_dict in practice

    summary: CutflowSummary | None = None

    # meta is intentionally open-ended (provenance now, renderers later)
    meta: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.schema != "hepflow.cutflow.v1":
            raise ValueError(f"Unsupported cutflow schema: {self.schema!r}")
        if not (isinstance(self.stage, str) and self.stage.strip()):
            raise ValueError("CutflowReport.stage must be a non-empty string")
        if not (isinstance(self.dataset, str) and self.dataset.strip()):
            raise ValueError(
                "CutflowReport.dataset must be a non-empty string")

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "schema": self.schema,
            "stage": self.stage,
            "dataset": self.dataset,
            "weight_expr": self.weight_expr,
            "selection": self.selection.to_dict(),
            "meta": dict(self.meta) if self.meta else {},
        }
        if self.summary is not None:
            d["summary"] = self.summary.to_dict()
        return d

    @staticmethod
    def from_dict(d: dict[str, Any]) -> CutflowReport:
        selection = SelectionNode.from_dict(d.get("selection") or {})
        summary = d.get("summary")
        return CutflowReport(
            schema=str(d.get("schema", "hepflow.cutflow.v1")),
            stage=str(d.get("stage", "")),
            dataset=str(d.get("dataset", "")),
            weight_expr=d.get("weight_expr"),
            selection=selection,
            summary=None if summary is None else CutflowSummary.from_dict(
                summary),
            meta=dict(d.get("meta") or {}),
        )


@dataclass(frozen=True)
class CutflowCollection:
    """
    Index file that groups per-dataset cutflow reports for a stage.
    """
    schema: str = "hepflow.cutflow_collection.v1"
    stage: str = ""
    datasets: tuple[str, ...] = field(default_factory=tuple)
    paths: dict[str, str] = field(default_factory=dict)
    meta: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.schema != "hepflow.cutflow_collection.v1":
            raise ValueError(
                f"Unsupported cutflow collection schema: {self.schema!r}")
        if not (isinstance(self.stage, str) and self.stage.strip()):
            raise ValueError(
                "CutflowCollection.stage must be a non-empty string")
        # ensure deterministic order
        object.__setattr__(self, "datasets", tuple(self.datasets))

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "stage": self.stage,
            "datasets": list(self.datasets),
            "paths": dict(self.paths),
            "meta": dict(self.meta) if self.meta else {},
        }

    @staticmethod
    def from_dict(d: dict[str, Any]) -> CutflowCollection:
        return CutflowCollection(
            schema=str(d.get("schema", "hepflow.cutflow_collection.v1")),
            stage=str(d.get("stage", "")),
            datasets=tuple(str(x) for x in (d.get("datasets") or [])),
            paths=dict(d.get("paths") or {}),
            meta=dict(d.get("meta") or {}),
        )


# ----------------------------
# Strict merge utilities
# ----------------------------

def _assert_same_leaf_identity(a: SelectionNode, b: SelectionNode) -> None:
    if a.op != b.op:
        raise ValueError(f"Selection shape mismatch: op {a.op} != {b.op}")
    if a.op == SelectionOp.CUT and (a.expr or "") != (b.expr or ""):
        raise ValueError(
            f"Selection shape mismatch: cut expr differs: {a.expr!r} != {b.expr!r}")
    if a.op == SelectionOp.REDUCE and (a.reduce or {}) != (b.reduce or {}):
        raise ValueError(
            f"Selection shape mismatch: reduce differs: {a.reduce!r} != {b.reduce!r}")


def _merge_selection_nodes(a: SelectionNode, b: SelectionNode) -> SelectionNode:
    _assert_same_leaf_identity(a, b)

    if a.op in (SelectionOp.ALL, SelectionOp.ANY):
        if len(a.items) != len(b.items):
            raise ValueError(
                f"Selection shape mismatch: {a.op} items len differs: {len(a.items)} != {len(b.items)}")
        merged_items = tuple(_merge_selection_nodes(x, y)
                             for x, y in zip(a.items, b.items, strict=False))
        return replace(a, stats=a.stats.add(b.stats), items=merged_items)

    # leaf
    return replace(a, stats=a.stats.add(b.stats))


def merge_cutflow_reports(reports: list[CutflowReport]) -> CutflowReport:
    """
    Merge partition-level reports into one dataset-level report.
    Strict: requires identical selection tree shape and identities.
    """
    if not reports:
        raise ValueError("merge_cutflow_reports: no reports provided")

    first = reports[0]
    for r in reports[1:]:
        if r.schema != first.schema:
            raise ValueError("Cutflow schema mismatch")
        if r.stage != first.stage:
            raise ValueError("Cutflow stage mismatch")
        if r.dataset != first.dataset:
            raise ValueError("Cutflow dataset mismatch")
        if (r.weight_expr or None) != (first.weight_expr or None):
            raise ValueError("Cutflow weight_expr mismatch")

    sel = first.selection
    for r in reports[1:]:
        sel = _merge_selection_nodes(sel, r.selection)

    # recompute summary from root stats (optional but handy)
    root = sel.stats
    eff = (root.n_out / root.n_in) if root.n_in else None
    summary = CutflowSummary(
        n_in=root.n_in,
        n_out=root.n_out,
        sumw_in=root.sumw_in,
        sumw_out=root.sumw_out,
        eff=eff,
    )

    # meta: keep first, but you can append provenance later
    meta = dict(first.meta) if first.meta else {}
    meta.setdefault("merged_partitions", len(reports))

    return CutflowReport(
        stage=first.stage,
        dataset=first.dataset,
        weight_expr=first.weight_expr,
        selection=sel,
        summary=summary,
        meta=meta,
    )
