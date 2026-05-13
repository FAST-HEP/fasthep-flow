# hepflow/model/plan.py
from __future__ import annotations

import os
from dataclasses import asdict, dataclass, field
from typing import Any, Literal

from hepflow.model.defaults import (
    DEFAULT_RESULTS_DIR,
    DEFAULT_WORK_DIR,
)
from hepflow.model.deps import RequiredInput
from hepflow.model.ir import InputRef

# ---- Core plan pieces ----


@dataclass(frozen=True)
class Paths:
    work: str = DEFAULT_WORK_DIR
    results: str = DEFAULT_RESULTS_DIR

    def resolve(
        self, *, base_dir: str | None = None
    ) -> tuple[Paths, dict[str, Any]]:
        report: dict[str, Any] = {"changed": False, "notes": []}

        def _abs(p: str, *, rel_to: str | None) -> str:
            if os.path.isabs(p):
                return os.path.normpath(p)
            root = rel_to or os.getcwd()
            return os.path.normpath(os.path.abspath(os.path.join(root, p)))

        base = os.path.abspath(base_dir or os.getcwd())

        # Resolve work relative to base
        work_abs = _abs(self.work, rel_to=base)
        if work_abs != self.work:
            report["changed"] = True
            report["notes"].append(
                f"paths.work made absolute: {self.work} -> {work_abs}"
            )

        # Resolve results
        if os.path.isabs(self.results):
            results_abs = os.path.normpath(self.results)
        else:
            # Guard: if user already wrote results like "<work>/something", don't re-prefix with work again.
            work_rel_norm = os.path.normpath(str(self.work))
            results_rel_norm = os.path.normpath(str(self.results))

            looks_prefixed_by_work = (
                work_rel_norm
                and work_rel_norm not in (".", os.sep)
                and (
                    results_rel_norm == work_rel_norm
                    or results_rel_norm.startswith(work_rel_norm + os.sep)
                )
            )

            if looks_prefixed_by_work:
                # interpret results relative to base, not relative-to-work
                results_abs = _abs(self.results, rel_to=base)
                report["changed"] = True
                report["notes"].append(
                    "paths.results looked already prefixed by work; "
                    f"interpreting relative to base_dir instead of work: {self.results} -> {results_abs}"
                )
            else:
                results_abs = _abs(self.results, rel_to=work_abs)
                if results_abs != self.results:
                    report["changed"] = True
                    report["notes"].append(
                        f"paths.results resolved relative to work: {self.results} -> {results_abs}"
                    )

        return Paths(work=work_abs, results=results_abs), report

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class DatasetEntry:
    files: list[str]
    nevents: int
    eventtype: str = "mc"
    group: str = ""
    meta: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class Partition:
    dataset: str
    file: str
    part: str
    start: int
    stop: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ProductPlan:
    id: str
    kind: str  # "hist" | "cutflow" (v1)
    ext: str  # "pkl" | "json"
    ir_node: str
    ir_port: str
    map: dict[str, Any]
    reduce: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RenderPlan:
    id: str
    when: str
    input: dict[str, Any]
    output: str
    params: dict[str, Any] = field(default_factory=dict)
    op: str = "hep.render.plot"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class NodeDeps:
    requires: tuple[str, ...] = ()
    provides: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {"requires": list(self.requires), "provides": list(self.provides)}


@dataclass(frozen=True)
class PlanDeps:
    context_symbols: tuple[str, ...] = ()
    external_symbols: tuple[str, ...] = ()

    # optional but often useful later:
    unresolved_external_symbols: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        d = {
            "context_symbols": list(self.context_symbols),
            "external_symbols": list(self.external_symbols),
        }
        if self.unresolved_external_symbols:
            d["unresolved_external_symbols"] = list(self.unresolved_external_symbols)
        return d


@dataclass(frozen=True)
class ExecNode:
    id: str
    op: str
    in_: tuple[InputRef, ...] = ()
    params: dict[str, Any] = field(default_factory=dict)
    out: dict[str, str] = field(default_factory=dict)
    deps: NodeDeps = field(default_factory=NodeDeps)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "op": self.op,
            "in": [x.to_dict() for x in self.in_],
            "params": self.params,
            "out": self.out,
            "deps": self.deps.to_dict(),
        }


@dataclass(frozen=True)
class Plan:
    version: str = "2.1"
    paths: Paths = field(default_factory=Paths)

    datasets: dict[str, DatasetEntry] = field(default_factory=dict)
    partitions: list[Partition] = field(default_factory=list)

    primary_stream: str = "events"
    streams: dict[str, Any] = field(default_factory=dict)

    # required_inputs comes from Deps (stream_id -> RequiredInput dict)
    required_inputs: dict[str, RequiredInput] = field(default_factory=dict)

    exec_graph: tuple[ExecNode, ...] = field(default_factory=tuple)
    deps: PlanDeps = field(default_factory=PlanDeps)

    products: list[ProductPlan] = field(default_factory=list)
    renders: list[RenderPlan] = field(default_factory=list)

    # Optional debug helpers (cheap, but helpful)
    fieldmap: dict[str, Any] = field(default_factory=dict)

    reports: dict[str, Any] = field(default_factory=dict)

    globals: dict[str, Any] = field(default_factory=dict)

    registry: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = asdict(self)

        # Override pieces where asdict() loses our desired structure/types:
        d["required_inputs"] = {
            sid: {"kind": ri.kind, "tree": ri.tree, "branches": list(ri.branches)}
            for sid, ri in self.required_inputs.items()
        }
        d["exec_graph"] = [n.to_dict() for n in self.exec_graph]
        d["deps"] = self.deps.to_dict()

        return d


Scope = Literal["partition", "dataset", "global"]
MaterializeMode = Literal["never", "if_requested", "always"]


@dataclass(slots=True, frozen=True)
class PlanInputRef:
    node_id: str
    output_name: str
    input_name: str

    def to_dict(self) -> dict[str, str]:
        return {
            "node_id": self.node_id,
            "output_name": self.output_name,
            "input_name": self.input_name,
        }


@dataclass(slots=True)
class PartitionSpec:
    """
    Backend-neutral partitioning intent.

    mode:
      - none: unpartitioned/global
      - dataset: one unit per dataset
      - dataset_chunks: dataset split into entry chunks
    """

    mode: Literal["none", "dataset", "dataset_chunks"] = "none"
    chunk_size: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "chunk_size": self.chunk_size,
        }


@dataclass(slots=True)
class ExecutionNode:
    """
    Backend-neutral execution unit description.
    """

    id: str
    graph_node_id: str
    role: Literal["source", "transform", "observer", "sink"]
    impl: str

    inputs: list[PlanInputRef] = field(default_factory=list)
    params: dict[str, Any] = field(default_factory=dict)
    outputs: dict[str, str] = field(default_factory=dict)

    input_scope: Scope = "global"
    output_scope: Scope = "global"
    partitioning: PartitionSpec = field(default_factory=PartitionSpec)

    materialize: MaterializeMode = "never"

    meta: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "graph_node_id": self.graph_node_id,
            "role": self.role,
            "impl": self.impl,
            "inputs": [item.to_dict() for item in self.inputs],
            "params": self.params,
            "outputs": self.outputs,
            "input_scope": self.input_scope,
            "output_scope": self.output_scope,
            "partitioning": self.partitioning.to_dict(),
            "materialize": self.materialize,
            "meta": self.meta,
        }


@dataclass(slots=True)
class ExecutionPlan:
    nodes: list[ExecutionNode] = field(default_factory=list)
    node_index: dict[str, ExecutionNode] = field(default_factory=dict)
    partitions: list[ExecutionPartition] = field(default_factory=list)
    context: dict[str, Any] = field(default_factory=dict)
    registry: dict[str, Any] = field(default_factory=dict)
    provenance: dict[str, Any] = field(default_factory=dict)
    execution: dict[str, Any] = field(
        default_factory=lambda: {
            "backend": "local",
            "strategy": "default",
            "config": {},
        }
    )
    execution_hooks: list[dict[str, Any]] = field(default_factory=list)
    data_flow: dict[str, Any] = field(default_factory=dict)

    def add_node(self, node: ExecutionNode) -> None:
        if node.id in self.node_index:
            raise ValueError(f"Duplicate execution node id: {node.id}")
        self.nodes.append(node)
        self.node_index[node.id] = node

    def get_node(self, node_id: str) -> ExecutionNode:
        try:
            return self.node_index[node_id]
        except KeyError as exc:
            raise KeyError(f"Unknown execution node id: {node_id}") from exc

    def to_dict(self) -> dict[str, Any]:
        return {
            "context": self.context,
            "nodes": [node.to_dict() for node in self.nodes],
            "partitions": [partition.to_dict() for partition in self.partitions],
            "registry": self.registry,
            "provenance": self.provenance,
            "execution": self.execution,
            "execution_hooks": self.execution_hooks,
            "data_flow": self.data_flow,
        }


def resolve_plan_ref(ref: str, plan: ExecutionPlan) -> Any:
    parts = str(ref).split(".")
    if not parts or parts[0] != "context":
        raise ValueError(f"Unsupported plan ref {ref!r}; only context.* is supported")

    current: Any = plan.context
    for part in parts[1:]:
        if not isinstance(current, dict):
            raise KeyError(f"Plan ref {ref!r} cannot be resolved through non-mapping segment {part!r}")
        try:
            current = current[part]
        except KeyError as exc:
            raise KeyError(f"Plan ref {ref!r} missing segment {part!r}") from exc
    return current


@dataclass(slots=True, frozen=True)
class ExecutionPartition:
    id: str
    dataset: str
    file: str
    source: str
    part: str
    start: int | None = None
    stop: int | None = None

    def to_context(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "dataset": self.dataset,
            "file": self.file,
            "source": self.source,
            "part": self.part,
            "start": self.start,
            "stop": self.stop,
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "dataset": self.dataset,
            "file": self.file,
            "source": self.source,
            "part": self.part,
            "start": self.start,
            "stop": self.stop,
        }
