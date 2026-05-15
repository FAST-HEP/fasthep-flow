from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from importlib import resources
from importlib.resources.abc import Traversable
from pathlib import Path
from typing import Any

import networkx as nx

from hepflow.backends.loaders import load_backend, normalize_backend_override
from hepflow.backends.model import BackendResult
from hepflow.compiler.includes import load_author_with_includes
from hepflow.compiler.lower_graph import lower_author_to_graph
from hepflow.compiler.normalize import normalize_author
from hepflow.compiler.plan import build_execution_plan
from hepflow.compiler.plan_diff import (
    diff_plans,
    format_plan_diff,
    load_plan_yaml,
)
from hepflow.compiler.profiles import (
    load_profile_config_with_provenance,
    load_profile_registry_layer,
    normalize_profile_names,
)
from hepflow.model.lifecycle import WHEN_ALIASES
from hepflow.model.plan import (
    ExecutionNode,
    ExecutionPartition,
    ExecutionPlan,
    PartitionSpec,
    PlanInputRef,
)
from hepflow.registry.defaults import (
    default_expr_registry_config,
    default_runtime_registry_config,
)
from hepflow.registry.merge import (
    RegistryLayer,
    RegistryMergeResult,
    merge_registry_layers,
)
from hepflow.utils import read_yaml, write_yaml

__all__ = [
    "InitResult",
    "compile_author_file",
    "diff_plan_files",
    "init_project",
    "load_plan_file",
    "make_plan_file",
    "normalise_author_file",
    "normalize_author_file",
    "run_author_file",
    "run_plan_file",
]


@dataclass(slots=True)
class InitResult:
    profile_dir: Path
    created_profile_dir: bool
    copied: list[Path] = field(default_factory=list)
    skipped_existing: list[Path] = field(default_factory=list)
    overwritten: list[Path] = field(default_factory=list)

    @property
    def written(self) -> list[Path]:
        return [*self.copied, *self.overwritten]

    def __iter__(self):
        return iter(self.written)

    def __len__(self) -> int:
        return len(self.written)

    def __contains__(self, path: object) -> bool:
        return path in self.written


def load_author_yaml(path: str | Path) -> dict[str, Any]:
    return load_author_with_includes(str(path)).doc


def init_project(
    *,
    target_dir: str | Path,
    force: bool = False,
) -> InitResult:
    """Create project-local profile templates from bundled flow profiles."""
    project_dir = Path(target_dir)
    profile_dir = project_dir / ".fasthep" / "profiles" / "hepflow"
    created_profile_dir = not profile_dir.exists()
    profile_dir.mkdir(parents=True, exist_ok=True)

    result = InitResult(
        profile_dir=profile_dir,
        created_profile_dir=created_profile_dir,
    )
    for relative_path, source in _packaged_profile_files():
        destination = profile_dir / relative_path
        exists = destination.exists()
        if exists and not force:
            result.skipped_existing.append(destination)
            continue
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(source.read_bytes())
        if exists:
            result.overwritten.append(destination)
        else:
            result.copied.append(destination)

    return result


def normalise_author_file(
    author_path: str | Path,
    *,
    outdir: str | Path,
) -> dict[str, Any]:
    """Normalise an author YAML file and write ``normalized.yaml``."""
    author_file = Path(author_path)
    out_path = Path(outdir)
    out_path.mkdir(parents=True, exist_ok=True)

    author = load_author_yaml(str(author_file))
    normalized = normalize_author(author)

    registry_result = resolve_author_registry(author, author_path=author_file)
    execution_result = resolve_author_execution(author, author_path=author_file)
    hooks_result = resolve_author_execution_hooks(author, author_path=author_file)

    normalized["registry"] = registry_result.registry
    normalized["execution"] = execution_result["execution"]
    normalized["execution_hooks"] = hooks_result["execution_hooks"]
    normalized.setdefault("provenance", {}).update(registry_result.provenance)
    normalized.setdefault("provenance", {}).update(execution_result["provenance"])
    normalized.setdefault("provenance", {}).update(hooks_result["provenance"])

    write_yaml(normalized, str(out_path / "normalized.yaml"))
    return normalized


normalize_author_file = normalise_author_file


def make_plan_file(
    normalized_path: str | Path,
    *,
    outdir: str | Path,
    chunk_size: int | None = None,
) -> ExecutionPlan:
    """Lower a normalized YAML file and write plan/graph artifacts."""
    normalized_file = Path(normalized_path)
    out_path = Path(outdir)
    out_path.mkdir(parents=True, exist_ok=True)

    normalized = read_yaml(str(normalized_file)) or {}
    graph = lower_author_to_graph(normalized)
    plan = build_execution_plan(
        graph,
        chunk_size=chunk_size,
        registry=dict(normalized.get("registry") or {}),
        provenance=dict(normalized.get("provenance") or {}),
        execution=dict(normalized.get("execution") or {}),
        execution_hooks=list(normalized.get("execution_hooks") or []),
    )

    write_graph_artifacts(graph, str(out_path))
    write_yaml(plan.to_dict(), str(out_path / "plan.yaml"))
    return plan


def compile_author_file(
    author_path: str | Path,
    *,
    outdir: str | Path,
    chunk_size: int | None = None,
) -> ExecutionPlan:
    """Normalise an author YAML file, lower it, and write compile artifacts."""
    out_path = Path(outdir)
    normalise_author_file(author_path, outdir=out_path)
    return make_plan_file(
        out_path / "normalized.yaml",
        outdir=out_path,
        chunk_size=chunk_size,
    )


def load_plan_file(plan_path: str | Path) -> ExecutionPlan:
    """Load an ``ExecutionPlan`` from a compiled ``plan.yaml`` file."""
    doc = read_yaml(str(plan_path)) or {}
    plan = ExecutionPlan(
        context=dict(doc.get("context") or {}),
        registry=dict(doc.get("registry") or {}),
        provenance=dict(doc.get("provenance") or {}),
        execution=dict(doc.get("execution") or {}),
        execution_hooks=list(doc.get("execution_hooks") or []),
        data_flow=dict(doc.get("data_flow") or {}),
    )
    plan.partitions = [
        ExecutionPartition(
            id=str(item["id"]),
            dataset=str(item["dataset"]),
            file=str(item["file"]),
            source=str(item["source"]),
            part=str(item["part"]),
            start=item.get("start"),
            stop=item.get("stop"),
        )
        for item in list(doc.get("partitions") or [])
    ]

    for item in list(doc.get("nodes") or []):
        partitioning = dict(item.get("partitioning") or {})
        node = ExecutionNode(
            id=str(item["id"]),
            graph_node_id=str(item.get("graph_node_id") or item["id"]),
            role=item["role"],
            impl=str(item["impl"]),
            inputs=[
                PlanInputRef(
                    node_id=str(ref["node_id"]),
                    output_name=str(ref["output_name"]),
                    input_name=str(ref["input_name"]),
                )
                for ref in list(item.get("inputs") or [])
            ],
            params=dict(item.get("params") or {}),
            outputs=dict(item.get("outputs") or {}),
            input_scope=item.get("input_scope", "global"),
            output_scope=item.get("output_scope", "global"),
            partitioning=PartitionSpec(
                mode=partitioning.get("mode", "none"),
                chunk_size=partitioning.get("chunk_size"),
            ),
            materialize=item.get("materialize", "never"),
            meta=dict(item.get("meta") or {}),
        )
        plan.add_node(node)

    return plan


def run_plan_file(
    plan_path: str | Path,
    *,
    outdir: str | Path | None = None,
    backend: str | None = None,
    strategy: str | None = None,
    scheduler: str | None = None,
    workers: int | None = None,
) -> BackendResult:
    """Run a compiled plan file and write ``run_summary.yaml``."""
    plan_file = Path(plan_path)
    out_path = Path(outdir) if outdir is not None else plan_file.parent
    out_path.mkdir(parents=True, exist_ok=True)

    plan = load_plan_file(plan_file)
    runtime_execution = _runtime_execution_with_overrides(
        plan.execution,
        backend=backend,
        strategy=strategy,
        scheduler=scheduler,
        workers=workers,
    )
    plan.execution = runtime_execution

    backend_impl = load_backend(plan)
    result = backend_impl.run(plan, ctx={"outdir": str(out_path.resolve())})

    summary = {
        "backend": result.backend,
        "strategy": result.strategy,
        "success": result.success,
        "execution": runtime_execution,
        **result.summary,
    }
    write_yaml(summary, str(out_path / "run_summary.yaml"))
    return result


def run_author_file(
    author_path: str | Path,
    *,
    outdir: str | Path,
    backend: str | None = None,
    strategy: str | None = None,
    scheduler: str | None = None,
    workers: int | None = None,
    chunk_size: int | None = None,
) -> BackendResult:
    """Compile and run an author YAML file in one call."""
    out_path = Path(outdir)
    compile_author_file(author_path, outdir=out_path, chunk_size=chunk_size)
    return run_plan_file(
        out_path / "plan.yaml",
        outdir=out_path,
        backend=backend,
        strategy=strategy,
        scheduler=scheduler,
        workers=workers,
    )


def diff_plan_files(
    old_plan: str | Path,
    new_plan: str | Path,
) -> tuple[str, bool]:
    """Return a formatted structural diff and equality flag for two plan files."""
    report = diff_plans(
        load_plan_yaml(old_plan),
        load_plan_yaml(new_plan),
    )
    return format_plan_diff(report), report.equal


def _packaged_profile_files() -> Iterable[tuple[Path, Traversable]]:
    profiles_root = resources.files("hepflow.profiles")

    def walk(
        node: Traversable,
        relative_to_root: Path,
    ) -> Iterable[tuple[Path, Traversable]]:
        for child in node.iterdir():
            if child.name == "__pycache__":
                continue
            child_relative = relative_to_root / child.name
            if child.is_dir():
                yield from walk(child, child_relative)
                continue
            if child.name == "__init__.py":
                continue
            yield child_relative, child

    yield from walk(profiles_root, Path())


def _runtime_execution_with_overrides(
    execution: dict[str, Any] | None,
    *,
    backend: str | None,
    strategy: str | None,
    scheduler: str | None,
    workers: int | None,
) -> dict[str, Any]:
    runtime_execution = dict(execution or {})
    override = normalize_backend_override(backend, strategy)
    if override:
        runtime_execution.update(override)

    runtime_execution["backend"] = str(runtime_execution.get("backend") or "local")
    runtime_execution["strategy"] = str(runtime_execution.get("strategy") or "default")
    runtime_execution["config"] = dict(runtime_execution.get("config") or {})
    if scheduler is not None:
        runtime_execution["config"]["scheduler"] = scheduler
    if workers is not None:
        runtime_execution["config"]["n_workers"] = workers
    return runtime_execution


def resolve_author_registry(
    author: dict[str, Any],
    *,
    author_path: Path,
) -> RegistryMergeResult:
    project_root = author_path.parent
    use_block = author.get("use") or {}
    if not isinstance(use_block, dict):
        raise ValueError("use must be a mapping")

    profile_names = normalize_profile_names(use_block.get("profiles"))
    builtin_registry = {
        **default_expr_registry_config(),
        **default_runtime_registry_config(),
    }
    layers = [
        RegistryLayer(name="builtin", kind="builtin", registry=builtin_registry),
        *[
            load_profile_registry_layer(name, project_root=project_root)
            for name in profile_names
        ],
        RegistryLayer(
            name="author",
            kind="author",
            registry=dict(author.get("registry") or {}),
            path=str(author_path),
        ),
    ]
    return merge_registry_layers(layers)


def resolve_author_execution(
    author: dict[str, Any],
    *,
    author_path: Path,
) -> dict[str, Any]:
    project_root = author_path.parent
    use_block = author.get("use") or {}
    if not isinstance(use_block, dict):
        raise ValueError("use must be a mapping")

    profile_names = normalize_profile_names(use_block.get("profiles"))
    layers: list[dict[str, Any]] = [
        {
            "name": "builtin",
            "kind": "builtin",
            "execution": {
                "backend": "local",
                "strategy": "default",
                "config": {},
            },
        }
    ]
    for name in profile_names:
        config, provenance = load_profile_config_with_provenance(
            name,
            project_root=project_root,
        )
        layers.append(
            {
                "name": name,
                "kind": "profile",
                "path": provenance["path"],
                "execution": dict(config.get("execution") or {}),
            }
        )
    layers.append(
        {
            "name": "author",
            "kind": "author",
            "path": str(author_path),
            "execution": dict(author.get("execution") or {}),
        }
    )

    merged = _merge_execution_layers(layers)
    return {
        "execution": merged,
        "provenance": {
            "execution_layers": _provenance_layers(layers),
        },
    }


def resolve_author_execution_hooks(
    author: dict[str, Any],
    *,
    author_path: Path,
) -> dict[str, Any]:
    project_root = author_path.parent
    use_block = author.get("use") or {}
    if not isinstance(use_block, dict):
        raise ValueError("use must be a mapping")

    profile_names = normalize_profile_names(use_block.get("profiles"))
    layers: list[dict[str, Any]] = [
        {
            "name": "builtin",
            "kind": "builtin",
            "execution_hooks": [],
        }
    ]
    for name in profile_names:
        config, provenance = load_profile_config_with_provenance(
            name,
            project_root=project_root,
        )
        layers.append(
            {
                "name": name,
                "kind": "profile",
                "path": provenance["path"],
                "execution_hooks": list(config.get("execution_hooks") or []),
            }
        )
    layers.append(
        {
            "name": "author",
            "kind": "author",
            "path": str(author_path),
            "execution_hooks": list(author.get("execution_hooks") or []),
        }
    )

    return {
        "execution_hooks": _merge_execution_hook_layers(layers),
        "provenance": {
            "execution_hook_layers": _provenance_layers(layers),
        },
    }


def write_graph_artifacts(
    graph: nx.DiGraph,
    outdir: str | Path,
    *,
    execution_hooks: list[dict[str, Any]] | None = None,
    with_hooks: bool = False,
) -> dict[str, str]:
    out_path = Path(outdir)
    out_path.mkdir(parents=True, exist_ok=True)

    mermaid_path = out_path / "graph.mmd"
    mermaid_path.write_text(
        _lowered_graph_to_mermaid(
            graph,
            execution_hooks=execution_hooks,
            with_hooks=with_hooks,
        ),
        encoding="utf-8",
    )

    dot_path = out_path / "graph.dot"
    dot_path.write_text(_lowered_graph_to_dot(graph), encoding="utf-8")

    return {
        "graph_mermaid": str(mermaid_path),
        "graph_dot": str(dot_path),
    }


def _merge_execution_layers(layers: list[dict[str, Any]]) -> dict[str, Any]:
    merged: dict[str, Any] = {
        "backend": "local",
        "strategy": "default",
        "config": {},
    }
    for layer in layers:
        execution = dict(layer.get("execution") or {})
        config = dict(execution.pop("config", {}) or {})
        merged.update(
            {key: value for key, value in execution.items() if value is not None}
        )
        merged["config"] = {
            **dict(merged.get("config") or {}),
            **config,
        }
    return {
        "backend": str(merged.get("backend") or "local"),
        "strategy": str(merged.get("strategy") or "default"),
        "config": dict(merged.get("config") or {}),
    }


def _merge_execution_hook_layers(layers: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[tuple[Any, ...], dict[str, Any]] = {}
    order: list[tuple[Any, ...]] = []

    for layer in layers:
        source = _hook_source(layer)
        for raw_hook in list(layer.get("execution_hooks") or []):
            if not isinstance(raw_hook, dict):
                raise ValueError("execution_hooks entries must be mappings")
            hook = dict(raw_hook)
            kind = str(hook.get("kind") or "")
            if not kind:
                raise ValueError("execution_hooks entries must define non-empty 'kind'")
            events = [
                WHEN_ALIASES.get(str(event).strip(), str(event).strip())
                for event in list(hook.get("events") or [])
            ]
            params = dict(hook.get("params") or {})
            hook["kind"] = kind
            hook["events"] = events
            if params:
                hook["params"] = params
            else:
                hook.pop("params", None)
            hook["source"] = source
            match = hook.get("match")
            key = (
                kind,
                tuple(events),
                _freeze_for_key(params),
                _freeze_for_key(match),
            )
            if key not in merged:
                order.append(key)
            merged[key] = hook

    return [merged[key] for key in order]


def _provenance_layers(layers: list[dict[str, Any]]) -> list[dict[str, str]]:
    provenance_layers: list[dict[str, str]] = []
    for layer in layers:
        item = {
            "name": str(layer["name"]),
            "kind": str(layer["kind"]),
        }
        if layer.get("path") is not None:
            item["path"] = str(layer["path"])
        provenance_layers.append(item)
    return provenance_layers


def _hook_source(layer: dict[str, Any]) -> str:
    kind = str(layer.get("kind") or "")
    name = str(layer.get("name") or "")
    if kind == "profile":
        return f"profile:{name}"
    return kind or name


def _freeze_for_key(value: Any) -> Any:
    if isinstance(value, dict):
        return tuple((key, _freeze_for_key(value[key])) for key in sorted(value))
    if isinstance(value, list):
        return tuple(_freeze_for_key(item) for item in value)
    return value


def _lowered_graph_to_mermaid(
    graph: nx.DiGraph,
    *,
    execution_hooks: list[dict[str, Any]] | None = None,
    with_hooks: bool = False,
) -> str:
    lines = ["flowchart TD"]

    for node_id in graph.nodes:
        payload = graph.nodes[node_id]["payload"]
        label = f"{payload.id}<br/>{payload.role}<br/>{payload.impl}"
        lines.append(f'  {_mermaid_id(node_id)}["{_escape_mermaid(label)}"]')

    for upstream, downstream, edge_data in graph.edges(data=True):
        output_name = str(edge_data.get("output") or "stream")
        input_name = str(edge_data.get("input_name") or "stream")
        label = _escape_mermaid(output_name + " -> " + input_name)
        lines.append(
            f"  {_mermaid_id(upstream)} -->|{label}| {_mermaid_id(downstream)}"
        )

    if with_hooks and execution_hooks:
        lines.append("  subgraph Execution Hooks")
        for index, hook in enumerate(execution_hooks):
            kind = str(hook.get("kind") or "hook")
            events = list(hook.get("events") or [])
            event_label = ", ".join(str(event) for event in events) or "all"
            hook_id = f"hook_{index}_{_mermaid_id(kind)}"
            label = f"{event_label}: {kind}"
            lines.append(f'    {hook_id}["{_escape_mermaid(label)}"]')
        lines.append("  end")

    return "\n".join(lines) + "\n"


def _lowered_graph_to_dot(graph: nx.DiGraph) -> str:
    lines = ["digraph hepflow {"]
    for node_id in graph.nodes:
        payload = graph.nodes[node_id]["payload"]
        label = _dot_escape(f"{payload.id}\\n{payload.role}\\n{payload.impl}")
        lines.append(f'  "{node_id}" [label="{label}"];')

    for upstream, downstream, edge_data in graph.edges(data=True):
        output_name = str(edge_data.get("output") or "stream")
        lines.append(
            f'  "{upstream}" -> "{downstream}" [label="{_dot_escape(output_name)}"];'
        )

    lines.append("}")
    return "\n".join(lines) + "\n"


def _mermaid_id(node_id: str) -> str:
    return node_id.replace(".", "_").replace("-", "_")


def _escape_mermaid(value: str) -> str:
    return (
        str(value)
        .replace("\\", "\\\\")
        .replace('"', "&quot;")
        .replace("\n", "<br/>")
    )


def _dot_escape(value: str) -> str:
    return str(value).replace("\\", "\\\\").replace('"', '\\"')
