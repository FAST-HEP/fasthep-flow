from __future__ import annotations

import posixpath
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

from hepflow.build_layout import BuildPaths
from hepflow.model.lifecycle import normalize_lifecycle_event
from hepflow.model.plan import ExecutionNode, ExecutionPlan


@dataclass(frozen=True, slots=True)
class OutputClaim:
    key: tuple[str, str]
    display: str
    node_id: str
    declaration: str


def validate_output_claims(plan: ExecutionPlan) -> None:
    """Reject duplicate compiler-known static artifact outputs."""
    claims: dict[tuple[str, str], OutputClaim] = {}
    for reserved in _reserved_run_artifact_claims():
        claims[reserved.key] = reserved

    for node in plan.nodes:
        for claim in _node_output_claims(node):
            existing = claims.get(claim.key)
            if existing is not None:
                _raise_claim_collision(existing, claim)
            claims[claim.key] = claim


def _node_output_claims(node: ExecutionNode) -> list[OutputClaim]:
    if node.role != "sink":
        return []

    params = dict(node.params or {})
    if "path" in params:
        return _writer_path_claims(node, params)
    if "out" in params or isinstance(params.get("spec"), dict):
        return _render_output_claims(node, params)
    return []


def _writer_path_claims(
    node: ExecutionNode,
    params: dict[str, Any],
) -> list[OutputClaim]:
    raw_path = params.get("path")
    if not isinstance(raw_path, str) or not raw_path.strip():
        return []

    when = _sink_when(node)
    resolved_path = _writer_claim_path(raw_path, when=when, node=node)
    if resolved_path is None:
        return []

    claims = [
        OutputClaim(
            key=_path_key(resolved_path),
            display=_path_display(resolved_path),
            node_id=node.id,
            declaration=f"path: {raw_path}",
        )
    ]
    if when == "partition_end":
        manifest_path = (
            BuildPaths(root=Path()).artifact(
                "files",
                Path(raw_path).stem or "artifact",
            )
            / "manifest.json"
        )
        claims.append(
            OutputClaim(
                key=_path_key(manifest_path),
                display=_path_display(manifest_path),
                node_id=node.id,
                declaration=f"writer manifest from path: {raw_path}",
            )
        )
    return claims


def _render_output_claims(
    node: ExecutionNode,
    params: dict[str, Any],
) -> list[OutputClaim]:
    spec = dict(params.get("spec") or {})
    raw_out = params.get("out") or spec.get("out")
    if raw_out is None:
        raw_out = (
            node.meta.get("stage_id")
            or node.meta.get("node_id")
            or node.id
            or "artifact"
        )
        declaration = "<default out>"
    elif isinstance(raw_out, str) and raw_out.strip():
        declaration = f"out: {raw_out}"
    else:
        return []

    out_path = Path(str(raw_out))
    if not out_path.suffix:
        out_path = out_path.with_suffix(".png")
    if not out_path.is_absolute():
        family = "tables" if spec.get("op") == "hep.render.cutflow_csv" else "plots"
        out_path = BuildPaths(root=Path()).artifact(family, out_path)
    return [
        OutputClaim(
            key=_path_key(out_path),
            display=_path_display(out_path),
            node_id=node.id,
            declaration=declaration,
        )
    ]


def _reserved_run_artifact_claims() -> list[OutputClaim]:
    paths = BuildPaths(root=Path())
    reserved_paths = [
        paths.compile_file("normalized.yaml"),
        paths.compile_file("plan.yaml"),
        paths.compile_file("analysis.ir.yaml"),
        paths.compile_file("deps.yaml"),
        paths.compile_file("report.compile.yaml"),
        paths.compile_file("worker_environment.json"),
        paths.render_dir() / "report.render.json",
        paths.artifact_dir("histograms") / "manifest.json",
        paths.artifact_dir("cutflows") / "manifest.json",
        paths.provenance_manifest(),
        paths.provenance_execution(),
        paths.run_summary(),
    ]
    return [
        OutputClaim(
            key=_path_key(path),
            display=_path_display(path),
            node_id="<reserved run artifact>",
            declaration="reserved run artifact",
        )
        for path in reserved_paths
    ]


def _sink_when(node: ExecutionNode) -> str:
    default = "run_end" if str(node.impl).startswith("hep.render.") else "partition_end"
    return normalize_lifecycle_event(dict(node.params or {}).get("when") or default)


def _writer_claim_path(
    raw_path: str,
    *,
    when: str,
    node: ExecutionNode,
) -> Path | None:
    path_template = raw_path
    for token, value in {
        "node_id": node.id,
    }.items():
        path_template = path_template.replace("{" + token + "}", value)

    if "{" in path_template or "}" in path_template:
        allowed_tokens = {"dataset", "part", "partition_id"}
        unresolved = _format_tokens(path_template)
        if unresolved - allowed_tokens:
            return None
        path = Path(path_template)
    else:
        path = Path(path_template)
        if when == "partition_end":
            path = path.with_suffix("") / "{dataset}" / ("{part}" + path.suffix)

    if not path.is_absolute():
        path = BuildPaths(root=Path()).artifact("files", path)
    return path


def _format_tokens(value: str) -> set[str]:
    tokens: set[str] = set()
    parts = value.split("{")
    for part in parts[1:]:
        token = part.split("}", 1)[0]
        if token:
            tokens.add(token)
    return tokens


def _path_key(path: Path) -> tuple[str, str]:
    if path.is_absolute():
        return ("absolute", path.resolve(strict=False).as_posix())
    normalized = posixpath.normpath(PurePosixPath(path).as_posix())
    return ("relative", normalized)


def _path_display(path: Path) -> str:
    key = _path_key(path)
    return key[1]


def _raise_claim_collision(existing: OutputClaim, claim: OutputClaim) -> None:
    raise ValueError(
        "Conflicting output artifact claim for "
        f"{claim.display!r}: node {existing.node_id!r} declares "
        f"{existing.declaration!r}, and node {claim.node_id!r} declares "
        f"{claim.declaration!r}. Output paths and artifact identities must be unique; "
        "last-write-wins output overwrites are not allowed."
    )
