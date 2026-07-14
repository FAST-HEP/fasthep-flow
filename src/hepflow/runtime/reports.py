from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from hepflow.build_layout import BuildPaths
from hepflow.model.io import OutputResult
from hepflow.model.plan import ExecutionPlan
from hepflow.registry.loaders import load_runtime_spec_and_impl


def run_workflow_reports(
    plan: ExecutionPlan,
    *,
    outdir: str | Path,
    summary: dict[str, Any],
) -> list[dict[str, Any]]:
    """Render configured workflow reports after run products are finalised."""
    reports = list(plan.reports or [])
    if not reports:
        return []

    paths = BuildPaths.from_plan(plan, outdir=outdir)
    results: list[dict[str, Any]] = []
    for report in reports:
        report_id = str(report["id"])
        source_name = str(report["source"])
        context = resolve_report_source(source_name, plan=plan, paths=paths, summary=summary)
        outputs = _resolve_report_outputs(report, paths)
        _spec, impl = load_runtime_spec_and_impl(plan.registry, "sinks", str(report["op"]))
        rendered = impl(
            report_context=context,
            source=source_name,
            template=report.get("template"),
            outputs=outputs,
            ctx={
                "outdir": str(paths.root),
                "build_paths": paths,
                "registry": plan.registry,
                "report_templates": dict(plan.registry.get("report_templates") or {}),
                "author_dir": _author_dir(plan),
            },
        )
        results.extend(_rendered_report_records(report_id, rendered))
    return results


def resolve_report_source(
    source: str,
    *,
    plan: ExecutionPlan,
    paths: BuildPaths,
    summary: dict[str, Any],
) -> dict[str, Any]:
    if source == "provenance":
        return provenance_report_context(plan=plan, paths=paths, summary=summary)
    raise ValueError(f"Unknown report source: {source!r}")


def provenance_report_context(
    *,
    plan: ExecutionPlan,
    paths: BuildPaths,
    summary: dict[str, Any],
) -> dict[str, Any]:
    manifest_path = paths.provenance_manifest()
    execution_path = paths.provenance_execution()
    if not manifest_path.exists():
        raise FileNotFoundError(f"Provenance manifest not found: {manifest_path}")
    if not execution_path.exists():
        raise FileNotFoundError(f"Provenance execution index not found: {execution_path}")

    manifest = _read_json(manifest_path)
    execution = _read_json(execution_path)
    artifacts = _artifact_context(manifest)
    resources = _resource_context(execution)
    executions = _execution_context(execution)
    operations = [
        operation
        for execution_record in executions
        for operation in execution_record["operations"]
    ]
    return {
        "source": "provenance",
        "run": {
            "id": execution.get("run_id") or manifest.get("run_id"),
            "workflow": dict(execution.get("workflow") or {}),
            "summary": dict(summary),
            "datasets": list((plan.context.get("datasets") or {}).keys()),
        },
        "resources": resources,
        "executions": executions,
        "operations": operations,
        "artifacts": artifacts,
        "environment": dict(execution.get("execution") or {}),
        "software_versions": dict(execution.get("software") or {}),
        "warnings": _warning_context(resources),
    }


def _artifact_context(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    artifacts: list[dict[str, Any]] = []
    for record in list(manifest.get("records") or []):
        if not isinstance(record, dict):
            continue
        artifacts.append(
            {
                "path": str(record.get("artifact") or ""),
                "kind": str(record.get("kind") or "artifact"),
                "node_id": str(record.get("node_id") or ""),
                "record": str(record.get("record") or ""),
            }
        )
    return artifacts


def _resource_context(execution: dict[str, Any]) -> list[dict[str, Any]]:
    resources: list[dict[str, Any]] = []
    for resource_id, record in sorted(dict(execution.get("resources") or {}).items()):
        if not isinstance(record, dict):
            continue
        selected = dict(record.get("selected") or {})
        resources.append(
            {
                "id": str(resource_id),
                "kind": str(record.get("kind") or ""),
                "requested_era": record.get("requested_era"),
                "selected_era": selected.get("era"),
                "path": selected.get("path"),
                "correction": selected.get("correction"),
                "fallback": bool(selected.get("fallback", False)),
                "reason": selected.get("reason"),
            }
        )
    return resources


def _execution_context(execution: dict[str, Any]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for execution_id, record in dict(execution.get("executions") or {}).items():
        if not isinstance(record, dict):
            continue
        operations = []
        for operation in list(record.get("operations") or []):
            if not isinstance(operation, dict):
                continue
            inputs = dict(operation.get("inputs") or {})
            outputs = dict(operation.get("outputs") or {})
            operations.append(
                {
                    "execution_id": str(execution_id),
                    "node_id": str(record.get("node_id") or ""),
                    "impl": str(record.get("impl") or ""),
                    "role": str(record.get("role") or ""),
                    "dataset": record.get("dataset"),
                    "partition": record.get("partition"),
                    "inputs": {
                        "symbols": list(inputs.get("symbols") or []),
                        "resources": list(inputs.get("resources") or []),
                    },
                    "outputs": {"symbols": list(outputs.get("symbols") or [])},
                }
            )
        records.append(
            {
                "id": str(execution_id),
                "node_id": str(record.get("node_id") or ""),
                "impl": str(record.get("impl") or ""),
                "role": str(record.get("role") or ""),
                "dataset": record.get("dataset"),
                "partition": record.get("partition"),
                "operations": operations,
            }
        )
    return records


def _warning_context(resources: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "level": "warning",
            "code": "RESOURCE_FALLBACK",
            "message": (
                f"Resource {resource['id']} used fallback era "
                f"{resource.get('selected_era')}"
            ),
            "resource": resource,
        }
        for resource in resources
        if resource.get("fallback")
    ]


def _resolve_report_outputs(
    report: dict[str, Any],
    paths: BuildPaths,
) -> list[dict[str, str]]:
    outputs: list[dict[str, str]] = []
    for output in list(report.get("outputs") or []):
        path = Path(str(output["path"]))
        if not path.is_absolute():
            path = paths.root / path
        outputs.append({"path": str(path), "format": str(output["format"])})
    return outputs


def _rendered_report_records(
    report_id: str,
    rendered: Any,
) -> list[dict[str, Any]]:
    if isinstance(rendered, OutputResult):
        return [_output_record(report_id, rendered)]
    if isinstance(rendered, list):
        records: list[dict[str, Any]] = []
        for item in rendered:
            if isinstance(item, OutputResult):
                records.append(_output_record(report_id, item))
            elif isinstance(item, dict):
                records.append({"report_id": report_id, **dict(item)})
        return records
    if isinstance(rendered, dict):
        return [{"report_id": report_id, **dict(rendered)}]
    return [{"report_id": report_id, "value": rendered}]


def _output_record(report_id: str, output: OutputResult) -> dict[str, Any]:
    return {
        "report_id": report_id,
        "path": str(output.path),
        "format": output.format,
        "kind": output.kind,
    }


def _author_dir(plan: ExecutionPlan) -> str | None:
    author_path = plan.context.get("author_path")
    if isinstance(author_path, str) and author_path.strip():
        return str(Path(author_path).parent)
    return None


def _read_json(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Expected JSON object in {path}")
    return data


__all__ = [
    "provenance_report_context",
    "resolve_report_source",
    "run_workflow_reports",
]
