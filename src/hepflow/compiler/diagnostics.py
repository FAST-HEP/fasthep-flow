from __future__ import annotations

from hepflow.model.report import CompileReport


def format_diagnostics_text(report: CompileReport) -> str:
    lines: list[str] = []
    lines.append("Compile report")
    lines.append("--------------")
    lines.append(f"work_dir   : {report.work_dir}")
    lines.append(f"results_dir: {report.results_dir}")
    lines.append(f"primary    : {report.primary_stream}")
    lines.append("")

    lines.append("Datasets")
    lines.append("--------")
    for d in report.datasets:
        lines.append(
            f"- {d.name} (eventtype={d.eventtype}, nevents={d.nevents} [{d.nevents_source}])"
        )
        for f in d.files:
            lines.append(f"    - {f}")
    lines.append("")

    lines.append("Required inputs")
    lines.append("---------------")
    for sid, ri in (report.required_inputs or {}).items():
        lines.append(f"- {sid}: {ri.get('kind')} tree={ri.get('tree')}")
        for b in ri.get("branches", [])[:50]:
            lines.append(f"    - {b}")
        if len(ri.get("branches", [])) > 50:
            lines.append(f"    ... ({len(ri['branches']) - 50} more)")
    lines.append("")

    if report.messages:
        lines.append("Messages")
        lines.append("--------")
        for m in report.messages:
            lines.append(f"- [{m.level}] {m.code}: {m.message}")
    lines.append("")
    return "\n".join(lines)
