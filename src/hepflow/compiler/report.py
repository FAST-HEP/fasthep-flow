from __future__ import annotations

from typing import Any, Dict, List, Optional

from hepflow.model.report import (
    CompileReport,
    DatasetReport,
    RenderReport,
    ReportMessage,
    StreamReport,
)


def _maybe_hepflow_version() -> Optional[str]:
    try:
        import hepflow
        return getattr(hepflow, "__version__", None)
    except Exception:
        return None


def build_compile_report(
    *,
    author_path: Optional[str],
    work_dir: str,
    results_dir: str,
    norm: Dict[str, Any],
    ir: Dict[str, Any],
    deps: Any,
    plan: Dict[str, Any],
    inspection: dict[str, Any] | None = None,
    norm_before_inspection: dict[str, Any] | None = None,
) -> CompileReport:
    # streams
    streams: List[StreamReport] = []
    for sid, s in (ir.get("streams") or {}).items():
        kind = str(s.get("kind", ""))
        if kind == "root_tree":
            streams.append(
                StreamReport(
                    stream_id=sid,
                    kind="root_tree",
                    tree=str(s.get("tree", "")),
                )
            )
        elif kind == "zip_join":
            streams.append(
                StreamReport(
                    stream_id=sid,
                    kind="zip_join",
                    inputs=tuple(dict(x) for x in (s.get("inputs") or [])),
                    on_mismatch=str(s.get("on_mismatch", "")) or None,
                )
            )
        else:
            streams.append(StreamReport(stream_id=sid, kind=kind or "unknown"))

    # datasets + nevents source
    norm0 = norm_before_inspection or norm
    datasets: List[DatasetReport] = []
    norm0_datasets = norm0.get("data", {}).get("datasets") or []
    norm_datasets = norm.get("data", {}).get("datasets") or []
    norm0_by_name = {str(d.get("name")): d for d in norm0_datasets}

    for ds in norm_datasets:
        name = str(ds.get("name"))
        eventtype = str(ds.get("eventtype", "mc"))
        files = tuple(str(f) for f in (ds.get("files") or []))

        nevents = None
        if ds.get("nevents") is not None:
            nevents = int(ds["nevents"])
        ds0 = norm0_by_name.get(name)
        nevents0_raw = None if ds0 is None else ds0.get("nevents", None)

        if nevents0_raw is not None:
            nevents_source = "author"
        else:
            nevents_source = "inferred" if inspection else "missing"

        total_entries = None
        if inspection:
            try:
                total_entries = int(
                    inspection["datasets"][name]["total_entries"])
            except Exception:
                pass

        datasets.append(
            DatasetReport(
                name=name,
                eventtype=eventtype,
                files=files,
                nevents=nevents,
                nevents_source=nevents_source,
                total_entries=total_entries,
            )
        )

    required_inputs: Dict[str, Dict[str, Any]] = {}
    for sid, ri in getattr(deps, "required_inputs", {}).items():
        required_inputs[sid] = {
            "kind": ri.kind,
            "tree": ri.tree,
            "branches": list(ri.branches),
        }

    products = tuple(dict(p) for p in (plan.get("products") or []))

    renders: List[RenderReport] = []
    for r in (plan.get("renders") or []):
        inp = (r.get("input") or {})
        renders.append(
            RenderReport(
                render_id=str(r.get("id")),
                kind=str(r.get("kind")),
                when=str(r.get("when", "")),
                product=str(inp.get("product", "")),
                output=str(r.get("output", "")),
                style=r.get("style"),
                spec_path=r.get("spec_path"),
                status="compiled" if r.get("spec") else "planned",
            )
        )

    msgs: List[ReportMessage] = []
    if getattr(deps, "unresolved_external_symbols", ()):
        msgs.append(
            ReportMessage(
                level="warn",
                code="UNRESOLVED_SYMBOLS",
                message="Some symbols could not be resolved to inputs or provided columns.",
                meta={"symbols": list(deps.unresolved_external_symbols)},
            )
        )

    return CompileReport(
        hepflow_version=_maybe_hepflow_version(),
        author_path=author_path,
        work_dir=work_dir,
        results_dir=results_dir,
        primary_stream=str(ir.get("primary_stream", "")),
        streams=tuple(streams),
        datasets=tuple(datasets),
        external_symbols=tuple(getattr(deps, "external_symbols", ())),
        unresolved_external_symbols=tuple(
            getattr(deps, "unresolved_external_symbols", ())),
        required_inputs=required_inputs,
        products=products,
        renders=tuple(renders),
        messages=tuple(msgs),
    )
