from __future__ import annotations

import json
import os
from typing import Any

from hepflow.model.render import RenderStatus
from hepflow.model.report import RenderAttempt, RenderExecutionReport

_RENDER_STATUS_SUFFIX = ".render.json"


def _load_json(path: str) -> dict[str, Any]:
    with open(path) as f:
        return json.load(f) or {}


def build_render_execution_report(
    *,
    plan: dict[str, Any],
    results_dir: str,
) -> RenderExecutionReport:
    attempts: list[RenderAttempt] = []

    # index status files by their implied output png path
    # convention: "<out_png>.render.json"
    status_by_out: dict[str, str] = {}
    for root, _, files in os.walk(results_dir):
        for fn in files:
            if fn.endswith(_RENDER_STATUS_SUFFIX):
                status_path = os.path.join(root, fn)
                png_full_path = status_path[: -len(_RENDER_STATUS_SUFFIX)]
                status_by_out[png_full_path] = status_path

    rendered = skipped = failed = 0

    for r in (plan.get("renders") or []):
        out_png = r.get("output")
        inp = r.get("input") or {}
        attempt = RenderAttempt(
            render_id=str(r.get("id", "")),
            op=str(r.get("op", "")),
            when=str(r.get("when", "")),
            product=str(inp.get("product", "")),
            input=str(inp.get("path", "")),
            output=str(out_png or ""),
            select=r.get("select") or {},
            status=RenderStatus.PLANNED,
        )
        png_full_path = os.path.normpath(os.path.join(
            results_dir, out_png)) if out_png else None
        if png_full_path and png_full_path in status_by_out:
            st = _load_json(status_by_out[png_full_path])
            attempt = RenderAttempt.from_dict(st)
        attempts.append(attempt)

    planned_products = {a.product for a in attempts if a.product}
    missing: list[str] = []
    for p in (plan.get("products") or []):
        if p.get("kind") == "hist":
            pid = str(p.get("id"))
            print("checking product in report:", pid, planned_products)
            if pid and pid not in planned_products:
                missing.append(pid)

    total = len(attempts)
    rendered = sum(1 for a in attempts if a.status == RenderStatus.RENDERED)
    skipped = sum(1 for a in attempts if a.status == RenderStatus.SKIPPED)
    failed = sum(1 for a in attempts if a.status == RenderStatus.FAILED)
    return RenderExecutionReport(
        total=total,
        rendered=rendered,
        skipped=skipped,
        failed=failed,
        attempts=tuple(attempts),
        missing_renders=tuple(missing),
    )
