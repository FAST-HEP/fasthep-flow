from __future__ import annotations

import json
from pathlib import Path
from typing import Any

TOY_WRITE_SPEC = {
    "name": "toy.write",
    "kind": "sink",
    "input": {"name": "target", "required": True},
    "params": {
        "path": {"required": True},
        "keep": {"required": False, "default": None},
    },
    "result": {"artifact": "artifact"},
    "requires": {
        "symbols": [
            {
                "from": "params.keep",
                "kind": "field_list",
            }
        ]
    },
}


def run_toy_write(
    *,
    target: Any,
    path: str,
    ctx: dict[str, Any] | None = None,
    **params: Any,
) -> dict[str, str]:
    output_path = Path(path)
    if not output_path.is_absolute():
        output_path = Path((ctx or {}).get("outdir") or ".") / output_path
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(target, sort_keys=True), encoding="utf-8")
    return {"path": str(output_path)}


TOY_CAPTURE_REGISTRY_SPEC = {
    "name": "toy.capture_registry",
    "kind": "sink",
    "input": {"name": "target", "required": True},
    "params": {},
    "result": {"artifact": "artifact"},
}


def run_toy_capture_registry(
    *,
    target: Any,
    ctx: dict[str, Any] | None = None,
) -> dict[str, Any]:
    runtime_registry = (ctx or {}).get("runtime_registry")
    plan = dict((ctx or {}).get("plan") or {})
    return {
        "target": target,
        "plan_has_registry": "registry" in plan,
        "product_handlers": sorted(getattr(runtime_registry, "product_handlers", {})),
    }


TOY_REPORT_SPEC = {
    "name": "toy.report",
    "kind": "sink",
    "params": {
        "source": {"required": True},
        "template": {"required": False},
        "outputs": {"required": True},
    },
    "result": {"artifact": "artifact"},
}


def run_toy_report(
    *,
    report_context: dict[str, Any],
    source: str,
    outputs: list[dict[str, str]],
    template: str | None = None,
    ctx: dict[str, Any] | None = None,
) -> list[dict[str, str]]:
    del template, ctx
    written: list[dict[str, str]] = []
    for output in outputs:
        output_path = Path(output["path"])
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(
                {
                    "source": source,
                    "run_id": report_context["run"]["id"],
                    "artifacts": [item["path"] for item in report_context["artifacts"]],
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        written.append({"path": str(output_path), "format": output["format"]})
    return written
