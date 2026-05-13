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
    },
    "result": {"artifact": "artifact"},
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
