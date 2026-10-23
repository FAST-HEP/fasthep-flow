"""Write ``compile/inputs.manifest.yaml``: every input path the compiler resolved.

The manifest records the project root and, per declared input path, what the author wrote
(``declared``), what the compiler resolved it to, and the sha256 and size of the file at
compile time. It is the seed of the reproducible compile bundle (fasthep-flow#94).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from hepflow.model.plan import ExecutionPlan
from hepflow.utils import write_yaml

MANIFEST_NAME = "inputs.manifest.yaml"
MANIFEST_VERSION = 1
_ROW_KEYS = ("role", "declared", "resolved", "origin", "sha256", "size", "note")


def write_inputs_manifest(
    compile_dir: str | Path,
    *,
    root: str | None,
    inputs: list[dict[str, Any]],
) -> Path:
    """Write the manifest into ``compile_dir`` and return its path."""
    rows = [{key: item.get(key) for key in _ROW_KEYS} for item in inputs]
    rows.sort(key=lambda row: (str(row["role"]), str(row["declared"])))
    path = Path(compile_dir) / MANIFEST_NAME
    write_yaml(
        {"version": MANIFEST_VERSION, "root": root, "inputs": rows},
        str(path),
    )
    return path


def mapping_file_inputs(plan: ExecutionPlan) -> list[dict[str, Any]]:
    """Return manifest rows for files loaded by ``flow.load_mapping`` parameter hooks."""
    rows: list[dict[str, Any]] = []
    for node in plan.nodes:
        stage = node.id.removeprefix("stage.")
        hook_records = dict(node.meta.get("compile_hooks") or {})
        for param_name, records in sorted(hook_records.items()):
            for record in records if isinstance(records, list) else []:
                if record.get("hook") != "flow.load_mapping":
                    continue
                if "resolved_path" not in record:
                    continue
                rows.append(
                    {
                        "role": f"param:{stage}.{param_name}",
                        "declared": record.get("input"),
                        "resolved": record["resolved_path"],
                        "origin": record.get("origin"),
                        "sha256": record.get("sha256"),
                        "size": record.get("size"),
                        "note": None,
                    }
                )
    return rows
