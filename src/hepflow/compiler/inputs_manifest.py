"""Write ``compile/inputs.manifest.yaml``: every input path the compiler resolved.

The manifest records the project root and, per declared input path, what the author wrote
(``declared``), what the compiler resolved it to, and the sha256 and size of the file at
compile time. It is the seed of the reproducible compile bundle (fasthep-flow#94).
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import asdict
from pathlib import Path
from typing import Any

from hepflow.compiler.input_paths import ResolvedInput
from hepflow.model.plan import ExecutionPlan
from hepflow.utils import write_yaml

MANIFEST_NAME = "inputs.manifest.yaml"
MANIFEST_VERSION = 1
_ROW_KEYS = ("role", "declared", "resolved", "origin", "sha256", "size", "note")


class InputCollector:
    """Collects the input paths a compile resolved, from every place that resolves them.

    ``entries()`` returns the rows in the order they were recorded (what ``provenance.inputs``
    holds); ``rows()`` returns the canonical manifest rows: manifest keys only, one row per
    ``(role, resolved)`` (the first, authored, declaration wins: a parameter that is both
    ``kind: path`` and loaded by ``flow.load_mapping`` is seen by both resolvers), sorted by
    role and declared text.
    """

    def __init__(self) -> None:
        self._entries: list[dict[str, Any]] = []

    @classmethod
    def from_rows(cls, rows: Iterable[Mapping[str, Any]]) -> InputCollector:
        collector = cls()
        collector.extend(rows)
        return collector

    def add(self, entry: ResolvedInput | Mapping[str, Any]) -> None:
        data = asdict(entry) if isinstance(entry, ResolvedInput) else entry
        self._entries.append({key: data.get(key) for key in _ROW_KEYS})

    def extend(self, entries: Iterable[ResolvedInput | Mapping[str, Any]]) -> None:
        for entry in entries:
            self.add(entry)

    def entries(self) -> list[dict[str, Any]]:
        """The recorded rows in insertion order (copies)."""
        return [dict(entry) for entry in self._entries]

    def rows(self) -> list[dict[str, Any]]:
        """The canonical manifest rows (de-duplicated, sorted)."""
        rows: list[dict[str, Any]] = []
        seen: set[tuple[Any, Any]] = set()
        for entry in self._entries:
            identity = (entry["role"], entry["resolved"])
            if identity in seen:
                continue
            seen.add(identity)
            rows.append(dict(entry))
        rows.sort(key=lambda row: (str(row["role"]), str(row["declared"])))
        return rows


def write_inputs_manifest(
    compile_dir: str | Path,
    *,
    root: str | None,
    inputs: list[dict[str, Any]],
) -> Path:
    """Write the manifest into ``compile_dir`` and return its path."""
    rows = InputCollector.from_rows(inputs).rows()
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
