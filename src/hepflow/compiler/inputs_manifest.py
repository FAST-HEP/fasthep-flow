"""Write ``compile/inputs.manifest.yaml``: the record of one compile bundle.

The manifest (version 2) records the project root, the ``bundle_id``, the ``plan_hash``,
the software record (``software.json``, listed by hash), the invocation, every compile
hook run and, per declared input path, what the author wrote (``declared``), what the
compiler resolved it to, and the sha256 and size of the file at compile time
(fasthep-flow#94).
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import asdict
from pathlib import Path
from typing import Any

from fasthep_toolbench.misc import file_fingerprint

from hepflow.compiler.bundle import bundle_id, plan_hash, yaml_safe
from hepflow.compiler.input_paths import ResolvedInput
from hepflow.model.plan import ExecutionPlan
from hepflow.utils import write_yaml

MANIFEST_NAME = "inputs.manifest.yaml"
MANIFEST_VERSION = 2
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
    software: str | Path | None = None,
    invocation: Mapping[str, Any] | None = None,
    hooks: Iterable[Mapping[str, Any]] | None = None,
    plan: str | Path | None = None,
) -> Path:
    """Write the manifest into ``compile_dir`` and return its path.

    ``software`` is the path of ``software.json`` (listed by path, sha256 and size, or
    ``null`` when absent), ``invocation`` the invocation record, ``hooks`` the compile
    hook records and ``plan`` the path of ``plan.yaml`` (hashed into ``plan_hash``).
    ``bundle_id`` is computed from everything else (see :func:`bundle.bundle_id`).
    """
    rows = InputCollector.from_rows(inputs).rows()
    manifest: dict[str, Any] = {
        "version": MANIFEST_VERSION,
        "root": root,
        "bundle_id": None,
        "plan_hash": plan_hash(Path(plan), root or "") if plan is not None else None,
        "software": _software_entry(software),
        "invocation": yaml_safe(dict(invocation or {})),
        "hooks": [yaml_safe(dict(record)) for record in hooks or []],
        "inputs": rows,
    }
    manifest["bundle_id"] = bundle_id(manifest)
    path = Path(compile_dir) / MANIFEST_NAME
    write_yaml(manifest, str(path))
    return path


def _software_entry(software: str | Path | None) -> dict[str, Any] | None:
    if software is None or not Path(software).is_file():
        return None
    sha256, size = file_fingerprint(Path(software))
    return {"path": Path(software).name, "sha256": sha256, "size": size}


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
