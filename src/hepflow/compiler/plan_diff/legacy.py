from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from hepflow.utils import read_yaml


@dataclass(slots=True)
class PlanDiffEntry:
    path: str
    kind: str
    old: Any = None
    new: Any = None


@dataclass(slots=True)
class PlanDiffReport:
    entries: list[PlanDiffEntry]

    @property
    def equal(self) -> bool:
        return not self.entries

    def summary(self) -> str:
        if self.equal:
            return "Plans are structurally equal."

        counts: dict[str, int] = {}
        for entry in self.entries:
            counts[entry.kind] = counts.get(entry.kind, 0) + 1

        parts = [f"{kind}={counts[kind]}" for kind in sorted(counts)]
        return f"Plan differences: {', '.join(parts)}"


def load_plan_yaml(path: str | Path) -> dict[str, Any]:
    return dict(read_yaml(str(path)) or {})


def diff_plans(old: dict[str, Any], new: dict[str, Any]) -> PlanDiffReport:
    entries: list[PlanDiffEntry] = []
    _diff_values(old, new, path="", entries=entries)
    return PlanDiffReport(entries=entries)


def format_plan_diff(report: PlanDiffReport) -> str:
    if report.equal:
        return report.summary()

    lines = [report.summary()]
    for entry in report.entries:
        line = f"- {entry.kind}: {entry.path}"
        if entry.kind == "changed":
            line += f" old={entry.old!r} new={entry.new!r}"
        elif entry.kind == "missing":
            line += f" old={entry.old!r}"
        elif entry.kind == "additional":
            line += f" new={entry.new!r}"
        lines.append(line)
    return "\n".join(lines)


def _diff_values(old: Any, new: Any, *, path: str, entries: list[PlanDiffEntry]) -> None:
    if isinstance(old, dict) and isinstance(new, dict):
        _diff_dicts(old, new, path=path, entries=entries)
        return

    if isinstance(old, list) and isinstance(new, list):
        _diff_lists(old, new, path=path, entries=entries)
        return

    if old != new:
        entries.append(PlanDiffEntry(path=path or "<root>", kind="changed", old=old, new=new))


def _diff_dicts(
    old: dict[str, Any],
    new: dict[str, Any],
    *,
    path: str,
    entries: list[PlanDiffEntry],
) -> None:
    all_keys = sorted(set(old) | set(new))
    for key in all_keys:
        child_path = f"{path}.{key}" if path else key
        if key not in new:
            entries.append(PlanDiffEntry(path=child_path, kind="missing", old=old[key]))
            continue
        if key not in old:
            entries.append(PlanDiffEntry(path=child_path, kind="additional", new=new[key]))
            continue
        _diff_values(old[key], new[key], path=child_path, entries=entries)


def _diff_lists(
    old: list[Any],
    new: list[Any],
    *,
    path: str,
    entries: list[PlanDiffEntry],
) -> None:
    if path == "partitions":
        _diff_partitions(old, new, path=path, entries=entries)
        return

    shared = min(len(old), len(new))
    for index in range(shared):
        _diff_values(old[index], new[index], path=f"{path}[{index}]", entries=entries)

    for index in range(shared, len(old)):
        entries.append(
            PlanDiffEntry(path=f"{path}[{index}]", kind="missing", old=old[index])
        )

    for index in range(shared, len(new)):
        entries.append(
            PlanDiffEntry(path=f"{path}[{index}]", kind="additional", new=new[index])
        )


def _diff_partitions(
    old: list[Any],
    new: list[Any],
    *,
    path: str,
    entries: list[PlanDiffEntry],
) -> None:
    old_map = {_partition_identity(item): item for item in old}
    new_map = {_partition_identity(item): item for item in new}

    all_keys = sorted(set(old_map) | set(new_map))
    for identity in all_keys:
        item_path = _partition_path(path, identity)
        if identity not in new_map:
            entries.append(PlanDiffEntry(path=item_path, kind="missing", old=old_map[identity]))
            continue
        if identity not in old_map:
            entries.append(PlanDiffEntry(path=item_path, kind="additional", new=new_map[identity]))
            continue
        _diff_values(old_map[identity], new_map[identity], path=item_path, entries=entries)


def _partition_identity(item: Any) -> tuple[Any, Any, Any]:
    if not isinstance(item, dict):
        return (repr(item), None, None)
    return (
        item.get("dataset"),
        item.get("file"),
        item.get("part"),
    )


def _partition_path(path: str, identity: tuple[Any, Any, Any]) -> str:
    dataset, file_path, part = identity
    return f"{path}[dataset={dataset},file={file_path},part={part}]"
