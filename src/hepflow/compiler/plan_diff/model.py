"""Report model of the plan comparison: sections, kinds, entries, scoped views."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from hepflow.compiler.plan_diff.errors import EmptyScopeError

REPORT_SCHEMA_VERSION = 1


class DiffSection(StrEnum):
    GRAPH = "graph"
    DATA = "data"
    EXECUTION = "execution"
    REPORTS = "reports"
    PROVENANCE = "provenance"
    UNKNOWN = "unknown"

    @classmethod
    def ordered(cls) -> tuple[DiffSection, ...]:
        return tuple(cls)


class DiffKind(StrEnum):
    ADDED = "added"
    REMOVED = "removed"
    CHANGED = "changed"
    MOVED = "moved"
    IDENTITY_CONFLICT = "identity_conflict"


_SECTION_ORDER = {section: i for i, section in enumerate(DiffSection)}
_KIND_ORDER = {kind: i for i, kind in enumerate(DiffKind)}


@dataclass(frozen=True)
class Key:
    name: str


@dataclass(frozen=True)
class Index:
    position: int


@dataclass(frozen=True)
class Selector:
    identity: tuple[tuple[str, Any], ...]


Location = tuple[Key | Index | Selector, ...]


def render_location(location: Location) -> str:
    if not location:
        return "<root>"
    parts: list[str] = []
    for segment in location:
        if isinstance(segment, Key):
            parts.append(f".{segment.name}" if parts else segment.name)
        elif isinstance(segment, Index):
            parts.append(f"[{segment.position}]")
        else:
            inner = ",".join(f"{k}={v!r}" for k, v in segment.identity)
            parts.append(f"[{inner}]")
    return "".join(parts)


def _location_json(location: Location) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for segment in location:
        if isinstance(segment, Key):
            out.append({"key": segment.name})
        elif isinstance(segment, Index):
            out.append({"index": segment.position})
        else:
            out.append({"selector": dict(segment.identity)})
    return out


def _location_sort_key(location: Location) -> tuple[Any, ...]:
    key: list[tuple[int, Any]] = []
    for segment in location:
        if isinstance(segment, Key):
            key.append((0, segment.name))
        elif isinstance(segment, Index):
            key.append((1, segment.position))
        else:
            key.append((2, repr(sorted(segment.identity, key=lambda kv: kv[0]))))
    return tuple(key)


@dataclass(frozen=True)
class PlanDiffEntry:
    section: DiffSection
    kind: DiffKind
    location: Location
    old_present: bool
    new_present: bool
    old: Any = None
    new: Any = None
    identity: dict[str, Any] | None = None
    fallback: bool = False

    @property
    def rendered_location(self) -> str:
        return render_location(self.location)

    def sort_key(self) -> tuple[Any, ...]:
        return (
            _SECTION_ORDER[self.section],
            _location_sort_key(self.location),
            _KIND_ORDER[self.kind],
        )

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "section": self.section.value,
            "kind": self.kind.value,
            "location": _location_json(self.location),
            "rendered_location": self.rendered_location,
            "identity": self.identity,
            "old_present": self.old_present,
            "new_present": self.new_present,
            "fallback": self.fallback,
        }
        if self.old_present:
            out["old"] = self.old
        if self.new_present:
            out["new"] = self.new
        return out


@dataclass(frozen=True)
class PlanDiffWarning:
    code: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {"code": self.code, "message": self.message, "details": self.details}


@dataclass(frozen=True)
class PlanDiffScope:
    include: tuple[DiffSection, ...]
    ignore: tuple[DiffSection, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "include": [s.value for s in self.include],
            "ignore": [s.value for s in self.ignore],
        }


class PlanDiffReport:
    """Complete result of comparing two plans, or a scoped view of one."""

    def __init__(
        self,
        entries: Iterable[PlanDiffEntry],
        *,
        classifier_version: int,
        warnings: Sequence[PlanDiffWarning] = (),
        scope: PlanDiffScope | None = None,
    ) -> None:
        self._entries = tuple(sorted(entries, key=PlanDiffEntry.sort_key))
        self._warnings = tuple(warnings)
        self._classifier_version = classifier_version
        self._scope = scope

    @property
    def entries(self) -> tuple[PlanDiffEntry, ...]:
        return self._entries

    @property
    def warnings(self) -> tuple[PlanDiffWarning, ...]:
        return self._warnings

    @property
    def scope(self) -> PlanDiffScope | None:
        return self._scope

    @property
    def classifier_version(self) -> int:
        return self._classifier_version

    @property
    def equal(self) -> bool:
        return not self._entries

    @property
    def semantically_equal(self) -> bool:
        return all(e.section is DiffSection.PROVENANCE for e in self._entries)

    def counts(self) -> dict[str, dict[str, int]]:
        sections = {s.value: 0 for s in DiffSection}
        kinds = {k.value: 0 for k in DiffKind}
        for entry in self._entries:
            sections[entry.section.value] += 1
            kinds[entry.kind.value] += 1
        return {"sections": sections, "kinds": kinds}

    def scoped(
        self,
        include: Iterable[DiffSection] | None = None,
        ignore: Iterable[DiffSection] = (),
    ) -> PlanDiffReport:
        included = (
            set(DiffSection) if include is None else {DiffSection(s) for s in include}
        )
        ignored = {DiffSection(s) for s in ignore}
        selected = included - ignored
        if not selected:
            raise EmptyScopeError("the selected sections are empty")
        return PlanDiffReport(
            [e for e in self._entries if e.section in selected],
            classifier_version=self._classifier_version,
            warnings=self._warnings,
            scope=PlanDiffScope(
                include=tuple(s for s in DiffSection if s in included),
                ignore=tuple(s for s in DiffSection if s in ignored),
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "report_schema_version": REPORT_SCHEMA_VERSION,
            "classifier_version": self._classifier_version,
            "equal": self.equal,
            "semantically_equal": self.semantically_equal,
            "counts": self.counts(),
            "entries": [e.to_dict() for e in self._entries],
            "warnings": [w.to_dict() for w in self._warnings],
            "scope": None if self._scope is None else self._scope.to_dict(),
        }
