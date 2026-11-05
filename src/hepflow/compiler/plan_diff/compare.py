"""Recursive comparison of two plans into a :class:`PlanDiffReport`."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

import yaml

from hepflow.compiler.plan_diff.classify import CLASSIFIER_VERSION, classify
from hepflow.compiler.plan_diff.errors import InputNotFound, InvalidPlan
from hepflow.compiler.plan_diff.identity import (
    IdentitySpec,
    MoveSpec,
    identity_of,
    identity_spec,
)
from hepflow.compiler.plan_diff.model import (
    DiffKind,
    DiffSection,
    Index,
    Key,
    Location,
    PlanDiffEntry,
    PlanDiffReport,
    PlanDiffWarning,
    Selector,
)
from hepflow.compiler.plan_diff.paths import ROOT_FIELDS, display_path, is_path_field
from hepflow.compiler.plan_diff.validate import validate_plan
from hepflow.utils import read_yaml


def diff_plan_files(old_plan: str | Path, new_plan: str | Path) -> PlanDiffReport:
    """Load two plan files and compare them."""
    return diff_plans(_load(old_plan, "old"), _load(new_plan, "new"))


def diff_plans(old: Mapping[str, Any], new: Mapping[str, Any]) -> PlanDiffReport:
    """Compare two loaded plans; neither is modified."""
    validate_plan(old, side="old")
    validate_plan(new, side="new")
    comparer = _Comparer(_project_root(old), _project_root(new))
    comparer.compare(old, new, ())
    return PlanDiffReport(
        comparer.entries,
        classifier_version=CLASSIFIER_VERSION,
        warnings=_version_warnings(old, new),
    )


def _load(path: str | Path, side: str) -> Any:
    location = Path(path)
    if not location.exists():
        raise InputNotFound(location)
    if location.is_dir():
        raise InvalidPlan(f"{side} plan {location} is a directory, not a plan file")
    try:
        return read_yaml(location)
    except (yaml.YAMLError, UnicodeDecodeError) as error:
        raise InvalidPlan(f"{side} plan is not valid YAML: {error}") from error
    except OSError as error:
        raise InvalidPlan(f"{side} plan cannot be read: {error}") from error


def _project_root(plan: Mapping[str, Any]) -> str | None:
    context = plan.get("context")
    root = context.get("project_root") if isinstance(context, dict) else None
    return root if isinstance(root, str) else None


def _key_path(loc: Location) -> tuple[str, ...]:
    return tuple(s.name for s in loc if isinstance(s, Key))


def _canon(item: Any) -> str:
    return json.dumps(item, sort_keys=True, separators=(",", ":"))


def _collapse(items: list[Any]) -> list[Any]:
    seen: set[str] = set()
    kept = []
    for item in items:
        canon = _canon(item)
        if canon not in seen:
            seen.add(canon)
            kept.append(item)
    return kept


_SHA256 = re.compile(r"^[0-9a-f]{64}$")

_IndexMap = dict[tuple[Any, ...], tuple[tuple[tuple[str, Any], ...], list[Any]]]


def _ident_key(identity: tuple[tuple[str, Any], ...]) -> tuple[Any, ...]:
    return tuple((name, type(value).__name__, value) for name, value in identity)


def _index(items: list[Any], spec: IdentitySpec) -> _IndexMap:
    index: _IndexMap = {}
    for item in items:
        identity = identity_of(spec, item)
        index.setdefault(_ident_key(identity), (identity, []))[1].append(item)
    return index


def _drop_identical_duplicates(old_index: _IndexMap, new_index: _IndexMap) -> None:
    """Remove duplicated identities whose rows are identical on both sides.

    Such a group is no difference (a plan equals itself); only duplicate groups that
    differ between the plans are conflicts.
    """
    for key in old_index.keys() & new_index.keys():
        old_rows, new_rows = old_index[key][1], new_index[key][1]
        duplicated = len(old_rows) > 1 or len(new_rows) > 1
        if duplicated and sorted(map(_canon, old_rows)) == sorted(
            map(_canon, new_rows)
        ):
            del old_index[key], new_index[key]


class _Comparer:
    """Compares two plans and collects entries; knows each side's project root."""

    def __init__(self, old_root: str | None, new_root: str | None) -> None:
        self.old_root = old_root
        self.new_root = new_root
        self.entries: list[PlanDiffEntry] = []

    # -- entries ---------------------------------------------------------------

    def _displays(
        self, loc: Location, old: Any, new: Any, *, old_present: bool, new_present: bool
    ) -> tuple[Any, Any]:
        key_path = _key_path(loc)
        if not is_path_field(key_path):
            return None, None
        root = key_path in ROOT_FIELDS

        def show(value: Any, side_root: str | None, present: bool) -> Any:
            if not present or not isinstance(value, str):
                return None
            shown = display_path(value, None if root else side_root)
            return {"value": value, "label": "root"} if root else shown.to_dict()

        return (
            show(old, self.old_root, old_present),
            show(new, self.new_root, new_present),
        )

    def _added(
        self,
        loc: Location,
        value: Any,
        *,
        whole: DiffSection | None = None,
        identity: dict[str, Any] | None = None,
        fallback: bool,
    ) -> None:
        _, new_display = self._displays(
            loc, None, value, old_present=False, new_present=True
        )
        self.entries.append(
            PlanDiffEntry(
                section=classify(loc, whole_object_section=whole),
                kind=DiffKind.ADDED,
                location=loc,
                old_present=False,
                new_present=True,
                new=value,
                identity=identity,
                fallback=fallback,
                new_display=new_display,
            )
        )

    def _removed(
        self,
        loc: Location,
        value: Any,
        *,
        whole: DiffSection | None = None,
        identity: dict[str, Any] | None = None,
        fallback: bool,
    ) -> None:
        old_display, _ = self._displays(
            loc, value, None, old_present=True, new_present=False
        )
        self.entries.append(
            PlanDiffEntry(
                section=classify(loc, whole_object_section=whole),
                kind=DiffKind.REMOVED,
                location=loc,
                old_present=True,
                new_present=False,
                old=value,
                identity=identity,
                fallback=fallback,
                old_display=old_display,
            )
        )

    # -- recursion -------------------------------------------------------------

    def compare(
        self, old: Any, new: Any, loc: Location, *, fallback: bool = False
    ) -> None:
        if isinstance(old, dict) and isinstance(new, dict):
            for key in sorted(set(old) | set(new)):
                child = (*loc, Key(key))
                if key not in new:
                    self._removed(child, old[key], fallback=fallback)
                elif key not in old:
                    self._added(child, new[key], fallback=fallback)
                else:
                    self.compare(old[key], new[key], child, fallback=fallback)
        elif isinstance(old, list) and isinstance(new, list):
            self.compare_list(old, new, loc, fallback=fallback)
        elif type(old) is not type(new) or old != new:
            old_display, new_display = self._displays(
                loc, old, new, old_present=True, new_present=True
            )
            self.entries.append(
                PlanDiffEntry(
                    section=classify(loc),
                    kind=DiffKind.CHANGED,
                    location=loc,
                    old_present=True,
                    new_present=True,
                    old=old,
                    new=new,
                    fallback=fallback,
                    old_display=old_display,
                    new_display=new_display,
                )
            )

    def compare_list(
        self, old: list[Any], new: list[Any], loc: Location, *, fallback: bool
    ) -> None:
        spec = identity_spec(_key_path(loc))
        if spec is None:
            self.compare_anonymous(old, new, loc, fallback=fallback)
        else:
            self.compare_identified(old, new, loc, spec, fallback=fallback)

    def compare_anonymous(
        self, old: list[Any], new: list[Any], loc: Location, *, fallback: bool
    ) -> None:
        matcher = SequenceMatcher(
            None, [_canon(i) for i in old], [_canon(i) for i in new], autojunk=False
        )
        for tag, i1, i2, j1, j2 in matcher.get_opcodes():
            if tag == "equal":
                continue
            paired = min(i2 - i1, j2 - j1) if tag == "replace" else 0
            for k in range(paired):
                self.compare(
                    old[i1 + k], new[j1 + k], (*loc, Index(j1 + k)), fallback=fallback
                )
            for i in range(i1 + paired, i2):
                self._removed((*loc, Index(i)), old[i], fallback=fallback)
            for j in range(j1 + paired, j2):
                self._added((*loc, Index(j)), new[j], fallback=fallback)

    def compare_identified(
        self,
        old: list[Any],
        new: list[Any],
        loc: Location,
        spec: IdentitySpec,
        *,
        fallback: bool,
    ) -> None:
        if spec.collapse_exact_duplicates:
            old, new = _collapse(old), _collapse(new)
        old_index, new_index = _index(old, spec), _index(new, spec)
        _drop_identical_duplicates(old_index, new_index)
        conflicts = sorted(
            (
                key
                for key in old_index.keys() | new_index.keys()
                if len(old_index.get(key, (None, []))[1]) > 1
                or len(new_index.get(key, (None, []))[1]) > 1
            ),
            key=repr,
        )
        if conflicts:
            for key in conflicts:
                identity = (old_index.get(key) or new_index[key])[0]
                self.entries.append(
                    PlanDiffEntry(
                        section=spec.section,
                        kind=DiffKind.IDENTITY_CONFLICT,
                        location=(*loc, Selector(identity)),
                        old_present=key in old_index,
                        new_present=key in new_index,
                        old=old_index[key][1] if key in old_index else None,
                        new=new_index[key][1] if key in new_index else None,
                        identity=dict(identity),
                        fallback=fallback,
                    )
                )
            self.compare_anonymous(old, new, loc, fallback=True)
            return
        removed: list[tuple[tuple[tuple[str, Any], ...], Any]] = []
        added: list[tuple[tuple[tuple[str, Any], ...], Any]] = []
        for key in sorted(old_index.keys() | new_index.keys(), key=repr):
            in_old, in_new = old_index.get(key), new_index.get(key)
            if in_old is None:
                assert in_new is not None
                added.append((in_new[0], in_new[1][0]))
            elif in_new is None:
                removed.append((in_old[0], in_old[1][0]))
            else:
                identity = in_old[0]
                self.compare(
                    in_old[1][0],
                    in_new[1][0],
                    (*loc, Selector(identity)),
                    fallback=fallback,
                )
        if spec.move is not None:
            removed, added = self._pair_moves(spec.move, loc, removed, added, fallback)
        for identity, row in removed:
            self._removed(
                (*loc, Selector(identity)),
                row,
                whole=spec.section,
                identity=dict(identity),
                fallback=fallback,
            )
        for identity, row in added:
            self._added(
                (*loc, Selector(identity)),
                row,
                whole=spec.section,
                identity=dict(identity),
                fallback=fallback,
            )

    def _pair_moves(
        self,
        move: MoveSpec,
        loc: Location,
        removed: list[tuple[tuple[tuple[str, Any], ...], Any]],
        added: list[tuple[tuple[tuple[str, Any], ...], Any]],
        fallback: bool,
    ) -> tuple[
        list[tuple[tuple[tuple[str, Any], ...], Any]],
        list[tuple[tuple[tuple[str, Any], ...], Any]],
    ]:
        """Pair unmatched rows into moves; anything not uniquely and fully evidenced stays."""

        def group(
            rows: list[tuple[tuple[tuple[str, Any], ...], Any]],
        ) -> dict[Any, list[int]]:
            groups: dict[Any, list[int]] = {}
            for position, (_, row) in enumerate(rows):
                digest = row.get(move.hash_field)
                if not isinstance(digest, str) or not _SHA256.match(digest):
                    continue
                key = tuple(
                    (name, type(row.get(name)).__name__, row.get(name))
                    for name in move.pair_key
                )
                groups.setdefault(key, []).append(position)
            return groups

        old_groups, new_groups = group(removed), group(added)
        paired_old: set[int] = set()
        paired_new: set[int] = set()
        for key in sorted(old_groups.keys() & new_groups.keys(), key=repr):
            if len(old_groups[key]) != 1 or len(new_groups[key]) != 1:
                continue
            i, j = old_groups[key][0], new_groups[key][0]
            old_row, new_row = removed[i][1], added[j][1]
            if any(
                (name in old_row) != (name in new_row)
                or type(old_row.get(name)) is not type(new_row.get(name))
                or old_row.get(name) != new_row.get(name)
                for name in move.metadata_fields
            ):
                continue
            paired_old.add(i)
            paired_new.add(j)
            identity = {name: old_row.get(name) for name in move.pair_key}
            self.entries.append(
                PlanDiffEntry(
                    section=DiffSection.PROVENANCE,
                    kind=DiffKind.MOVED,
                    location=(*loc, Selector(tuple(identity.items()))),
                    old_present=True,
                    new_present=True,
                    old=old_row,
                    new=new_row,
                    identity=identity,
                    fallback=fallback,
                    old_display=self._row_display(old_row, self.old_root),
                    new_display=self._row_display(new_row, self.new_root),
                )
            )
        return (
            [r for n, r in enumerate(removed) if n not in paired_old],
            [r for n, r in enumerate(added) if n not in paired_new],
        )

    @staticmethod
    def _row_display(row: dict[str, Any], root: str | None) -> dict[str, Any]:
        return {
            name: display_path(row[name], root).to_dict()
            for name in ("declared", "resolved")
            if isinstance(row.get(name), str)
        }


def _flow_version(plan: Mapping[str, Any]) -> str | None:
    provenance = plan.get("provenance")
    compilation = (
        provenance.get("compilation") if isinstance(provenance, dict) else None
    )
    version = compilation.get("flow_version") if isinstance(compilation, dict) else None
    return version if isinstance(version, str) else None


def _version_warnings(
    old: Mapping[str, Any], new: Mapping[str, Any]
) -> list[PlanDiffWarning]:
    old_version, new_version = _flow_version(old), _flow_version(new)
    if old_version is not None and new_version is not None:
        if old_version == new_version:
            return []
        return [
            PlanDiffWarning(
                code="flow_version_mismatch",
                message=(
                    f"plans were compiled by different Flow versions: "
                    f"{old_version} and {new_version}"
                ),
                details={"old": old_version, "new": new_version},
            )
        ]
    missing = [
        side
        for side, version in (("old", old_version), ("new", new_version))
        if version is None
    ]
    if len(missing) == 2:
        return []
    return [
        PlanDiffWarning(
            code="flow_version_unknown",
            message=f"the Flow version is not recorded in the {' and '.join(missing)} plan",
            details={"missing": missing},
        )
    ]
