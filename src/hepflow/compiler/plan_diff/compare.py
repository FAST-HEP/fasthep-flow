"""Recursive comparison of two plans into a :class:`PlanDiffReport`."""

from __future__ import annotations

import json
from collections.abc import Mapping
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

import yaml

from hepflow.compiler.plan_diff.classify import CLASSIFIER_VERSION, classify
from hepflow.compiler.plan_diff.errors import InvalidPlan
from hepflow.compiler.plan_diff.identity import IdentitySpec, identity_of, identity_spec
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
from hepflow.compiler.plan_diff.validate import validate_plan
from hepflow.utils import read_yaml


def diff_plan_files(old_plan: str | Path, new_plan: str | Path) -> PlanDiffReport:
    """Load two plan files and compare them; a missing file is not handled here."""
    return diff_plans(_load(old_plan, "old"), _load(new_plan, "new"))


def diff_plans(old: Mapping[str, Any], new: Mapping[str, Any]) -> PlanDiffReport:
    """Compare two loaded plans; neither is modified."""
    validate_plan(old, side="old")
    validate_plan(new, side="new")
    entries: list[PlanDiffEntry] = []
    _compare(old, new, (), entries, fallback=False)
    return PlanDiffReport(
        entries,
        classifier_version=CLASSIFIER_VERSION,
        warnings=_version_warnings(old, new),
    )


def _load(path: str | Path, side: str) -> Any:
    try:
        return read_yaml(path)
    except yaml.YAMLError as error:
        raise InvalidPlan(f"{side} plan is not valid YAML: {error}") from error


def _compare(
    old: Any, new: Any, loc: Location, out: list[PlanDiffEntry], *, fallback: bool
) -> None:
    if isinstance(old, dict) and isinstance(new, dict):
        for key in sorted(set(old) | set(new)):
            child = (*loc, Key(key))
            if key not in new:
                out.append(_removed(child, old[key], fallback=fallback))
            elif key not in old:
                out.append(_added(child, new[key], fallback=fallback))
            else:
                _compare(old[key], new[key], child, out, fallback=fallback)
    elif isinstance(old, list) and isinstance(new, list):
        _compare_list(old, new, loc, out, fallback=fallback)
    elif type(old) is not type(new) or old != new:
        out.append(
            PlanDiffEntry(
                section=classify(loc),
                kind=DiffKind.CHANGED,
                location=loc,
                old_present=True,
                new_present=True,
                old=old,
                new=new,
                fallback=fallback,
            )
        )


def _added(
    loc: Location,
    value: Any,
    *,
    whole: DiffSection | None = None,
    identity: dict[str, Any] | None = None,
    fallback: bool,
) -> PlanDiffEntry:
    return PlanDiffEntry(
        section=classify(loc, whole_object_section=whole),
        kind=DiffKind.ADDED,
        location=loc,
        old_present=False,
        new_present=True,
        new=value,
        identity=identity,
        fallback=fallback,
    )


def _removed(
    loc: Location,
    value: Any,
    *,
    whole: DiffSection | None = None,
    identity: dict[str, Any] | None = None,
    fallback: bool,
) -> PlanDiffEntry:
    return PlanDiffEntry(
        section=classify(loc, whole_object_section=whole),
        kind=DiffKind.REMOVED,
        location=loc,
        old_present=True,
        new_present=False,
        old=value,
        identity=identity,
        fallback=fallback,
    )


def _key_path(loc: Location) -> tuple[str, ...]:
    return tuple(s.name for s in loc if isinstance(s, Key))


def _compare_list(
    old: list[Any],
    new: list[Any],
    loc: Location,
    out: list[PlanDiffEntry],
    *,
    fallback: bool,
) -> None:
    spec = identity_spec(_key_path(loc))
    if spec is None:
        _compare_anonymous(old, new, loc, out, fallback=fallback)
    else:
        _compare_identified(old, new, loc, spec, out, fallback=fallback)


def _canon(item: Any) -> str:
    return json.dumps(item, sort_keys=True, separators=(",", ":"))


def _compare_anonymous(
    old: list[Any],
    new: list[Any],
    loc: Location,
    out: list[PlanDiffEntry],
    *,
    fallback: bool,
) -> None:
    matcher = SequenceMatcher(
        None, [_canon(i) for i in old], [_canon(i) for i in new], autojunk=False
    )
    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == "equal":
            continue
        paired = min(i2 - i1, j2 - j1) if tag == "replace" else 0
        for k in range(paired):
            _compare(
                old[i1 + k], new[j1 + k], (*loc, Index(j1 + k)), out, fallback=fallback
            )
        for i in range(i1 + paired, i2):
            out.append(_removed((*loc, Index(i)), old[i], fallback=fallback))
        for j in range(j1 + paired, j2):
            out.append(_added((*loc, Index(j)), new[j], fallback=fallback))


def _ident_key(identity: tuple[tuple[str, Any], ...]) -> tuple[Any, ...]:
    return tuple((name, type(value).__name__, value) for name, value in identity)


def _collapse(items: list[Any]) -> list[Any]:
    seen: set[str] = set()
    kept = []
    for item in items:
        canon = _canon(item)
        if canon not in seen:
            seen.add(canon)
            kept.append(item)
    return kept


def _index(
    items: list[Any], spec: IdentitySpec
) -> dict[tuple[Any, ...], tuple[tuple[tuple[str, Any], ...], list[Any]]]:
    index: dict[tuple[Any, ...], tuple[tuple[tuple[str, Any], ...], list[Any]]] = {}
    for item in items:
        identity = identity_of(spec, item)
        key = _ident_key(identity)
        index.setdefault(key, (identity, []))[1].append(item)
    return index


def _drop_identical_duplicates(
    old_index: dict[tuple[Any, ...], tuple[tuple[tuple[str, Any], ...], list[Any]]],
    new_index: dict[tuple[Any, ...], tuple[tuple[tuple[str, Any], ...], list[Any]]],
) -> None:
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


def _compare_identified(
    old: list[Any],
    new: list[Any],
    loc: Location,
    spec: IdentitySpec,
    out: list[PlanDiffEntry],
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
            out.append(
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
        _compare_anonymous(old, new, loc, out, fallback=True)
        return
    for key in sorted(old_index.keys() | new_index.keys(), key=repr):
        in_old, in_new = old_index.get(key), new_index.get(key)
        identity = (in_old or in_new)[0]  # type: ignore[index]
        child = (*loc, Selector(identity))
        if in_old is None:
            assert in_new is not None
            out.append(
                _added(
                    child,
                    in_new[1][0],
                    whole=spec.section,
                    identity=dict(identity),
                    fallback=fallback,
                )
            )
        elif in_new is None:
            out.append(
                _removed(
                    child,
                    in_old[1][0],
                    whole=spec.section,
                    identity=dict(identity),
                    fallback=fallback,
                )
            )
        else:
            _compare(in_old[1][0], in_new[1][0], child, out, fallback=fallback)


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
