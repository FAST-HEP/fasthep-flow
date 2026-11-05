"""Resolve a plan file, build root or ``compile/`` directory to one plan path."""

from __future__ import annotations

import posixpath
from pathlib import Path
from typing import Any

import yaml

from hepflow.build_layout import BuildPaths
from hepflow.compiler.plan_diff.errors import (
    AmbiguousPlans,
    InputNotFound,
    InvalidPlan,
    NoPlanFound,
)
from hepflow.utils import read_yaml

_PLAN = "plan.yaml"
_SYSTEMATICS = "systematics.yaml"


def resolve_plan_input(path: str | Path) -> Path:
    """Return the plan file ``path`` stands for.

    An existing file is returned as supplied (its contents are not read). A directory is a
    build root (holding ``compile/``) or a compile directory (holding ``plan.yaml`` or
    ``systematics.yaml``). ``plan.yaml`` is canonical; otherwise ``systematics.yaml`` is read
    and exactly one declared variation plan may be selected. Nothing is searched recursively
    and ``nominal`` is never chosen silently.
    """
    candidate = Path(path)
    if candidate.is_file():
        return candidate
    if not candidate.exists():
        raise InputNotFound(candidate)
    if not candidate.is_dir():
        raise InvalidPlan(f"{candidate} is neither a file nor a directory")

    nested = (candidate / "compile").is_dir()
    direct = (candidate / _PLAN).is_file() or (candidate / _SYSTEMATICS).is_file()
    if nested and direct:
        raise InvalidPlan(
            f"ambiguous layout: {candidate} looks like a build root and a compile directory"
        )
    if nested:
        build_root = candidate
        compile_dir = BuildPaths(root=candidate).compile_dir()
    elif direct:
        build_root = candidate.parent
        compile_dir = candidate
    else:
        raise NoPlanFound(candidate)

    plan, systematics = compile_dir / _PLAN, compile_dir / _SYSTEMATICS
    if plan.is_file() and systematics.is_file():
        raise InvalidPlan(
            f"conflicting canonical artifacts in {compile_dir}: "
            f"{_PLAN} and {_SYSTEMATICS}"
        )
    if plan.is_file():
        return plan
    if systematics.is_file():
        return _select_declared(candidate, build_root, systematics)
    raise NoPlanFound(candidate)


def _select_declared(origin: Path, build_root: Path, manifest: Path) -> Path:
    candidates = _declared(manifest, build_root)
    if not candidates:
        raise NoPlanFound(origin)
    for _, plan in candidates:
        if not plan.is_file():
            raise InputNotFound(plan)
    if len(candidates) > 1:
        raise AmbiguousPlans(origin, candidates)
    return candidates[0][1]


def _declared(manifest: Path, build_root: Path) -> list[tuple[str, Path]]:
    try:
        data: Any = read_yaml(manifest)
    except yaml.YAMLError as error:
        raise InvalidPlan(f"{manifest} is not valid YAML: {error}") from error
    variations = data.get("variations") if isinstance(data, dict) else None
    if variations is None and isinstance(data, dict):
        variations = []
    if not isinstance(variations, list):
        raise InvalidPlan(f"{manifest}: variations must be a list", "variations")
    names: set[str] = set()
    plans: set[str] = set()
    declared: list[tuple[str, Path]] = []
    for index, entry in enumerate(variations):
        where = f"variations[{index}]"
        if not isinstance(entry, dict):
            raise InvalidPlan(f"{manifest}: entry must be a mapping", where)
        name, plan = entry.get("name"), entry.get("plan")
        if not isinstance(name, str) or not name:
            raise InvalidPlan(f"{manifest}: name must be a non-empty string", where)
        if not isinstance(plan, str) or not plan:
            raise InvalidPlan(f"{manifest}: plan must be a non-empty string", where)
        normalised = posixpath.normpath(plan)
        if posixpath.isabs(normalised) or normalised.split("/")[0] == "..":
            raise InvalidPlan(
                f"{manifest}: plan {plan!r} escapes the build root", where
            )
        if name in names or normalised in plans:
            raise InvalidPlan(f"{manifest}: duplicate declaration {name!r}", where)
        names.add(name)
        plans.add(normalised)
        declared.append((name, build_root / normalised))
    return declared
