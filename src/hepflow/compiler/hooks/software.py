"""The ``flow.software`` compile hook: record the software environment of a compile.

The hook writes ``compile/software.json``. It only observes (interpreter, installed
distributions, git state of the project and of editable checkouts, lock file, allowlisted
environment variables) and never changes ``normalized.yaml`` or ``plan.yaml``.
"""

from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor
from importlib import metadata, util
from pathlib import Path
from typing import Any

from fasthep_toolbench.package.environment import (
    distribution_info,
    find_lockfile,
    git_state,
    normalise_name,
    python_info,
)

from hepflow.compiler.hooks.model import CompileHookContext

SOFTWARE_HOOK_SPEC = {
    "name": "flow.software",
    "kind": "compile_hook",
    "version": "1.0",
    "lifecycle": {"when": "after_datasets"},
    "result": {"artifacts": ["software"]},
}

ENVIRONMENT_PREFIXES = ("FASTHEP_", "HEPFLOW_")
GIT_MAX_WORKERS = 4
_PACKAGE_PATH_PREFIX = "package:"


def software(context: CompileHookContext, *, git: bool = True) -> dict[str, Any]:
    """Describe the software environment.

    ``git=False`` skips all git capture (``git`` is then ``None`` with a note).
    """
    root = _project_root(context)
    # One pass over the installed distributions (first one wins, as in toolbench).
    found: dict[str, Any] = {}
    for dist in metadata.distributions():
        name = _dist_name(dist)
        if name is not None and name not in found:
            found[name] = dist
    distributions = [(name, found[name].version) for name in sorted(found)]
    direct_urls: list[dict[str, Any]] = []
    editable_paths: dict[str, str] = {}
    for name in sorted(found):
        if _has_direct_url(found[name]):
            info = distribution_info(name)
            if info is None or info.direct_url is None:
                continue
            direct = info.direct_url
            direct_urls.append(
                {
                    "name": info.name,
                    "url": direct.url,
                    "vcs": direct.vcs,
                    "commit_id": direct.commit_id,
                    "editable": direct.editable,
                }
            )
            if direct.editable and info.editable_path:
                editable_paths[info.name] = info.editable_path
    git_record = _git_record(root, editable_paths) if git else None
    git_note = None if git else "disabled by hook parameter 'git'"
    lockfile = find_lockfile(root) if root is not None else None
    result: dict[str, Any] = {
        "python": python_info(),
        "distributions": [
            {"name": name, "version": version} for name, version in distributions
        ],
        "direct_urls": direct_urls,
        "git": git_record,
        "layers": _layer_distributions(context.normalized, editable_paths),
        "lockfile": lockfile,
        "environment": {
            key: value
            for key, value in sorted(os.environ.items())
            if key.startswith(ENVIRONMENT_PREFIXES)
        },
    }
    if git_note is not None:
        result["git_note"] = git_note
    return {"software": result}


def _dist_name(dist: Any) -> str | None:
    try:
        raw = dist.metadata["Name"]
    except KeyError:
        return None
    return normalise_name(raw) if raw else None


def _has_direct_url(dist: Any) -> bool:
    try:
        return dist.read_text("direct_url.json") is not None
    except (OSError, UnicodeDecodeError):
        return True  # let distribution_info record the unreadable note


def _git_record(root: Path | None, editable_paths: dict[str, str]) -> dict[str, Any]:
    """git state of the root and of each editable path, run concurrently.

    One call per distinct path; output order does not depend on completion order.
    """
    paths: list[str] = sorted(
        {*editable_paths.values(), *([str(root)] if root else [])}
    )
    states: dict[str, dict[str, Any]] = {}
    if paths:
        with ThreadPoolExecutor(max_workers=GIT_MAX_WORKERS) as pool:
            for path, state in zip(
                paths, pool.map(lambda p: git_state(p).to_dict(), paths), strict=True
            ):
                states[path] = state
    root_state = (
        states[str(root)]
        if root is not None
        else {
            "root": None,
            "revision": None,
            "dirty": None,
            "note": "project root unknown",
        }
    )
    return {
        "root": root_state,
        "editable": {
            name: states[editable_paths[name]] for name in sorted(editable_paths)
        },
    }


def _project_root(context: CompileHookContext) -> Path | None:
    raw = context.plan_context.get("project_root")
    if isinstance(raw, (str, Path)) and str(raw).strip():
        return Path(raw)
    return None


def _layer_distributions(
    normalized: dict[str, Any], editable_paths: dict[str, str]
) -> list[str]:
    """Names of the distributions that supplied a packaged registry or profile layer.

    The registry layer provenance marks a packaged layer with the path
    ``package:<package>.profiles/<file>``; the import package maps to its distribution
    (editable installs are not in ``packages_distributions``, so they are matched by the
    checkout that contains the package).
    """
    provenance = dict(normalized.get("provenance") or {})
    packages: set[str] = set()
    for layer in provenance.get("registry_layers") or []:
        path = layer.get("path") if isinstance(layer, dict) else None
        if not isinstance(path, str) or not path.startswith(_PACKAGE_PATH_PREFIX):
            continue
        module = path[len(_PACKAGE_PATH_PREFIX) :].split("/", 1)[0]
        packages.add(module.split(".", 1)[0])
    owners = metadata.packages_distributions()
    names: set[str] = set()
    for package in packages:
        dists = owners.get(package)
        if dists:
            names.update(normalise_name(dist) for dist in dists)
        else:
            names.add(
                _editable_owner(package, editable_paths) or normalise_name(package)
            )
    return sorted(names)


def _editable_owner(package: str, editable_paths: dict[str, str]) -> str | None:
    try:
        spec = util.find_spec(package)
    except (ImportError, ValueError):
        return None
    origin = spec.origin if spec is not None else None
    if not origin:
        return None
    for name, path in editable_paths.items():
        if Path(origin).is_relative_to(path):
            return name
    return None
