"""The ``flow.software`` compile hook: record the software environment of a compile.

The hook writes ``compile/software.json``. It only observes (interpreter, installed
distributions, git state of the project and of editable checkouts, lock file, allowlisted
environment variables) and never changes ``normalized.yaml`` or ``plan.yaml``.
"""

from __future__ import annotations

import copy
import os
import sys
import threading
import time
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
# The distribution scan and git states are reused within a process for this long.
SOFTWARE_CACHE_TTL_SECONDS = 30.0
_PACKAGE_PATH_PREFIX = "package:"
_UNMATCHED_LAYER_NOTE = (
    "no installed distribution matched this import package; the import name is recorded"
)

_CacheKey = tuple[str | None, bool, tuple[str, ...]]
_cache: dict[_CacheKey, tuple[float, dict[str, Any]]] = {}
_cache_lock = threading.Lock()


def clear_software_cache() -> None:
    """Forget the memoised distribution scan and git states of this process."""
    with _cache_lock:
        _cache.clear()


def software(context: CompileHookContext, *, git: bool = True) -> dict[str, Any]:
    """Describe the software environment.

    ``git=False`` skips all git capture (``git`` is then ``None`` with a note).
    """
    root = _project_root(context)
    scan = _cached_scan(root, git=git)
    editable_paths: dict[str, str] = scan["editable_paths"]
    layers, layer_notes = _layer_distributions(context.normalized, editable_paths)
    lockfile = find_lockfile(root) if root is not None else None
    result: dict[str, Any] = {
        "python": python_info(),
        "distributions": scan["distributions"],
        "direct_urls": scan["direct_urls"],
        "git": scan["git"],
        "layers": layers,
        "lockfile": lockfile,
        "environment": {
            key: value
            for key, value in sorted(os.environ.items())
            if key.startswith(ENVIRONMENT_PREFIXES)
        },
    }
    if not git:
        result["git_note"] = "disabled by hook parameter 'git'"
    if layer_notes:
        result["layer_notes"] = layer_notes
    return {"software": result}


def _cached_scan(root: Path | None, *, git: bool) -> dict[str, Any]:
    """The distribution scan and git states, memoised per process.

    The key is (project root, git flag, ``sys.path``); an entry lives
    ``SOFTWARE_CACHE_TTL_SECONDS`` (monotonic clock). Callers get a deep copy.
    """
    key: _CacheKey = (str(root) if root is not None else None, git, tuple(sys.path))
    now = time.monotonic()
    with _cache_lock:
        hit = _cache.get(key)
        if hit is not None and now - hit[0] < SOFTWARE_CACHE_TTL_SECONDS:
            return copy.deepcopy(hit[1])
    scan = _scan_environment(root, git=git)
    with _cache_lock:
        _cache[key] = (time.monotonic(), scan)
    return copy.deepcopy(scan)


def _scan_environment(root: Path | None, *, git: bool) -> dict[str, Any]:
    """Installed distributions, direct URLs, editable paths and (optionally) git states."""
    # One pass over the installed distributions (first one wins, as in toolbench).
    found: dict[str, Any] = {}
    for dist in metadata.distributions():
        name = _dist_name(dist)
        if name is not None and name not in found:
            found[name] = dist
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
    return {
        "distributions": [
            {"name": name, "version": found[name].version} for name in sorted(found)
        ],
        "direct_urls": direct_urls,
        "editable_paths": editable_paths,
        "git": _git_record(root, editable_paths) if git else None,
    }


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
) -> tuple[list[str], dict[str, str]]:
    """Names of the distributions that supplied a packaged registry or profile layer.

    The registry layer provenance marks a packaged layer with the path
    ``package:<package>.profiles/<file>``. The import package is matched to a distribution
    by name (``_`` read as ``-`` or ``.``), then to the editable checkout that contains it.
    When nothing matches, the import name is recorded with a note (second value).
    """
    provenance = dict(normalized.get("provenance") or {})
    packages: set[str] = set()
    for layer in provenance.get("registry_layers") or []:
        path = layer.get("path") if isinstance(layer, dict) else None
        if not isinstance(path, str) or not path.startswith(_PACKAGE_PATH_PREFIX):
            continue
        module = path[len(_PACKAGE_PATH_PREFIX) :].split("/", 1)[0]
        packages.add(module.split(".", 1)[0])
    names: set[str] = set()
    notes: dict[str, str] = {}
    for package in sorted(packages):
        name = _distribution_by_name(package) or _editable_owner(
            package, editable_paths
        )
        if name is None:
            name = package
            notes[package] = _UNMATCHED_LAYER_NOTE
        names.add(name)
    return sorted(names), notes


def _distribution_by_name(package: str) -> str | None:
    for candidate in dict.fromkeys(
        (package, package.replace("_", "-"), package.replace("_", "."))
    ):
        try:
            dist = metadata.distribution(candidate)
        except metadata.PackageNotFoundError:
            continue
        return _dist_name(dist) or normalise_name(candidate)
    return None


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
