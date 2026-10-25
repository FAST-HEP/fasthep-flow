"""The ``flow.software`` compile hook: record the software environment of a compile.

The hook writes ``compile/software.json``. It only observes (interpreter, installed
distributions, git state of the project and of editable checkouts, lock file, allowlisted
environment variables) and never changes ``normalized.yaml`` or ``plan.yaml``.
"""

from __future__ import annotations

import os
from importlib import metadata, util
from pathlib import Path
from typing import Any

from fasthep_toolbench.package.environment import (
    distribution_info,
    find_lockfile,
    git_state,
    installed_distributions,
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
_PACKAGE_PATH_PREFIX = "package:"


def software(context: CompileHookContext, **params: Any) -> dict[str, Any]:
    del params
    root = _project_root(context)
    distributions = installed_distributions()
    direct_urls: list[dict[str, Any]] = []
    editable_git: dict[str, dict[str, Any]] = {}
    editable_paths: dict[str, str] = {}
    for name, _version in distributions:
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
            editable_git[info.name] = git_state(info.editable_path).to_dict()
    lockfile = find_lockfile(root) if root is not None else None
    return {
        "software": {
            "python": python_info(),
            "distributions": [
                {"name": name, "version": version} for name, version in distributions
            ],
            "direct_urls": direct_urls,
            "git": {
                "root": (
                    git_state(root).to_dict()
                    if root is not None
                    else {
                        "root": None,
                        "revision": None,
                        "dirty": None,
                        "note": "project root unknown",
                    }
                ),
                "editable": editable_git,
            },
            "layers": _layer_distributions(context.normalized, editable_paths),
            "lockfile": lockfile,
            "environment": {
                key: value
                for key, value in sorted(os.environ.items())
                if key.startswith(ENVIRONMENT_PREFIXES)
            },
        }
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
