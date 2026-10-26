"""Records of the reproducible compile bundle: invocation, workflow sources, plan hash.

``compile/invocation.yaml`` holds facts that differ on every compile (timestamp, hostname,
output directory). It is a file of its own so that none of them reaches ``normalized.yaml``
or ``plan.yaml``, which keeps ``plan_hash`` stable (fasthep-flow#94).
"""

from __future__ import annotations

import hashlib
import os
import re
import socket
import warnings
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

from hepflow.compiler.input_paths import resolve_input_path
from hepflow.utils import read_yaml

if TYPE_CHECKING:
    from hepflow.compiler.inputs_manifest import InputCollector

INVOCATION_NAME = "invocation.yaml"
SOURCE_ROLES = frozenset({"workflow", "include", "profile"})
PLAN_HASH_CANONICALIZATION = "root-prefix"
_ROOT_PLACEHOLDER = "<root>"
# Manifest keys that differ between compiles of the same bundle; bundle_id ignores them.
BUNDLE_ID_EXCLUDED_KEYS = frozenset(
    {"timestamp", "hostname", "invocation", "bundle_id"}
)
# Input row keys that bundle_id ignores.
ROW_EXCLUDED_KEYS = frozenset({"copied"})


def write_invocation(
    compile_dir: Path,
    *,
    workflow_path: str,
    outdir: str,
    root: str,
    invocation: Mapping[str, Any] | None,
) -> Path:
    """Write ``compile/invocation.yaml``; ``invocation`` is the caller's own record."""
    path = Path(compile_dir) / INVOCATION_NAME
    path.parent.mkdir(parents=True, exist_ok=True)
    record = {
        "workflow_path": workflow_path,
        "outdir": outdir,
        "root": root,
        "timestamp": datetime.now(UTC).isoformat(timespec="seconds"),
        "hostname": socket.gethostname(),
        "caller": yaml_safe(dict(invocation or {})),
    }
    # Serialise first, so a failure cannot leave a truncated file behind.
    text = yaml.safe_dump(record, sort_keys=False)
    path.write_text(text, encoding="utf-8")
    return path


def yaml_safe(value: Any) -> Any:
    """Return a copy of ``value`` built from YAML-safe types only.

    Paths (``os.PathLike``) become strings, sets become sorted lists, tuples lists; any
    other unknown object becomes its ``repr``. Mapping keys that are not strings become
    their ``repr``.
    """
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    if isinstance(value, os.PathLike):
        return os.fspath(value)
    if isinstance(value, Mapping):
        return {
            key if isinstance(key, str) else repr(key): yaml_safe(item)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [yaml_safe(item) for item in value]
    if isinstance(value, (set, frozenset)):
        items = [yaml_safe(item) for item in value]
        try:
            return sorted(items)
        except TypeError:
            return sorted(items, key=repr)
    return repr(value)


def read_invocation(compile_dir: Path) -> dict[str, Any]:
    """Read ``compile/invocation.yaml``; empty for compile directories without one."""
    path = Path(compile_dir) / INVOCATION_NAME
    if not path.is_file():
        return {}
    data = read_yaml(str(path))
    return dict(data) if isinstance(data, dict) else {}


def plan_hash(plan_path: Path, root: str) -> dict[str, str]:
    """Hash ``plan.yaml`` with every occurrence of ``root`` replaced by ``<root>``.

    The hash is therefore equal for the same project compiled under different roots. Both the
    lexical root and its symlink-resolved form are replaced, at path boundaries only. The
    plan is loaded and dumped again without line wrapping before the replacement.
    """
    # Dump again without line wrapping: write_yaml folds long plain scalars at spaces,
    # which would split a root that contains a space across two lines.
    text = yaml.safe_dump(
        yaml.safe_load(Path(plan_path).read_text(encoding="utf-8")),
        sort_keys=False,
        width=float("inf"),
    )
    text = replace_root(text, root, _ROOT_PLACEHOLDER)
    return {
        "sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
        "canonicalization": PLAN_HASH_CANONICALIZATION,
    }


def replace_root(text: str, root: str, replacement: str) -> str:
    """Replace every whole-path occurrence of ``root`` in ``text`` by ``replacement``.

    Both the lexical root and its symlink-resolved form are replaced, at path boundaries
    only: ``/a/b`` does not rewrite ``/a/bc/x``.
    """
    forms = {root.rstrip("/") if len(root) > 1 else root}
    if root:
        forms.add(str(Path(root).resolve()).rstrip("/"))
    for form in sorted(filter(None, forms), key=len, reverse=True):
        text = re.sub(
            rf"(?<![\w.\-/]){re.escape(form)}(?![\w.\-])",
            lambda _match: replacement,
            text,
        )
    return text


def bundle_id(manifest: Mapping[str, Any]) -> str:
    """Return the sha256 identifying the compile bundle that ``manifest`` describes.

    The hash is taken over the YAML dump (keys sorted) of the manifest without
    ``timestamp``, ``hostname``, ``invocation`` and ``bundle_id``. Hook records count
    with their name, phase, parameters and artifact names only: an artifact's sha256 may
    depend on the output directory (for example a rendered graph path), and what reaches
    the plan is already in ``plan_hash``. Input rows count without their ``copied`` flag
    (taking a snapshot does not change the bundle). It therefore covers ``plan_hash``, the software
    record, the hooks that ran and every input row, but not when or with which output
    directory the compile ran.
    """
    canonical = {
        key: value
        for key, value in manifest.items()
        if key not in BUNDLE_ID_EXCLUDED_KEYS
    }
    if isinstance(canonical.get("hooks"), list):
        canonical["hooks"] = [_hook_identity(record) for record in canonical["hooks"]]
    if isinstance(canonical.get("inputs"), list):
        # Whether a file was copied into the snapshot is storage, not identity.
        canonical["inputs"] = [
            {key: value for key, value in row.items() if key not in ROW_EXCLUDED_KEYS}
            if isinstance(row, Mapping)
            else row
            for row in canonical["inputs"]
        ]
    text = yaml.safe_dump(canonical, sort_keys=True, allow_unicode=True)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _hook_identity(record: Any) -> Any:
    if not isinstance(record, Mapping):
        return record
    return {
        "name": record.get("name"),
        "phase": record.get("phase"),
        "params": record.get("params"),
        "artifacts": sorted(record.get("artifacts") or []),
    }


def record_source_files(
    collector: InputCollector,
    *,
    workflow_path: Path,
    include_files: Iterable[str],
    registry_layers: Iterable[Mapping[str, Any]],
    root: Path,
    entry_dir: Path,
) -> None:
    """Add the workflow, include and local profile files to ``collector``.

    Packaged profiles (``package:...`` paths) are not rows; the software record lists them.
    Include files are real paths; a file under the (possibly symlinked) root is mapped back
    under the lexical ``root`` so ``plan_hash`` can replace it. Files outside the root stay
    absolute real paths, which is expected.
    """
    real_root = root.resolve()

    def lexical(path: Path) -> str:
        real = path.resolve()
        try:
            return str(root / real.relative_to(real_root))
        except ValueError:
            return str(real)

    entry = lexical(workflow_path)
    collector.add(
        resolve_input_path(entry, role="workflow", root=root, entry_dir=entry_dir)
    )
    for include in include_files:
        include_path = lexical(Path(include))
        if include_path == entry:
            continue
        collector.add(
            resolve_input_path(
                include_path, role="include", root=root, entry_dir=entry_dir
            )
        )
    seen: set[str] = set()
    for layer in registry_layers:
        path = layer.get("path")
        if layer.get("kind") != "profile" or not isinstance(path, str):
            continue
        if path.startswith("package:") or path in seen:
            continue
        seen.add(path)
        # The profile resolver already warned about an entry-directory fallback.
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", FutureWarning)
            row = resolve_input_path(
                path, role="profile", root=root, entry_dir=entry_dir
            )
        collector.add(row)
