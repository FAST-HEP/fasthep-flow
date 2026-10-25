"""Records of the reproducible compile bundle: invocation, workflow sources, plan hash.

``compile/invocation.yaml`` holds facts that differ on every compile (timestamp, hostname,
output directory). It is a file of its own so that none of them reaches ``normalized.yaml``
or ``plan.yaml``, which keeps ``plan_hash`` stable (fasthep-flow#94).
"""

from __future__ import annotations

import hashlib
import re
import socket
import warnings
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from hepflow.compiler.input_paths import resolve_input_path
from hepflow.compiler.inputs_manifest import InputCollector
from hepflow.utils import read_yaml, write_yaml

INVOCATION_NAME = "invocation.yaml"
SOURCE_ROLES = frozenset({"workflow", "include", "profile"})
PLAN_HASH_CANONICALIZATION = "root-prefix"
_ROOT_PLACEHOLDER = "<root>"


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
    write_yaml(
        {
            "workflow_path": workflow_path,
            "outdir": outdir,
            "root": root,
            "timestamp": datetime.now(UTC).isoformat(timespec="seconds"),
            "hostname": socket.gethostname(),
            "caller": dict(invocation or {}),
        },
        path,
    )
    return path


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
    lexical root and its symlink-resolved form are replaced, at path boundaries only.
    """
    text = Path(plan_path).read_text(encoding="utf-8")
    forms = {root.rstrip("/") if len(root) > 1 else root}
    if root:
        forms.add(str(Path(root).resolve()).rstrip("/"))
    for form in sorted(filter(None, forms), key=len, reverse=True):
        # Replace only whole path prefixes: /a/b must not rewrite /a/bc/x.
        text = re.sub(
            rf"(?<![\w.\-/]){re.escape(form)}(?![\w.\-])", _ROOT_PLACEHOLDER, text
        )
    return {
        "sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
        "canonicalization": PLAN_HASH_CANONICALIZATION,
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
