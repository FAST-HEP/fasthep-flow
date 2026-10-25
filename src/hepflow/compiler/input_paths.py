"""Resolve input paths at compile time against an explicit project root.

Rules (see fasthep-flow#86):

* URI values (``root://``, ``https://``, ``file://``) are left untouched and never hashed.
* ``~`` expands to the home directory; environment variables are not expanded.
* Absolute paths are kept and hashed when readable; an unreadable one is recorded
  with ``sha256 = None`` and a warning, because the compile host may differ from the site
  that runs the plan (for example ``/cvmfs`` on a laptop).
* Relative paths resolve against the project root and must exist. For one release a path
  that is missing under the root falls back to the entry file's directory with a
  ``FutureWarning``.
"""

from __future__ import annotations

import os
import re
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fasthep_toolbench.misc import file_fingerprint

_URI_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9+.\-]+://")


@dataclass(frozen=True)
class ResolvedInput:
    """One input path as declared and as resolved by the compiler."""

    role: str
    declared: str
    resolved: str
    origin: str  # "root" | "absolute" | "entry_fallback" | "uri"
    sha256: str | None
    size: int | None
    note: str | None


def resolve_project_root(root: str | Path | None) -> Path:
    """Return the absolute project root: ``root`` if given, else the current directory."""
    path = _normalise(Path(root) if root is not None else Path.cwd())
    if not path.is_dir():
        raise FileNotFoundError(
            f"project root {str(path)!r} is not an existing directory"
        )
    return path


def resolve_path_value(
    value: Any,
    *,
    role: str,
    root: Path,
    entry_dir: Path | None,
    label: str | None = None,
) -> tuple[str | list[str], list[ResolvedInput]]:
    """Resolve a path declaration that is a string or a list of strings.

    List items get the role ``<role>[<index>]``. ``label`` names the declaration in the
    ``TypeError`` raised for any other value type (default: ``role``).
    """
    if isinstance(value, str):
        entry = resolve_input_path(value, role=role, root=root, entry_dir=entry_dir)
        return entry.resolved, [entry]
    if isinstance(value, list) and all(isinstance(item, str) for item in value):
        entries = [
            resolve_input_path(
                item, role=f"{role}[{index}]", root=root, entry_dir=entry_dir
            )
            for index, item in enumerate(value)
        ]
        return [entry.resolved for entry in entries], entries
    raise TypeError(f"{label or role} must be a string or a list of strings")


def is_uri(value: str) -> bool:
    """Return whether ``value`` has a ``scheme://`` form (drive letters are not URIs)."""
    return bool(_URI_PATTERN.match(value))


def resolve_input_path(
    value: str,
    *,
    role: str,
    root: Path,
    entry_dir: Path | None = None,
) -> ResolvedInput:
    """Resolve one declared input path; ``role`` names it in messages and the manifest."""
    declared = value
    text = value.strip()
    if not text:
        raise ValueError(f"{role}: input path is empty")
    if is_uri(text):
        return ResolvedInput(role, declared, text, "uri", None, None, None)

    expanded = Path(text).expanduser()
    if expanded.is_absolute():
        return _describe(role, declared, _normalise(expanded), "absolute", strict=False)

    candidate = _normalise(root / expanded)
    if candidate.exists():
        return _describe(role, declared, candidate, "root", strict=True)

    if entry_dir is not None:
        fallback = _normalise(entry_dir / expanded)
        if fallback.exists():
            warnings.warn(
                f"{role}: path {declared!r} was not found under the project root "
                f"{root}; using the entry file's directory {entry_dir}. Paths now "
                "resolve against the project root; this fallback is deprecated.",
                FutureWarning,
                stacklevel=2,
            )
            return _describe(role, declared, fallback, "entry_fallback", strict=True)

    raise FileNotFoundError(
        f"{role}: path {declared!r} not found under the project root {root}"
    )


def _normalise(path: Path) -> Path:
    """Make ``path`` absolute and collapse ``..`` lexically (symlinks are not resolved)."""
    if not path.is_absolute():
        path = Path.cwd() / path
    return Path(os.path.normpath(path))


def _describe(
    role: str,
    declared: str,
    resolved: Path,
    origin: str,
    *,
    strict: bool,
) -> ResolvedInput:
    if resolved.is_dir():
        return ResolvedInput(
            role, declared, str(resolved), origin, None, None, "directory"
        )
    try:
        digest, size = file_fingerprint(resolved)
    except OSError as exc:
        if strict:
            raise
        reason = exc.strerror or type(exc).__name__
        warnings.warn(
            f"{role}: input {declared!r} is unavailable at compile time ({reason}); "
            "it is recorded without a hash",
            UserWarning,
            stacklevel=3,
        )
        return ResolvedInput(
            role, declared, str(resolved), origin, None, None, f"unavailable: {reason}"
        )
    return ResolvedInput(role, declared, str(resolved), origin, digest, size, None)

