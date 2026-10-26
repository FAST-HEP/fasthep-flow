"""The opt-in snapshot of a compile's source files: ``compile/inputs/`` (fasthep-flow#94).

The snapshot mirrors the root-relative tree of the files the compile recorded, so that
``root=<compile dir>/inputs`` compiles the same workflow again. Rules, per manifest row:

* text files under the project root up to ``COPY_LIMIT_BYTES`` are copied; a copy larger
  than ``WARN_LIMIT_BYTES`` warns;
* files that look like credentials (see :func:`looks_secret`) are neither copied nor linked;
* other files under the root (binary, over the cap, under ``/cvmfs``) and directories are
  symlinked at their root-relative place, so that the tree still resolves where the
  original is available;
* files outside the root, URIs and unreadable files are recorded by path and hash only.

The snapshot directory carries a marker file; a compile removes a marked snapshot left by
an earlier compile and never touches an unmarked directory.
"""

from __future__ import annotations

import codecs
import fnmatch
import os
import shutil
import warnings
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from fasthep_toolbench.misc import file_sha256

SNAPSHOT_DIRNAME = "inputs"
SNAPSHOT_MARKER = ".hepflow-snapshot"
COPY_LIMIT_BYTES = 100 * 1024 * 1024
WARN_LIMIT_BYTES = 1024 * 1024
CVMFS_PREFIXES: tuple[str, ...] = ("/cvmfs",)
_SNIFF_BYTES = 8192

# Base names (matched case-insensitively) that are never copied.
SECRET_NAME_PATTERNS = (
    ".env",
    ".env.*",
    "*.env",
    ".netrc",
    ".pgpass",
    ".htpasswd",
    "id_rsa*",
    "id_dsa*",
    "id_ecdsa*",
    "id_ed25519*",
    "x509up_*",
    "*.pem",
    "*.key",
    "*.p12",
    "*.pfx",
    "*.jks",
    "*.keytab",
    "*secret*",
    "*token*",
    "*password*",
    "*passwd*",
    "*credential*",
)
# Directory names (matched case-insensitively) below which nothing is copied.
SECRET_DIRECTORIES = frozenset(
    {".ssh", ".gnupg", ".globus", ".aws", ".docker", ".kube", "private", "secrets"}
)
SECRET_CONTENT_MARKERS = (b"PRIVATE KEY-----",)


def snapshot_dir(compile_dir: str | Path) -> Path:
    """The snapshot directory of a compile directory."""
    return Path(compile_dir) / SNAPSHOT_DIRNAME


def looks_secret(path: Path, *, content: bytes | None = None) -> bool:
    """Return whether a file must never be copied because it may hold a credential.

    ``path`` is matched by base name and directory names (pass a root-relative path, so
    that the location of the project itself does not count); ``content`` is the start of
    the file, checked for a private-key marker.
    """
    name = path.name.lower()
    if any(fnmatch.fnmatchcase(name, pattern) for pattern in SECRET_NAME_PATTERNS):
        return True
    if any(part.lower() in SECRET_DIRECTORIES for part in path.parent.parts):
        return True
    return bool(content) and any(
        marker in (content or b"") for marker in SECRET_CONTENT_MARKERS
    )


def clear_snapshot(compile_dir: str | Path) -> None:
    """Remove a snapshot left by an earlier compile; leave an unmarked directory alone."""
    target = snapshot_dir(compile_dir)
    if (target / SNAPSHOT_MARKER).is_file() and not target.is_symlink():
        shutil.rmtree(target)


def write_snapshot(
    compile_dir: str | Path,
    rows: Iterable[Mapping[str, Any]],
    root: str | None,
) -> dict[str, bool]:
    """Write ``compile_dir/inputs`` and return, per resolved path, whether it was copied.

    Raises ``FileExistsError`` when ``inputs`` exists and is not a snapshot (no marker).
    """
    target = snapshot_dir(compile_dir)
    clear_snapshot(compile_dir)
    if target.exists() or target.is_symlink():
        raise FileExistsError(
            f"{target} exists and is not a compile snapshot; move it away to write one"
        )
    target.mkdir(parents=True)
    (target / SNAPSHOT_MARKER).write_text(
        "Snapshot of the compile sources written by hepflow; see inputs.manifest.yaml.\n",
        encoding="utf-8",
    )
    copied: dict[str, bool] = {}
    directories: list[tuple[Path, Path]] = []
    for row in rows:
        resolved = row.get("resolved")
        if not isinstance(resolved, str) or resolved in copied:
            continue
        copied[resolved] = False
        if root is None or row.get("origin") == "uri" or row.get("sha256") is None:
            if row.get("note") == "directory" and root is not None:
                relative = _relative_to_root(Path(resolved), Path(root))
                if relative is not None and relative != Path():
                    directories.append((Path(resolved), relative))
            continue
        relative = _relative_to_root(Path(resolved), Path(root))
        if relative is None:
            continue
        copied[resolved] = _snapshot_file(
            Path(resolved), relative, target, expected=row.get("sha256")
        )
    # Directories last: a file must never be written through a directory link.
    for source, relative in directories:
        destination = target / relative
        if (
            not destination.exists()
            and not destination.is_symlink()
            and _inside(destination.parent, target)
        ):
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.symlink_to(source, target_is_directory=True)
    return copied


def _snapshot_file(
    source: Path, relative: Path, target: Path, *, expected: Any
) -> bool:
    destination = target / relative
    if destination.exists() or destination.is_symlink():
        # The same file reached through another row (for example a symlinked root).
        return destination.is_file() and not destination.is_symlink()
    try:
        size = source.stat().st_size
        with source.open("rb") as handle:
            head = handle.read(_SNIFF_BYTES)
    except OSError:
        return False
    if looks_secret(relative, content=head):
        warnings.warn(
            f"snapshot: {relative} looks like a credential file and is not copied",
            UserWarning,
            stacklevel=4,
        )
        return False
    if not _inside(destination.parent, target):
        return False
    destination.parent.mkdir(parents=True, exist_ok=True)
    if _under_cvmfs(source) or not _is_text(head):
        destination.symlink_to(source)
        return False
    if size > COPY_LIMIT_BYTES:
        warnings.warn(
            f"snapshot: {relative} is larger than {COPY_LIMIT_BYTES} bytes and is not "
            "copied; it is recorded by path and hash",
            UserWarning,
            stacklevel=4,
        )
        destination.symlink_to(source)
        return False
    shutil.copyfile(source, destination)
    if expected is not None and file_sha256(destination) != expected:
        # The file changed after the compile hashed it: a copy would not match the row.
        destination.unlink()
        warnings.warn(
            f"snapshot: {relative} changed during the compile and is not copied",
            UserWarning,
            stacklevel=4,
        )
        return False
    if size > WARN_LIMIT_BYTES:
        warnings.warn(
            f"snapshot: {relative} is larger than {WARN_LIMIT_BYTES} bytes; it is "
            "copied into the snapshot",
            UserWarning,
            stacklevel=4,
        )
    return True


def _relative_to_root(path: Path, root: Path) -> Path | None:
    """``path`` relative to ``root``, lexically or after resolving symlinks; else ``None``."""
    try:
        return path.relative_to(root)
    except ValueError:
        pass
    try:
        return path.resolve().relative_to(root.resolve())
    except (ValueError, OSError):
        return None


def _under_cvmfs(path: Path) -> bool:
    candidates = {str(path), os.path.realpath(path)}
    return any(
        candidate == prefix or candidate.startswith(prefix.rstrip("/") + "/")
        for candidate in candidates
        for prefix in CVMFS_PREFIXES
    )


def _is_text(head: bytes) -> bool:
    """Whether the start of a file reads as UTF-8 text without NUL bytes."""
    if b"\x00" in head:
        return False
    try:
        codecs.getincrementaldecoder("utf-8")().decode(head, final=False)
    except UnicodeDecodeError:
        return False
    return True


def _inside(path: Path, directory: Path) -> bool:
    """Whether ``path`` (after resolving links that exist) stays below ``directory``."""
    return Path(os.path.realpath(path)).is_relative_to(os.path.realpath(directory))
