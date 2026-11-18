"""Dataset units: frozen per-dataset manifests and create-if-absent publication.

A per-dataset compile writes shared files (the plan, dependencies, graph) that every
dataset's compile produces byte for byte, and one unit directory per dataset. Many jobs
may compile concurrently into one output root, so a shared file is published once and
never replaced: the first writer wins, later writers verify that their content is the
same and fail with both hashes when it is not.
"""

from __future__ import annotations

import hashlib
import os
import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml

MANIFEST_VERSION = 1
MANIFEST_NAME = "manifest.yaml"


class PublishConflict(RuntimeError):
    """A published file already exists with other content."""


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def publish_if_absent(path: str | Path, data: bytes) -> bool:
    """Create ``path`` with ``data`` unless it exists; never replace it.

    The content goes to a temporary file in the same directory, is flushed to disk and is
    then hard-linked to ``path``: ``os.link`` fails when the target exists, so concurrent
    writers cannot overwrite each other and a reader never sees a partial file. Returns
    True when this call created the file and False when it already held the same content;
    raises :class:`PublishConflict` naming the file and both hashes otherwise.
    """
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(dir=target.parent, prefix=f".{target.name}.")
    temp = Path(temp_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temp, target)
        except FileExistsError:
            existing = target.read_bytes()
            if existing == data:
                return False
            raise PublishConflict(
                f"{target} already exists with other content: "
                f"existing sha256 {_sha256(existing)}, new sha256 {_sha256(data)}"
            ) from None
        return True
    finally:
        temp.unlink(missing_ok=True)


def manifest_hash(manifest: Mapping[str, Any]) -> str:
    """Hash of a manifest's content, without its own ``record_hash``."""
    content = {key: value for key, value in manifest.items() if key != "record_hash"}
    text = yaml.safe_dump(content, sort_keys=True)
    return _sha256(text.encode("utf-8"))


def dataset_manifest(record: Mapping[str, Any], *, chunk_size: int) -> dict[str, Any]:
    """Freeze what partitioning a dataset needs: files, event counts and chunk size."""
    files = [str(item) for item in record.get("files") or []]
    counts = list(record.get("file_nevents") or [None] * len(files))
    manifest: dict[str, Any] = {
        "version": MANIFEST_VERSION,
        "dataset": str(record["name"]),
        "files": files,
        "nevents": record.get("nevents"),
        "file_nevents": counts,
        "counted": [count is not None for count in counts],
        "chunk_size": int(chunk_size),
    }
    manifest["record_hash"] = manifest_hash(manifest)
    return manifest


def manifest_bytes(manifest: Mapping[str, Any]) -> bytes:
    return yaml.safe_dump(dict(manifest), sort_keys=True).encode("utf-8")


def write_manifest(path: str | Path, manifest: Mapping[str, Any]) -> bool:
    """Publish a manifest create-if-absent; see :func:`publish_if_absent`."""
    return publish_if_absent(path, manifest_bytes(manifest))


def read_manifest(path: str | Path) -> dict[str, Any]:
    """Read a manifest and check its hash, so a damaged unit is not run."""
    manifest = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    if manifest.get("version") != MANIFEST_VERSION:
        raise ValueError(
            f"{path}: unsupported manifest version {manifest.get('version')!r}"
        )
    if manifest.get("record_hash") != manifest_hash(manifest):
        raise ValueError(f"{path}: record_hash does not match the manifest content")
    return dict(manifest)
