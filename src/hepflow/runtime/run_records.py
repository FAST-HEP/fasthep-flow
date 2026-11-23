"""Run roots of the per-dataset mode: the binding, attempt ids and per-partition summaries.

A run root ``runs/[<variation>/]<dataset>/`` belongs to exactly one plan and one manifest. Its
``binding.json`` says which, is created by the first job and checked by every later one, so a
changed plan or manifest fails before any job writes a file. Each partition ``<N>/`` keeps its
records below its own directory; its ``run_summary.yaml`` is written last, so its presence means
the partition completed.
"""

from __future__ import annotations

import json
import os
import secrets
import tempfile
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

from hepflow.build_layout import BuildPaths
from hepflow.compiler.bundle import plan_hash
from hepflow.compiler.dataset_units import PublishConflict, _umask, publish_if_absent

BINDING_FIELDS = ("variation", "plan_hash", "manifest_hash", "chunk_size")


@dataclass(frozen=True, slots=True)
class Binding:
    """What a run root is bound to."""

    variation: str | None
    plan_hash: str
    manifest_hash: str
    chunk_size: int | None

    def to_bytes(self) -> bytes:
        return (json.dumps(asdict(self), indent=2, sort_keys=True) + "\n").encode("utf-8")


def build_binding(
    plan_file: str | Path,
    *,
    manifest: Mapping[str, Any],
    variation: str | None,
) -> Binding:
    """The binding of the plan file and manifest a job has loaded.

    ``plan_hash`` is the whole-plan hash of ``compiler.bundle`` (the shared plan holds no root),
    so it equals the one in the unit's ``inputs.manifest.yaml``.
    """
    return Binding(
        variation=variation,
        plan_hash=plan_hash(Path(plan_file), "")["sha256"],
        manifest_hash=str(manifest["record_hash"]),
        chunk_size=manifest.get("chunk_size"),
    )


def publish_binding(binding_file: str | Path, binding: Binding) -> bool:
    """Create ``binding.json`` unless it exists; fail if it holds another binding.

    Returns True when this call created it. The error names every field that differs, before the
    job has written anything.
    """
    target = Path(binding_file)
    try:
        return publish_if_absent(target, binding.to_bytes())
    except PublishConflict:
        existing = json.loads(target.read_text(encoding="utf-8"))
        wanted = asdict(binding)
        differing = [
            f"{key}: {existing.get(key)!r} in {target.name}, {wanted[key]!r} now"
            for key in BINDING_FIELDS
            if existing.get(key) != wanted[key]
        ]
        raise PublishConflict(
            f"{target} binds this run root to another plan or manifest "
            f"({'; '.join(differing) or 'content differs'}); recompile into a new out-dir "
            "or remove the run directory."
        ) from None


def new_attempt_id(now: datetime | None = None) -> str:
    """``<UTC timestamp to the second>-<4 hex>``, for example ``2026-11-19T140322Z-a3f9``."""
    stamp = (now or datetime.now(UTC)).astimezone(UTC).strftime("%Y-%m-%dT%H%M%SZ")
    return f"{stamp}-{secrets.token_hex(2)}"


def claim_attempt_dir(attempts_dir: str | Path) -> tuple[str, Path]:
    """Create a new attempt directory with an exclusive ``mkdir``; a clash picks a new id."""
    base = Path(attempts_dir)
    base.mkdir(parents=True, exist_ok=True)
    while True:
        attempt_id = new_attempt_id()
        directory = base / attempt_id
        try:
            directory.mkdir()
        except FileExistsError:
            continue
        return attempt_id, directory


def partition_summary(
    *,
    dataset: str,
    partition: str,
    partitions: int,
    binding: Binding,
    attempt_id: str,
    run: Mapping[str, Any],
) -> dict[str, Any]:
    """The run summary of one partition: what ran, under which binding, in which attempt."""
    return {
        **dict(run),
        "variation": binding.variation,
        "dataset": dataset,
        "partition": int(partition),
        "partitions": partitions,
        "chunk_size": binding.chunk_size,
        "plan_hash": binding.plan_hash,
        "manifest_hash": binding.manifest_hash,
        "attempt": attempt_id,
    }


def partition_outputs(run_dir: str | Path) -> list[dict[str, Any]]:
    """The data files a partition wrote, from its writer manifests (path, size and adler32).

    Nothing is opened but the manifests. A partition that wrote no file has no entry.
    """
    outputs: list[dict[str, Any]] = []
    for manifest_file in sorted((Path(run_dir) / "writers").glob("*/manifest.json")):
        manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
        for dataset in (manifest.get("datasets") or {}).values():
            for item in dataset.get("files") or []:
                entry = {
                    key: item[key]
                    for key in ("path", "path_type", "size_bytes", "adler32")
                    if key in item
                }
                outputs.append(entry)
    return sorted(outputs, key=lambda entry: str(entry["path"]))


def write_partition_summary(paths: BuildPaths, summary: Mapping[str, Any]) -> Path:
    """Write ``run_summary.yaml`` of a partition run last and atomically.

    A rerun of a completed partition replaces it (the last complete attempt wins).
    """
    target = paths.run_summary()
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(dir=target.parent, prefix=f".{target.name}.")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            os.fchmod(handle.fileno(), 0o666 & ~_umask())
            yaml.safe_dump(dict(summary), handle, sort_keys=False)
            handle.flush()
            os.fsync(handle.fileno())
        Path(temp_name).replace(target)
    except BaseException:
        Path(temp_name).unlink(missing_ok=True)
        raise
    return target
