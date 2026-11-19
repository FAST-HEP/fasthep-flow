"""Dataset units: frozen per-dataset manifests and create-if-absent publication.

A per-dataset compile writes shared files (the plan, dependencies, graph) that every
dataset's compile produces byte for byte, and one unit directory per dataset. Many jobs
may compile concurrently into one output root, so a shared file is published once and
never replaced: the first writer wins, later writers verify that their content is the
same and fail with both hashes when it is not.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml

from hepflow.compiler.partitioning import derive_dataset_partitions
from hepflow.model.plan import ExecutionPartition, ExecutionPlan

MANIFEST_VERSION = 1
MANIFEST_NAME = "manifest.yaml"


class PublishConflict(RuntimeError):
    """A published file already exists with other content."""


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _umask() -> int:
    mask = os.umask(0)
    os.umask(mask)
    return mask


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
            # mkstemp creates 0600 and the hard link keeps it; readers on other uids need
            # what a plain write would give.
            os.fchmod(handle.fileno(), 0o666 & ~_umask())
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


def dataset_manifest(
    record: Mapping[str, Any],
    *,
    chunk_size: int | None,
    workflow_hash: str | None = None,
) -> dict[str, Any]:
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
        "chunk_size": chunk_size,
    }
    if workflow_hash is not None:
        manifest["workflow_hash"] = workflow_hash
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


#: Keys of a dataset record that stay in the shared plan; the rest is file data.
SHARED_DATASET_KEYS = ("name", "eventtype", "group", "meta", "chunk_size")
MANIFEST_TEMPLATE = "datasets/{dataset}/manifest.yaml"


def unit_dir(compile_directory: str | Path, dataset: str) -> Path:
    """Directory of one dataset's unit below a compile (or variation) directory."""
    if not dataset or dataset in {".", ".."} or "/" in dataset or "\\" in dataset:
        raise ValueError(f"Dataset name {dataset!r} cannot name a unit directory")
    return Path(compile_directory) / "datasets" / dataset


#: Keys of a normalized workflow that say where it was compiled from, not what it is.
_LOCATION_KEYS = ("workflow_path", "project_root", "provenance")


def workflow_content_hash(normalized: Mapping[str, Any]) -> str:
    """Hash of what a workflow is, independent of where it was compiled from.

    Dataset records are reduced to their metadata (the plan's view) and the keys that
    name the compile location are left out, so the same workflow addressed by another
    path hashes the same, and any change to its steps, registry or metadata does not.
    """
    content = {
        key: value for key, value in normalized.items() if key not in _LOCATION_KEYS
    }
    data = dict(content.get("data") or {})
    data["datasets"] = [
        {key: record[key] for key in SHARED_DATASET_KEYS if key in record}
        for record in list(data.get("datasets") or [])
    ]
    content["data"] = data
    text = json.dumps(content, sort_keys=True, default=str)
    return _sha256(text.encode("utf-8"))


def _without_location(provenance: Mapping[str, Any]) -> dict[str, Any]:
    """Provenance of a shared plan: layer names and kinds, no host paths or input files."""
    result = {
        key: value
        for key, value in provenance.items()
        if key not in {"root", "inputs"}
    }
    for key, value in list(result.items()):
        if key.endswith("_layers") and isinstance(value, list):
            result[key] = [
                {
                    k: v
                    for k, v in layer.items()
                    if k != "path" or str(v).startswith("package:")
                }
                for layer in value
            ]
    return result


def shared_plan_dict(
    plan: ExecutionPlan, *, workflow_hash: str | None = None
) -> dict[str, Any]:
    """The plan every dataset's compile publishes: no files, counts or partitions.

    Dataset records keep their metadata only, and the partitioning section names the
    manifest template that :func:`assemble_unit_partitions` fills per dataset. Nothing
    names where the workflow was compiled from (that lives in each unit's
    ``invocation.yaml``); ``workflow_hash`` identifies what it is.
    """
    doc = plan.to_dict()
    context = dict(doc.get("context") or {})
    context.pop("workflow_path", None)
    context.pop("project_root", None)
    if workflow_hash is not None:
        context["workflow_hash"] = workflow_hash
    if doc.get("provenance"):
        doc["provenance"] = _without_location(doc["provenance"])
    context["datasets"] = {
        name: {key: record[key] for key in SHARED_DATASET_KEYS if key in record}
        for name, record in dict(context.get("datasets") or {}).items()
    }
    context.pop("chunk_sizes", None)
    doc["context"] = context
    execution = dict(doc.get("execution") or {})
    partitioning = dict(execution.get("partitioning") or {})
    partitioning["manifest"] = MANIFEST_TEMPLATE
    execution["partitioning"] = partitioning
    doc["execution"] = execution
    doc["partitions"] = []
    # The chunk size of a dataset is frozen in its manifest, not in the shared plan.
    for node in doc.get("nodes") or []:
        if (node.get("partitioning") or {}).get("chunk_size") is not None:
            node["partitioning"] = {**node["partitioning"], "chunk_size": None}
    return doc


def _restore_location(plan: ExecutionPlan, manifest_path: Path) -> None:
    """Put the compile location of a unit back into the context, as old-mode plans have it."""
    invocation = manifest_path.parent / "invocation.yaml"
    if not invocation.is_file():
        return
    record = yaml.safe_load(invocation.read_text(encoding="utf-8")) or {}
    if record.get("workflow_path"):
        plan.context["workflow_path"] = str(record["workflow_path"])
    if record.get("root"):
        plan.context["project_root"] = str(record["root"])


def assemble_unit_partitions(
    plan: ExecutionPlan, plan_dir: str | Path, dataset: str | None = None
) -> None:
    """Fill a shared plan from the manifests of its compiled dataset units.

    ``dataset`` selects one unit; without it every compiled unit is loaded, in the plan's
    dataset order. Files and counts come from the manifests, partitions are derived with
    the chunk size frozen in them, and ``context.chunk_sizes`` records that size.
    """
    template = str((plan.execution.get("partitioning") or {}).get("manifest") or "")
    if not template:
        return
    names = [str(name) for name in plan.context.get("dataset_names") or []]
    manifests: dict[str, dict[str, Any]] = {}
    present = [
        name for name in names if (Path(plan_dir) / template.format(dataset=name)).is_file()
    ]
    if dataset is not None and dataset not in present:
        compiled = ", ".join(present) or "none"
        raise ValueError(
            f"No compiled unit for dataset {dataset!r}; compiled datasets: {compiled}."
        )
    # Only the requested unit is read: a damaged sibling must not block this dataset.
    for name in [dataset] if dataset is not None else present:
        manifests[name] = read_manifest(Path(plan_dir) / template.format(dataset=name))

    if dataset is not None:
        _restore_location(plan, Path(plan_dir) / template.format(dataset=dataset))
    datasets = plan.context["datasets"]
    sources = [node for node in plan.nodes if node.role == "source"]
    partitions: list[ExecutionPartition] = []
    chunk_sizes: dict[str, int | None] = {}
    for name, manifest in manifests.items():
        record = {
            **dict(datasets.get(name) or {}),
            "name": name,
            "files": manifest["files"],
            "nevents": manifest["nevents"],
            "file_nevents": (
                None
                if not any(manifest["counted"])
                else manifest["file_nevents"]
            ),
        }
        datasets[name] = record
        chunk_sizes[name] = manifest["chunk_size"]
        for source in sources:
            source_name = str(
                source.meta.get("source_name") or source.id.removeprefix("read.")
            )
            partitions.extend(
                derive_dataset_partitions(
                    record,
                    source_name=source_name,
                    chunk_size=manifest["chunk_size"],
                    local_numbers=True,
                )
            )
    plan.context["chunk_sizes"] = chunk_sizes
    plan.partitions = partitions
