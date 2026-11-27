from __future__ import annotations

import os
import zlib
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from hepflow.compiler.input_paths import is_uri
from hepflow.model.io import (
    DELIVERY_STATUSES,
    ArtifactManifest,
    ArtifactReference,
    OutputResult,
)
from hepflow.model.plan import ExecutionNode


class ArtifactMissingError(ValueError):
    """A sink reported a local file that does not exist (or is not a file)."""


def normalise_location(
    uri: str,
    outdir: str | Path,
    *,
    root: str | Path | None = None,
) -> str:
    """
    Express where a sink put an artifact the way Flow stores it.

    A sink returns a path relative to its ``root`` (its artifact root, the
    directory it works in), an absolute path, or a URI. Flow stores a URI
    unchanged, a path inside ``outdir`` relative to it (POSIX separators) and
    any other path as an absolute path. Without ``root`` a relative input is
    read from the current working directory, never from ``outdir``; a relative
    ``root`` is too. Run this once per result in the process that produced it.
    Paths are made absolute and normalised lexically; symlinks are not
    resolved. An ``outdir`` that is a URI leaves the input unchanged.
    """
    if is_uri(uri) or is_uri(str(outdir)):
        return uri
    cwd = Path.cwd()
    start = Path(os.path.normpath(cwd / root)) if root is not None else cwd
    location = Path(os.path.normpath(start / uri))
    root = Path(os.path.normpath(cwd / outdir))
    try:
        return location.relative_to(root).as_posix()
    except ValueError:
        return location.as_posix()


def localise_artifacts(
    value: Any, outdir: str | Path, *, root: str | Path | None = None
) -> None:
    """Normalise the location of every artifact reference in ``value`` in place."""
    if isinstance(value, ArtifactManifest):
        for part in value.parts:
            localise_artifacts(part, outdir, root=root)
    elif isinstance(value, ArtifactReference):
        value.path = normalise_location(value.path, outdir, root=root)
    elif isinstance(value, dict) and isinstance(value.get("path"), str):
        value["path"] = normalise_location(value["path"], outdir, root=root)
    elif isinstance(value, list | tuple):
        for item in value:
            localise_artifacts(item, outdir, root=root)


def deliver_location(location: str, *, base: str | Path, destination: str) -> str:
    """
    The place a backend delivers a file to: ``destination`` joined with its path below ``base``.

    A backend that delivers files itself sets ``base`` (its job directory) and
    ``destination``. A location that is not an absolute path below ``base`` is
    returned unchanged. Run after :func:`localise_artifacts` and
    :func:`stat_artifacts`, which still need the local file.
    """
    if is_uri(location) or not Path(location).is_absolute():
        return location
    try:
        relative = Path(location).relative_to(Path(os.path.normpath(Path.cwd() / base)))
    except ValueError:
        return location
    return destination.rstrip("/") + "/" + relative.as_posix()


def deliver_artifacts(value: Any, *, base: str | Path, destination: str) -> None:
    """Record every artifact reference in ``value`` at its delivered location, in place."""
    if isinstance(value, ArtifactManifest):
        for part in value.parts:
            deliver_artifacts(part, base=base, destination=destination)
    elif isinstance(value, ArtifactReference):
        value.path = deliver_location(value.path, base=base, destination=destination)
    elif isinstance(value, list | tuple):
        for item in value:
            deliver_artifacts(item, base=base, destination=destination)


def wrap_artifact_results(
    items: list[ArtifactReference],
    *,
    node_id: str,
    output_name: str,
    dataset_name: str | None,
    root: str | None,
) -> ArtifactReference | ArtifactManifest | list[ArtifactReference]:
    """
    Turn the files one sink output reported into its recorded product.

    No file stays an empty list (a sink may write nothing), one file a bare
    reference, several a manifest tagged with the node identity and the output's
    ``root``. Two files with one location fail, naming the node.
    """
    seen: set[str] = set()
    for item in items:
        if item.uri in seen:
            raise ValueError(
                f"Node {node_id!r} reported {item.uri!r} more than once in one result"
            )
        seen.add(item.uri)
    if not items:
        return []
    if len(items) == 1:
        return items[0]
    return ArtifactManifest(
        product_kind="artifact",
        producer_node=node_id,
        output_name=output_name,
        dataset_name=dataset_name,
        parts=tuple(items),
        metadata={"node_id": node_id},
        root=root,
    )


def stat_artifacts(
    value: Any,
    *,
    outdir: str | Path,
    node_id: str,
    sink: str,
    with_adler32: bool,
) -> None:
    """
    Record ``size_bytes`` (and ``adler32`` when asked) of every local file in ``value``.

    Run after :func:`localise_artifacts`, in the process that produced the result.
    A relative location is read from ``outdir``. A local location that is not a
    file raises :class:`ArtifactMissingError` naming the sink, the node and the
    path; URIs are not checked.
    """
    if isinstance(value, ArtifactManifest):
        for part in value.parts:
            stat_artifacts(
                part,
                outdir=outdir,
                node_id=node_id,
                sink=sink,
                with_adler32=with_adler32,
            )
    elif isinstance(value, ArtifactReference):
        if value.path_type == "uri":
            return
        path = Path(value.uri)
        if not path.is_absolute():
            path = Path(outdir) / path
        if not path.is_file():
            raise ArtifactMissingError(
                f"Sink {sink!r} of node {node_id!r} reported {value.uri!r}, "
                "which is not a file that exists"
            )
        value.size_bytes = path.stat().st_size
        if with_adler32:
            value.adler32 = adler32_of_file(path)
    elif isinstance(value, list | tuple):
        for item in value:
            stat_artifacts(
                item,
                outdir=outdir,
                node_id=node_id,
                sink=sink,
                with_adler32=with_adler32,
            )


def _references(value: Any) -> list[ArtifactReference]:
    if isinstance(value, ArtifactManifest):
        return list(value.parts)
    if isinstance(value, ArtifactReference):
        return [value]
    if isinstance(value, list | tuple):
        return [ref for item in value for ref in _references(item)]
    return []


def apply_delivery_status(value: Any, statuses: Mapping[str, str]) -> None:
    """
    Record the per-file delivery outcome a backend reports.

    ``statuses`` maps a recorded location (the file's ``path``) to one of
    ``DELIVERY_STATUSES``. A location that names no file in ``value`` raises
    ``KeyError``, an unknown status ``ValueError``. A failed delivery is recorded;
    it never fails the run. Flow does not copy or check files itself.
    """
    references = {ref.uri: ref for ref in _references(value)}
    unknown = sorted(set(statuses) - set(references))
    if unknown:
        raise KeyError(f"No artifact at {unknown} to record a delivery status for")
    for status in statuses.values():
        if status not in DELIVERY_STATUSES:
            raise ValueError(
                f"delivery must be one of {DELIVERY_STATUSES}; got {status!r}"
            )
    for location, status in statuses.items():
        references[location].delivery = status


def delivery_counts(value: Any) -> dict[str, int]:
    """How many files of ``value`` have each delivery status (every status is a key)."""
    counts = dict.fromkeys(DELIVERY_STATUSES, 0)
    for ref in _references(value):
        if ref.delivery is not None:
            counts[ref.delivery] += 1
    return counts


def adler32_of_file(path: Path) -> str:
    """adler32 of a file as eight hex digits (the checksum EOS keeps)."""
    value = 1
    with path.open("rb") as stream:
        while chunk := stream.read(8 * 1024 * 1024):
            value = zlib.adler32(chunk, value)
    return f"{value & 0xFFFFFFFF:08x}"


def is_artifact_reference_like(value: Any) -> bool:
    """A reference, a manifest, or the empty result of a sink that wrote no file."""
    if isinstance(value, list | tuple):
        return not value
    return isinstance(value, ArtifactReference | ArtifactManifest)


def validate_artifact_reference_like(value: Any, *, context: str) -> None:
    if is_artifact_reference_like(value):
        return
    raise ValueError(
        f"{context} expected an ArtifactReference or ArtifactManifest; "
        f"got {type(value).__name__}"
    )


def merge_artifact_products(
    values: list[Any],
    *,
    node: ExecutionNode,
    output_name: str,
    dataset_name: str | None = None,
) -> ArtifactManifest | list[ArtifactManifest]:
    parts = _flatten_artifact_values(values)
    if not parts:
        return ArtifactManifest(
            product_kind="artifact",
                producer_node=node.id,
            output_name=output_name,
            dataset_name=dataset_name,
            parts=(),
        )

    groups: dict[str | None, list[ArtifactReference]] = {}
    for part in parts:
        groups.setdefault(part.dataset_name, []).append(part)

    roots = _agreed_roots(values)
    manifests = [
        _manifest_for_parts(
            parts_for_dataset,
            node=node,
            output_name=output_name,
            dataset_name=dataset,
            enforce_dataset=dataset_name,
            root=roots.get(dataset),
        )
        for dataset, parts_for_dataset in groups.items()
    ]
    manifests.sort(key=lambda manifest: str(manifest.dataset_name or ""))

    if dataset_name is None and len(manifests) > 1:
        return manifests
    return manifests[0]


def _agreed_roots(values: Iterable[Any]) -> dict[str | None, str]:
    """Per dataset, the root all merged input manifests share (single-file results have none)."""
    seen: dict[str | None, set[str | None]] = {}
    for value in values:
        if isinstance(value, ArtifactManifest):
            seen.setdefault(value.dataset_name, set()).add(value.root)
    return {
        dataset: next(iter(roots))  # type: ignore[misc]
        for dataset, roots in seen.items()
        if len(roots) == 1 and None not in roots
    }


def _flatten_artifact_values(values: Iterable[Any]) -> list[ArtifactReference]:
    parts: list[ArtifactReference] = []
    for value in values:
        if isinstance(value, ArtifactManifest):
            parts.extend(value.parts)
            continue
        if isinstance(value, ArtifactReference):
            parts.append(_with_reference_defaults(value))
            continue
        validate_artifact_reference_like(value, context="artifact merge")  # [] writes no file
    return parts


def _with_reference_defaults(reference: ArtifactReference) -> ArtifactReference:
    writer_manifest = reference.metadata.get("writer_manifest")
    if not isinstance(writer_manifest, dict):
        return reference
    if (
        reference.product_kind != "artifact"
        and reference.producer_node is not None
        and reference.output_name is not None
        and reference.dataset_name is not None
    ):
        return reference
    return OutputResult(
        kind=reference.product_kind,
        path=reference.uri,
        metadata=dict(reference.metadata),
        provenance=dict(reference.provenance),
        producer_node=reference.producer_node or writer_manifest.get("node_id"),
        output_name=reference.output_name or "artifact",
        dataset_name=reference.dataset_name or writer_manifest.get("dataset"),
        partition_id=reference.partition_id,
        partition_index=(
            reference.partition_index
            if reference.partition_index is not None
            else writer_manifest.get("partition")
        ),
        size_bytes=reference.size_bytes,
        adler32=reference.adler32,
        delivery=reference.delivery,
    )


def _manifest_for_parts(
    parts: list[ArtifactReference],
    *,
    node: ExecutionNode,
    output_name: str,
    dataset_name: str | None,
    enforce_dataset: str | None,
    root: str | None = None,
) -> ArtifactManifest:
    first = parts[0]
    product_kind = _logical_product_kind(first)
    producer_node = first.producer_node or node.id
    reference_output = first.output_name or output_name
    seen_partitions: dict[str, ArtifactReference] = {}

    for part in parts:
        _validate_part_compatible(
            part,
            product_kind=product_kind,
            producer_node=producer_node,
            output_name=reference_output,
            dataset_name=dataset_name,
            enforce_dataset=enforce_dataset,
        )
        partition_key = _partition_key(part)
        if partition_key in seen_partitions:
            previous = seen_partitions[partition_key]
            if previous.to_dict() != part.to_dict():
                raise ValueError(
                    f"Conflicting artifact references for partition {partition_key!r}"
                )
            raise ValueError(
                f"Duplicate artifact reference for partition {partition_key!r}"
            )
        seen_partitions[partition_key] = part

    return ArtifactManifest(
        product_kind=product_kind,
        producer_node=producer_node,
        output_name=reference_output,
        dataset_name=dataset_name,
        parts=tuple(sorted(parts, key=_part_order_key)),
        metadata={"node_id": node.id},
        root=root,
    )


def _validate_part_compatible(
    part: ArtifactReference,
    *,
    product_kind: str,
    producer_node: str,
    output_name: str,
    dataset_name: str | None,
    enforce_dataset: str | None,
) -> None:
    if _logical_product_kind(part) != product_kind:
        raise ValueError("Incompatible artifact product kinds in one manifest")
    if (part.producer_node or producer_node) != producer_node:
        raise ValueError("Incompatible artifact producer nodes in one manifest")
    if (part.output_name or output_name) != output_name:
        raise ValueError("Incompatible artifact output names in one manifest")
    if dataset_name is not None and part.dataset_name not in {None, dataset_name}:
        raise ValueError(
            f"Artifact reference dataset {part.dataset_name!r} does not match "
            f"manifest dataset {dataset_name!r}"
        )
    if enforce_dataset is not None and part.dataset_name != enforce_dataset:
        raise ValueError(
            f"Artifact reference dataset {part.dataset_name!r} does not match "
            f"dataset boundary {enforce_dataset!r}"
        )


def _logical_product_kind(part: ArtifactReference) -> str:
    writer_manifest = part.metadata.get("writer_manifest")
    if isinstance(writer_manifest, dict):
        return str(writer_manifest.get("kind") or part.product_kind)
    return str(part.product_kind)


def _partition_key(part: ArtifactReference) -> str:
    if part.partition_id is not None:
        return f"id:{part.partition_id}"
    if part.partition_index is not None:
        return f"index:{part.partition_index}"
    writer_manifest = part.metadata.get("writer_manifest")
    if (
        isinstance(writer_manifest, dict)
        and writer_manifest.get("partition") is not None
    ):
        return f"index:{writer_manifest['partition']}"
    return f"uri:{part.uri}"


def _part_order_key(part: ArtifactReference) -> tuple[int, str, str]:
    if part.partition_index is not None:
        return (int(part.partition_index), str(part.partition_id or ""), part.uri)
    writer_manifest = part.metadata.get("writer_manifest")
    if (
        isinstance(writer_manifest, dict)
        and writer_manifest.get("partition") is not None
    ):
        return (
            int(writer_manifest["partition"]),
            str(part.partition_id or ""),
            part.uri,
        )
    return (0, str(part.partition_id or ""), part.uri)
