from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from hepflow.model.io import ArtifactManifest, ArtifactReference, OutputResult
from hepflow.model.plan import ExecutionNode


def is_artifact_reference_like(value: Any) -> bool:
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
            format=None,
            producer_node=node.id,
            output_name=output_name,
            dataset_name=dataset_name,
            parts=(),
        )

    groups: dict[str | None, list[ArtifactReference]] = {}
    for part in parts:
        groups.setdefault(part.dataset_name, []).append(part)

    manifests = [
        _manifest_for_parts(
            parts_for_dataset,
            node=node,
            output_name=output_name,
            dataset_name=dataset,
            enforce_dataset=dataset_name,
        )
        for dataset, parts_for_dataset in groups.items()
    ]
    manifests.sort(key=lambda manifest: str(manifest.dataset_name or ""))

    if dataset_name is None and len(manifests) > 1:
        return manifests
    return manifests[0]


def _flatten_artifact_values(values: Iterable[Any]) -> list[ArtifactReference]:
    parts: list[ArtifactReference] = []
    for value in values:
        if isinstance(value, ArtifactManifest):
            parts.extend(value.parts)
            continue
        if isinstance(value, ArtifactReference):
            parts.append(_with_reference_defaults(value))
            continue
        validate_artifact_reference_like(value, context="artifact merge")
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
        format=reference.format,
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
    )


def _manifest_for_parts(
    parts: list[ArtifactReference],
    *,
    node: ExecutionNode,
    output_name: str,
    dataset_name: str | None,
    enforce_dataset: str | None,
) -> ArtifactManifest:
    first = parts[0]
    product_kind = _logical_product_kind(first)
    producer_node = first.producer_node or node.id
    reference_output = first.output_name or output_name
    artifact_format = _artifact_format(first)
    seen_partitions: dict[str, ArtifactReference] = {}

    for part in parts:
        _validate_part_compatible(
            part,
            product_kind=product_kind,
            producer_node=producer_node,
            output_name=reference_output,
            dataset_name=dataset_name,
            enforce_dataset=enforce_dataset,
            artifact_format=artifact_format,
        )
        partition_key = _partition_key(part)
        if partition_key in seen_partitions:
            previous = seen_partitions[partition_key]
            if previous.to_dict() != part.to_dict():
                raise ValueError(
                    "Conflicting artifact references for partition "
                    f"{partition_key!r}"
                )
            raise ValueError(f"Duplicate artifact reference for partition {partition_key!r}")
        seen_partitions[partition_key] = part

    return ArtifactManifest(
        product_kind=product_kind,
        format=artifact_format,
        producer_node=producer_node,
        output_name=reference_output,
        dataset_name=dataset_name,
        parts=tuple(sorted(parts, key=_part_order_key)),
        metadata={"node_id": node.id},
    )


def _validate_part_compatible(
    part: ArtifactReference,
    *,
    product_kind: str,
    producer_node: str,
    output_name: str,
    dataset_name: str | None,
    enforce_dataset: str | None,
    artifact_format: str | None,
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
    if artifact_format is not None and _artifact_format(part) not in {None, artifact_format}:
        raise ValueError("Incompatible artifact formats in one manifest")


def _logical_product_kind(part: ArtifactReference) -> str:
    writer_manifest = part.metadata.get("writer_manifest")
    if isinstance(writer_manifest, dict):
        return str(writer_manifest.get("kind") or part.product_kind)
    return str(part.product_kind)


def _artifact_format(part: ArtifactReference) -> str | None:
    writer_manifest = part.metadata.get("writer_manifest")
    if isinstance(writer_manifest, dict) and writer_manifest.get("format") is not None:
        return str(writer_manifest["format"])
    if part.format is not None:
        return str(part.format)
    return None


def _partition_key(part: ArtifactReference) -> str:
    if part.partition_id is not None:
        return f"id:{part.partition_id}"
    if part.partition_index is not None:
        return f"index:{part.partition_index}"
    writer_manifest = part.metadata.get("writer_manifest")
    if isinstance(writer_manifest, dict) and writer_manifest.get("partition") is not None:
        return f"index:{writer_manifest['partition']}"
    return f"uri:{part.uri}"


def _part_order_key(part: ArtifactReference) -> tuple[int, str, str]:
    if part.partition_index is not None:
        return (int(part.partition_index), str(part.partition_id or ""), part.uri)
    writer_manifest = part.metadata.get("writer_manifest")
    if isinstance(writer_manifest, dict) and writer_manifest.get("partition") is not None:
        return (int(writer_manifest["partition"]), str(part.partition_id or ""), part.uri)
    return (0, str(part.partition_id or ""), part.uri)
