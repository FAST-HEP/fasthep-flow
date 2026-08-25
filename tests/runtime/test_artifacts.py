from __future__ import annotations

import json

import pytest

from hepflow.model.io import ArtifactManifest, ArtifactReference, OutputResult
from hepflow.model.plan import ExecutionNode, ExecutionPlan, PlanInputRef
from hepflow.runtime.artifacts import merge_artifact_products
from hepflow.runtime.writer_manifests import write_writer_manifests


def test_artifact_reference_contract_serializes_metadata_and_remote_uri() -> None:
    ref = ArtifactReference(
        uri="s3://bucket/skims/data/part-0.root",
        product_kind="root_tree",
        format="rntuple",
        producer_node="write.Selected",
        output_name="artifact",
        dataset_name="data",
        partition_id="events__data__0",
        partition_index=0,
        metadata={"entries": 10},
    )

    roundtrip = ArtifactReference.from_dict(ref.to_dict())

    assert roundtrip.uri == "s3://bucket/skims/data/part-0.root"
    assert roundtrip.metadata == {"entries": 10}
    assert roundtrip.partition_id == "events__data__0"


def test_artifact_reference_rejects_missing_locator() -> None:
    with pytest.raises(ValueError, match="uri is required"):
        ArtifactReference(uri="", product_kind="root_tree")


def test_artifact_manifest_contract_is_read_only_and_serializable() -> None:
    ref = _reference(dataset="data", partition_index=0)
    manifest = ArtifactManifest(
        product_kind="root_tree",
        format="rntuple",
        producer_node="write.Selected",
        output_name="artifact",
        dataset_name="data",
        parts=(ref,),
        metadata={"tree": "Events"},
    )

    roundtrip = ArtifactManifest.from_dict(manifest.to_dict())

    assert roundtrip.parts[0].uri == ref.uri
    assert roundtrip.metadata == {"tree": "Events"}
    with pytest.raises(TypeError):
        manifest.metadata["tree"] = "Other"


def test_artifact_merge_produces_ordered_dataset_manifest_without_file_io() -> None:
    values = [
        _reference(dataset="data", partition_id="p2", partition_index=2),
        _reference(dataset="data", partition_id="p0", partition_index=0),
        _reference(dataset="data", partition_id="p1", partition_index=1),
    ]

    manifest = merge_artifact_products(
        values,
        node=_node(),
        output_name="artifact",
        dataset_name="data",
    )

    assert isinstance(manifest, ArtifactManifest)
    assert manifest.dataset_name == "data"
    assert [part.partition_index for part in manifest.parts] == [0, 1, 2]
    assert [part.uri for part in manifest.parts] == [
        "root://store/data/part-0.root",
        "root://store/data/part-1.root",
        "root://store/data/part-2.root",
    ]


def test_artifact_merge_flattens_nested_manifests() -> None:
    first = ArtifactManifest(
        product_kind="root_tree",
        format="rntuple",
        producer_node="write.Selected",
        output_name="artifact",
        dataset_name="data",
        parts=(_reference(dataset="data", partition_index=0),),
    )
    second = _reference(dataset="data", partition_index=1)

    manifest = merge_artifact_products(
        [first, second],
        node=_node(),
        output_name="artifact",
        dataset_name="data",
    )

    assert isinstance(manifest, ArtifactManifest)
    assert [part.partition_index for part in manifest.parts] == [0, 1]


def test_artifact_global_merge_preserves_dataset_identity() -> None:
    manifests = merge_artifact_products(
        [
            _reference(dataset="mc", partition_index=0),
            _reference(dataset="data", partition_index=0),
        ],
        node=_node(),
        output_name="artifact",
        dataset_name=None,
    )

    assert isinstance(manifests, list)
    assert [manifest.dataset_name for manifest in manifests] == ["data", "mc"]


def test_artifact_merge_rejects_incompatible_sets(
) -> None:
    cases = [
        (
            _reference(dataset="data", partition_index=0),
            _reference(dataset="mc", partition_index=1),
            "does not match dataset boundary",
        ),
        (
            _reference(dataset="data", producer_node="write.Other", partition_index=1),
            _reference(dataset="data", partition_index=0),
            "producer nodes",
        ),
        (
            _reference(dataset="data", format="ttree", partition_index=1),
            _reference(dataset="data", partition_index=0),
            "formats",
        ),
        (
            _reference(dataset="data", partition_id="same", partition_index=0),
            _reference(dataset="data", partition_id="same", partition_index=1),
            "Conflicting",
        ),
    ]
    for left, right, message in cases:
        with pytest.raises(ValueError, match=message):
            merge_artifact_products(
                [left, right],
                node=_node(),
                output_name="artifact",
                dataset_name="data",
            )


def test_output_result_is_artifact_reference_compatibility_adapter() -> None:
    result = OutputResult(kind="artifact", path="artifacts/files/out.root")

    assert isinstance(result, ArtifactReference)
    assert result.uri == "artifacts/files/out.root"
    assert result.path == "artifacts/files/out.root"
    assert result.kind == "artifact"


def test_writer_manifest_accepts_dataset_artifact_manifest(tmp_path) -> None:
    plan = ExecutionPlan()
    plan.add_node(
        ExecutionNode(
            id="write.Selected",
            graph_node_id="write.Selected",
            role="sink",
            impl="root_tree",
            inputs=[
                PlanInputRef(
                    node_id="stage.Selected",
                    output_name="stream",
                    input_name="target",
                )
            ],
            outputs={"artifact": "artifact"},
        )
    )
    parts = (
        _writer_reference(dataset="data", partition_index=1, entries=5),
        _writer_reference(dataset="data", partition_index=0, entries=7),
    )
    manifest = ArtifactManifest(
        product_kind="root_tree",
        format="rntuple",
        producer_node="write.Selected",
        output_name="artifact",
        dataset_name="data",
        parts=parts,
    )

    write_writer_manifests(
        plan,
        stores=[{("write.Selected", "artifact"): manifest}],
        outdir=tmp_path,
    )

    published = (tmp_path / "artifacts" / "files" / "selected" / "manifest.json")
    assert published.exists()
    payload = json.loads(published.read_text(encoding="utf-8"))
    assert [item["partition"] for item in payload["datasets"]["data"]["files"]] == [
        0,
        1,
    ]
    assert payload["total_entries"] == 12


def _reference(
    *,
    dataset: str,
    partition_index: int,
    partition_id: str | None = None,
    producer_node: str = "write.Selected",
    format: str = "rntuple",
) -> ArtifactReference:
    return ArtifactReference(
        uri=f"root://store/{dataset}/part-{partition_index}.root",
        product_kind="root_tree",
        format=format,
        producer_node=producer_node,
        output_name="artifact",
        dataset_name=dataset,
        partition_id=partition_id or f"events__{dataset}__{partition_index}",
        partition_index=partition_index,
        metadata={"tree": "Events"},
    )


def _writer_reference(
    *,
    dataset: str,
    partition_index: int,
    entries: int,
) -> OutputResult:
    path = f"artifacts/files/selected/{dataset}/0_{partition_index}.root"
    return OutputResult(
        kind="artifact",
        path=path,
        format="root",
        producer_node="write.Selected",
        output_name="artifact",
        dataset_name=dataset,
        partition_id=f"events__{dataset}__{partition_index}",
        partition_index=partition_index,
        metadata={
            "writer_manifest": {
                "kind": "root_tree",
                "name": "selected",
                "node_id": "write.Selected",
                "input_node": "stage.Selected",
                "tree": "Events",
                "format": "rntuple",
                "root_classname": "ROOT::RNTuple",
                "path": path,
                "path_type": "relative_to_outdir",
                "dataset": dataset,
                "partition": partition_index,
                "attempt": 0,
                "entries": entries,
                "size_bytes": 10,
            }
        },
    )


def _node() -> ExecutionNode:
    return ExecutionNode(
        id="write.Selected",
        graph_node_id="write.Selected",
        role="sink",
        impl="root_tree",
        outputs={"artifact": "artifact"},
    )
