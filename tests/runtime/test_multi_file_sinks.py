from __future__ import annotations

import zlib
from pathlib import Path
from typing import Any

import pytest

from hepflow.model.io import ArtifactManifest, ArtifactReference, OutputResult
from hepflow.model.plan import ExecutionNode
from hepflow.runtime.artifacts import (
    ArtifactMissingError,
    merge_artifact_products,
    validate_artifact_reference_like,
)
from hepflow.runtime.engine import _store_node_outputs
from hepflow.runtime.handlers import _run_writer_like_sink_located

NODE = "plot.Heat"


def _sink(names: list[str], *, report: list[str] | None = None) -> Any:
    """Write ``names`` below the sink's root; report ``report`` (default: the same names)."""

    def impl(target: Any, *, ctx: Any = None, **params: Any) -> list[OutputResult]:
        for name in names:
            path = ctx.artifact_path(name)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"abc")
        return [OutputResult(kind="artifact", path=name) for name in report or names]

    return impl


def _run(
    tmp_path: Path, impl: Any, *, params: dict[str, Any] | None = None, **extra: Any
) -> Any:
    ctx = {
        "outdir": str(tmp_path / "build"),
        "dataset_name": "ds",
        "partition": {"id": "p0", "dataset": "ds", "part": "0_0"},
        **extra,
    }
    result, root = _run_writer_like_sink_located(
        sink_name="toy",
        impl=impl,
        target={"x": 1},
        params={"out": "heat.png", **(params or {})},
        ctx=ctx,
        meta={"node_id": NODE},
    )
    store: dict[tuple[str, str], Any] = {}
    _store_node_outputs(
        NODE,
        {"artifact": "artifact"},
        result,
        store,
        ctx=ctx,
        artifact_root=root,
        sink="toy",
    )
    return store[(NODE, "artifact")]


def test_three_files_become_manifest_with_root(tmp_path: Path) -> None:
    out = _run(tmp_path, _sink(["heat/ds_a.png", "heat/ds_b.png", "heat/ds_c.png"]))

    assert isinstance(out, ArtifactManifest)
    assert out.root == "artifacts/plots"
    assert out.producer_node == NODE
    assert out.output_name == "artifact"
    assert out.dataset_name == "ds"
    assert [part.path for part in out.parts] == [
        "artifacts/plots/heat/ds_a.png",
        "artifacts/plots/heat/ds_b.png",
        "artifacts/plots/heat/ds_c.png",
    ]
    assert all(part.producer_node == NODE for part in out.parts)


def test_records_match_files_on_disk(tmp_path: Path) -> None:
    out = _run(tmp_path, _sink(["heat/1.png", "heat/2.png"]))

    for part in out.parts:
        assert (tmp_path / "build" / part.path).is_file()
        assert part.size_bytes == 3
        assert part.path_type == "relative_to_outdir"


def test_single_file_stays_bare_reference(tmp_path: Path) -> None:
    out = _run(tmp_path, _sink(["heat/only.png"]))

    assert isinstance(out, ArtifactReference)
    assert out.path == "artifacts/plots/heat/only.png"


def test_empty_list_is_no_files_and_no_manifest(tmp_path: Path) -> None:
    assert _run(tmp_path, _sink([])) == []


def test_duplicate_location_fails_naming_node(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match=NODE):
        _run(tmp_path, _sink(["a.png"], report=["a.png", "a.png"]))


def test_file_outside_root_is_recorded_absolute(tmp_path: Path) -> None:
    elsewhere = tmp_path / "elsewhere" / "x.png"
    elsewhere.parent.mkdir()
    elsewhere.write_bytes(b"abc")

    def impl(target: Any, *, ctx: Any = None, **params: Any) -> list[OutputResult]:
        inside = ctx.artifact_path("in.png")
        inside.parent.mkdir(parents=True, exist_ok=True)
        inside.write_bytes(b"abc")
        return [
            OutputResult(kind="artifact", path="in.png"),
            OutputResult(kind="artifact", path=str(elsewhere)),
        ]

    out = _run(tmp_path, impl)

    assert {part.path_type for part in out.parts} == {"relative_to_outdir", "absolute"}
    assert elsewhere.as_posix() in [part.path for part in out.parts]


def test_missing_reported_file_fails_node(tmp_path: Path) -> None:
    with pytest.raises(ArtifactMissingError, match=NODE):
        _run(tmp_path, _sink(["a.png"], report=["a.png", "never_written.png"]))


def test_delivered_files_are_destination_uris_with_checksums(tmp_path: Path) -> None:
    destination = "root://eos.example//store/out/"
    job = tmp_path / "job"

    out = _run(
        tmp_path,
        _sink(["1.png", "2.png"]),
        params={"path": "heat/x.root"},
        artifact_root=str(job),
        output_destination=destination,
    )

    assert out.root == "root://eos.example//store/out/heat"
    assert [part.path for part in out.parts] == [
        "root://eos.example//store/out/heat/1.png",
        "root://eos.example//store/out/heat/2.png",
    ]
    assert all(part.path_type == "uri" for part in out.parts)


def test_destination_checksum_is_recorded_for_local_files(tmp_path: Path) -> None:
    out = _run(
        tmp_path,
        _sink(["1.png", "2.png"]),
        output_destination="root://eos.example//store/out/",
    )

    # No backend base: files stay in the bundle, the checksum is still recorded.
    assert all(part.adler32 == f"{zlib.adler32(b'abc'):08x}" for part in out.parts)


def _node() -> ExecutionNode:
    return ExecutionNode(
        id=NODE,
        graph_node_id=NODE,
        role="sink",
        impl="toy",
        outputs={"artifact": "artifact"},
    )


def _manifest(root: str | None, index: int) -> ArtifactManifest:
    return ArtifactManifest(
        product_kind="artifact",
        format=None,
        producer_node=NODE,
        output_name="artifact",
        dataset_name="ds",
        parts=(
            OutputResult(
                kind="artifact",
                path=f"{root or 'x'}/{index}_a.png",
                producer_node=NODE,
                output_name="artifact",
                dataset_name="ds",
                partition_index=index,
            ),
            OutputResult(
                kind="artifact",
                path=f"{root or 'x'}/{index}_b.png",
                producer_node=NODE,
                output_name="artifact",
                dataset_name="ds",
                partition_index=index + 10,
            ),
        ),
        root=root,
    )


def test_merged_dataset_manifest_takes_the_sink_root() -> None:
    merged = merge_artifact_products(
        [_manifest("artifacts/plots/heat", 0), _manifest("artifacts/plots/heat", 1)],
        node=_node(),
        output_name="artifact",
    )

    assert isinstance(merged, ArtifactManifest)
    assert merged.root == "artifacts/plots/heat"


def test_merged_manifest_has_no_root_when_inputs_disagree() -> None:
    merged = merge_artifact_products(
        [_manifest("a", 0), _manifest("b", 1)], node=_node(), output_name="artifact"
    )

    assert isinstance(merged, ArtifactManifest)
    assert merged.root is None


def test_merge_skips_a_sink_that_wrote_nothing() -> None:
    merged = merge_artifact_products(
        [[], _manifest("artifacts/plots/heat", 0)], node=_node(), output_name="artifact"
    )

    assert isinstance(merged, ArtifactManifest)
    assert len(merged.parts) == 2


def test_merge_of_only_empty_results_is_an_empty_manifest() -> None:
    merged = merge_artifact_products([[], []], node=_node(), output_name="artifact")

    assert isinstance(merged, ArtifactManifest)
    assert merged.parts == ()


def test_boundary_validation_accepts_an_empty_result_but_not_other_lists() -> None:
    validate_artifact_reference_like([], context="x")
    with pytest.raises(ValueError, match="expected an ArtifactReference"):
        validate_artifact_reference_like([1], context="x")


def test_merge_keeps_size_checksum_and_delivery_of_a_writer_file() -> None:
    reference = OutputResult(
        kind="artifact",
        path="artifacts/files/skim/ds/0_0.root",
        producer_node=NODE,
        output_name="artifact",
        dataset_name="ds",
        partition_index=0,
        size_bytes=2571,
        adler32="0a0b0c0d",
        delivery="ok",
        metadata={"writer_manifest": {"kind": "root_tree", "partition": 0, "dataset": "ds"}},
    )

    merged = merge_artifact_products([reference], node=_node(), output_name="artifact")

    assert isinstance(merged, ArtifactManifest)
    [part] = merged.parts
    assert (part.size_bytes, part.adler32, part.delivery) == (2571, "0a0b0c0d", "ok")
