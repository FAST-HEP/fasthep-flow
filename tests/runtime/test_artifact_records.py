from __future__ import annotations

import json
from pathlib import Path

import pytest

from hepflow.model.io import ArtifactManifest, OutputResult
from hepflow.model.plan import ExecutionNode, ExecutionPlan
from hepflow.runtime.provenance import PROVENANCE_VERSION, load_provenance_document
from hepflow.runtime.writer_manifests import write_writer_manifests

NODE = "plot.Heat"


def _group(
    tmp_path: Path,
) -> tuple[ExecutionPlan, dict[tuple[str, str], ArtifactManifest]]:
    plan = ExecutionPlan()
    plan.add_node(
        ExecutionNode(
            id=NODE,
            graph_node_id=NODE,
            role="sink",
            impl="hep.render.heatmap2d",
            outputs={"artifact": "artifact"},
        )
    )
    parts = []
    for index, dataset in enumerate(["a", "b", "c"]):
        location = f"artifacts/plots/heat/{dataset}.png"
        (tmp_path / location).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / location).write_bytes(b"png")
        parts.append(
            OutputResult(
                kind="artifact",
                path=location,
                producer_node=NODE,
                output_name="artifact",
                dataset_name=dataset,
                partition_index=index,
                size_bytes=3,
            )
        )
    manifest = ArtifactManifest(
        product_kind="artifact",
        producer_node=NODE,
        output_name="artifact",
        dataset_name=None,
        parts=tuple(parts),
        root="artifacts/plots/heat",
    )
    return plan, {(NODE, "artifact"): manifest}


def test_provenance_version_is_2() -> None:
    assert PROVENANCE_VERSION == "3.0"


def test_old_provenance_bundle_is_rejected(tmp_path: Path) -> None:
    old = tmp_path / "execution.json"
    old.write_text(json.dumps({"version": "1.0", "run_id": "r"}), encoding="utf-8")

    with pytest.raises(ValueError, match=r"1\.0"):
        load_provenance_document(old)


def test_multi_file_group_has_its_root_in_the_provenance_index(tmp_path: Path) -> None:
    plan, store = _group(tmp_path)

    write_writer_manifests(plan, stores=[store], outdir=tmp_path)

    manifest = json.loads(
        (tmp_path / "artifacts/provenance/manifest.json").read_text(encoding="utf-8")
    )
    assert manifest["version"] == PROVENANCE_VERSION
    assert len(manifest["records"]) == 3
    assert {record["root"] for record in manifest["records"]} == {
        "artifacts/plots/heat"
    }
    first = json.loads(
        (tmp_path / manifest["records"][0]["record"]).read_text(encoding="utf-8")
    )
    assert first["artifact"]["root"] == "artifacts/plots/heat"


def test_every_record_resolves_to_a_file(tmp_path: Path) -> None:
    plan, store = _group(tmp_path)

    write_writer_manifests(plan, stores=[store], outdir=tmp_path)

    manifest = json.loads(
        (tmp_path / "artifacts/provenance/manifest.json").read_text(encoding="utf-8")
    )
    assert [record["artifact"] for record in manifest["records"]] == [
        "artifacts/plots/heat/a.png",
        "artifacts/plots/heat/b.png",
        "artifacts/plots/heat/c.png",
    ]
    for record in manifest["records"]:
        assert (tmp_path / record["artifact"]).is_file()
