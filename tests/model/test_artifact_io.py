from __future__ import annotations

import pytest

from hepflow.model.io import ArtifactManifest, ArtifactReference


@pytest.mark.parametrize(
    ("location", "path_type"),
    [
        ("artifacts/plots/a.png", "relative_to_outdir"),
        ("/x/y/a.png", "absolute"),
        ("root://eos.example//store/a.root", "uri"),
        ("file:///x/a.png", "uri"),
    ],
)
def test_path_type_is_derived(location: str, path_type: str) -> None:
    assert (
        ArtifactReference(uri=location, product_kind="artifact").path_type == path_type
    )


def test_reference_round_trip_keeps_size_and_checksum() -> None:
    ref = ArtifactReference(
        uri="a/b.root",
        product_kind="artifact",
        size_bytes=12,
        adler32="0000abcd",
        delivery="ok",
    )
    data = ref.to_dict()
    assert data["size_bytes"] == 12
    assert data["adler32"] == "0000abcd"
    assert data["delivery"] == "ok"
    assert data["path_type"] == "relative_to_outdir"
    back = ArtifactReference.from_dict(data)
    assert (back.size_bytes, back.adler32, back.delivery) == (12, "0000abcd", "ok")


def test_reference_without_optional_fields_omits_them() -> None:
    data = ArtifactReference(uri="a.png", product_kind="artifact").to_dict()
    assert "size_bytes" not in data
    assert "adler32" not in data
    assert "delivery" not in data


def test_manifest_root_round_trip() -> None:
    part = ArtifactReference(uri="p/0.png", product_kind="artifact")
    manifest = ArtifactManifest(
        product_kind="artifact",
        format=None,
        producer_node="n",
        output_name="artifact",
        dataset_name=None,
        parts=(part,),
        root="p",
    )
    data = manifest.to_dict()
    assert data["root"] == "p"
    assert ArtifactManifest.from_dict(data).root == "p"


def test_manifest_root_defaults_to_none() -> None:
    manifest = ArtifactManifest(
        product_kind="artifact",
        format=None,
        producer_node="n",
        output_name="artifact",
        dataset_name=None,
        parts=(),
    )
    assert manifest.root is None
    assert "root" not in manifest.to_dict()


def test_delivery_rejects_unknown_status() -> None:
    with pytest.raises(ValueError, match="delivery"):
        ArtifactReference(uri="a", product_kind="artifact", delivery="lost")


@pytest.mark.parametrize(
    "status", ["ok", "missing", "size_mismatch", "checksum_mismatch", "error"]
)
def test_delivery_accepts_known_statuses(status: str) -> None:
    assert (
        ArtifactReference(uri="a", product_kind="artifact", delivery=status).delivery
        == status
    )
