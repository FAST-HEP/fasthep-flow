from __future__ import annotations

import pytest

from hepflow.model.io import ArtifactManifest, ArtifactReference, OutputResult
from hepflow.model.persistence import StorageBinding, StorageCall


def _binding() -> StorageBinding:
    return StorageBinding(
        capability="histogram.data",
        capability_version="1",
        container="json",
        variant=None,
        locator="pt",
        save=StorageCall("pkg:save", "0.1", {"indent": 2}),
        load=StorageCall("pkg:load", "0.1", {}),
    )


def test_storage_binding_round_trips() -> None:
    binding = _binding()
    assert StorageBinding.from_dict(binding.to_dict()) == binding


def test_reference_carries_a_storage_binding_through_to_dict() -> None:
    ref = ArtifactReference(
        uri="a/pt.json", product_kind="histogram", storage=_binding()
    )
    back = ArtifactReference.from_dict(ref.to_dict())
    assert back.storage == _binding()


def test_reference_has_no_format() -> None:
    ref = ArtifactReference(uri="a.png", product_kind="artifact")
    assert not hasattr(ref, "format")
    assert "format" not in ref.to_dict()
    with pytest.raises(TypeError):
        ArtifactReference(uri="a.png", product_kind="artifact", format="png")  # type: ignore[call-arg]
    with pytest.raises(TypeError):
        OutputResult(kind="artifact", path="a.png", format="png")  # type: ignore[call-arg]


def test_manifest_has_no_format() -> None:
    manifest = ArtifactManifest(
        product_kind="artifact",
        producer_node="n",
        output_name="o",
        dataset_name=None,
        parts=(),
    )
    assert "format" not in manifest.to_dict()


@pytest.mark.parametrize("missing", ["capability", "capability_version", "container"])
def test_invalid_binding_rejected(missing: str) -> None:
    data = _binding().to_dict()
    data.pop(missing)
    with pytest.raises(ValueError, match=missing):
        StorageBinding.from_dict(data)
