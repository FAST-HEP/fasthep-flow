from __future__ import annotations

import json
from pathlib import Path

import pytest

from hepflow.model.persistence import StorageBinding, StorageCall
from hepflow.model.product_manifest import PayloadRef, ProductEntry
from hepflow.runtime.persistence.errors import (
    ManifestNotPublished,
    MissingEvidence,
    MissingPayload,
    SizeOrHashMismatch,
)
from hepflow.runtime.persistence.manifest import (
    describe_payload,
    read_manifest,
    write_product_set,
)


def _entry(folder: Path, text: str = '{"a": 1}') -> ProductEntry:
    folder.mkdir(parents=True, exist_ok=True)
    (folder / "counts.json").write_text(text, encoding="utf-8")
    return ProductEntry(
        producer_node="stage.Fill",
        output_name="counts",
        product_kind="toy_counts",
        scope="dataset",
        dataset_name="Zmumu",
        storage=StorageBinding(
            capability="toy.counts",
            capability_version="1",
            container="json",
            save=StorageCall("pkg:save", "0.1"),
            load=StorageCall("pkg:load", "0.1"),
        ),
        payloads=(describe_payload(folder, "counts.json", role="data"),),
    )


def test_manifest_round_trips_and_validates(tmp_path: Path) -> None:
    entry = _entry(tmp_path / "set")
    path = write_product_set(
        tmp_path / "set", [entry], {"records": []}, plan_hash="sha256:abc"
    )

    manifest = read_manifest(path)

    assert path.name == "manifest.json"
    assert manifest.schema_version == "1"
    assert manifest.plan_hash == "sha256:abc"
    assert manifest.products == (entry,)
    assert manifest.evidence == "evidence.json"


def test_manifest_written_last_and_atomically(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    folder = tmp_path / "set"
    entry = _entry(folder)
    seen: dict[str, bool] = {}
    real_replace = Path.replace

    def spy(self: Path, target: Path) -> Path:
        seen["evidence_first"] = (folder / "evidence.json").exists()
        seen["final_absent"] = not (folder / "manifest.json").exists()
        seen["same_dir"] = self.parent == Path(target).parent
        return real_replace(self, target)

    monkeypatch.setattr(Path, "replace", spy)
    write_product_set(folder, [entry], {}, plan_hash="h")

    assert seen == {"evidence_first": True, "final_absent": True, "same_dir": True}


def test_crash_before_rename_leaves_nothing_published(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    folder = tmp_path / "set"
    entry = _entry(folder)

    def boom(self: Path, target: Path) -> Path:
        raise OSError("disk full")

    monkeypatch.setattr(Path, "replace", boom)
    with pytest.raises(OSError, match="disk full"):
        write_product_set(folder, [entry], {}, plan_hash="h")

    assert not (folder / "manifest.json").exists()
    with pytest.raises(ManifestNotPublished):
        read_manifest(folder / "manifest.json")


def test_missing_manifest_means_unpublished(tmp_path: Path) -> None:
    folder = tmp_path / "set"
    _entry(folder)
    (folder / "evidence.json").write_text("{}", encoding="utf-8")

    with pytest.raises(ManifestNotPublished):
        read_manifest(folder / "manifest.json")


def test_missing_payload_names_producer_output_kind(tmp_path: Path) -> None:
    folder = tmp_path / "set"
    path = write_product_set(folder, [_entry(folder)], {}, plan_hash="h")
    (folder / "counts.json").unlink()

    with pytest.raises(MissingPayload) as caught:
        read_manifest(path)

    assert caught.value.code == "missing_payload"
    assert (
        caught.value.producer_node,
        caught.value.output_name,
        caught.value.kind,
    ) == ("stage.Fill", "counts", "toy_counts")


def test_same_size_edit_caught_by_sha256(tmp_path: Path) -> None:
    folder = tmp_path / "set"
    path = write_product_set(folder, [_entry(folder, '{"a": 1}')], {}, plan_hash="h")
    (folder / "counts.json").write_text('{"a": 2}', encoding="utf-8")

    with pytest.raises(SizeOrHashMismatch, match="sha256"):
        read_manifest(path)


def test_size_mismatch(tmp_path: Path) -> None:
    folder = tmp_path / "set"
    path = write_product_set(folder, [_entry(folder)], {}, plan_hash="h")
    (folder / "counts.json").write_text('{"a": 1, "b": 2}', encoding="utf-8")

    with pytest.raises(SizeOrHashMismatch, match="size"):
        read_manifest(path)


def test_missing_evidence(tmp_path: Path) -> None:
    folder = tmp_path / "set"
    path = write_product_set(folder, [_entry(folder)], {}, plan_hash="h")
    (folder / "evidence.json").unlink()

    with pytest.raises(MissingEvidence):
        read_manifest(path)


def test_evidence_without_manifest_is_ignored_on_restore(tmp_path: Path) -> None:
    folder = tmp_path / "set"
    _entry(folder)
    (folder / "evidence.json").write_text(json.dumps({"x": 1}), encoding="utf-8")

    with pytest.raises(ManifestNotPublished):
        read_manifest(folder / "manifest.json")


def test_payload_ref_is_plain_data() -> None:
    ref = PayloadRef(uri="a.json", size_bytes=2, sha256="00", role=None)
    assert PayloadRef.from_dict(ref.to_dict()) == ref
