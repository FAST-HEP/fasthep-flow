from __future__ import annotations

from typing import Any

import pytest

from hepflow.runtime.persistence import errors
from hepflow.runtime.persistence.evidence import (
    EvidenceLedger,
    build_evidence,
    import_evidence,
)
from hepflow.runtime.provenance.recorder import ProvenanceRecorder

IDENTITY = {
    "producer_node": "stage.Fill",
    "output_name": "hist",
    "product_kind": "histogram",
    "dataset_name": "Zmumu",
    "partition_id": "p0",
}


def _evidence(**overrides: Any) -> dict[str, Any]:
    evidence: dict[str, Any] = {
        "schema_version": "1",
        "identity": dict(IDENTITY),
        "attempt": {"id": "a1", "status": "succeeded"},
        "executions": {
            "stage.Fill::p0": {
                "id": "stage.Fill::p0",
                "node_id": "stage.Fill",
                "partition": "p0",
                "operations": [{"inputs": {"resources": ["jets"]}}],
            }
        },
        "resources": {
            "jets": {"id": "jets", "kind": "dataset", "path": "/store/jets.root"}
        },
    }
    evidence.update(overrides)
    return evidence


def test_identical_import_is_idempotent() -> None:
    ledger = EvidenceLedger()
    recorder = ProvenanceRecorder()

    first = import_evidence(ledger, _evidence(), store=recorder.store)
    second = import_evidence(ledger, _evidence(), store=recorder.store)

    assert (first, second) == ("imported", "unchanged")
    assert len(ledger.producers) == 1
    # the producer's execution was registered once, not twice
    [execution] = recorder.store.executions().values()
    assert len(execution.operations) == 1


def test_same_identity_different_content_conflicts() -> None:
    ledger = EvidenceLedger()
    import_evidence(ledger, _evidence())
    changed = _evidence()
    changed["executions"]["stage.Fill::p0"]["impl"] = "other.impl"

    with pytest.raises(errors.ConflictingProvenance) as caught:
        import_evidence(ledger, changed)

    assert caught.value.code == "conflicting_provenance"
    assert caught.value.producer_node == "stage.Fill"
    assert caught.value.output_name == "hist"
    assert caught.value.kind == "histogram"


def test_retries_stay_distinct() -> None:
    ledger = EvidenceLedger()
    import_evidence(ledger, _evidence())
    retry = _evidence(attempt={"id": "a2", "status": "succeeded"})
    retry["executions"]["stage.Fill::p0"]["impl"] = "other.impl"

    assert import_evidence(ledger, retry) == "imported"
    assert len(ledger.producers) == 2


def test_failed_attempt_never_a_successful_producer() -> None:
    ledger = EvidenceLedger()
    recorder = ProvenanceRecorder()

    status = import_evidence(
        ledger,
        _evidence(attempt={"id": "a1", "status": "failed"}),
        store=recorder.store,
    )

    assert status == "imported"
    assert ledger.producers == {}
    assert len(ledger.failed_attempts) == 1
    assert recorder.store.executions() == {}


def test_restore_adds_consumption_not_producer(tmp_path: Any) -> None:
    from tests.toy_components.persistence import (  # noqa: PLC0415
        publish_toy_set,
        registry_with,
        toy_capability,
    )

    from hepflow.runtime.persistence.restore import restore_manifest  # noqa: PLC0415

    manifest = publish_toy_set(tmp_path)
    ledger = EvidenceLedger()

    restore_manifest(manifest, registry_with(toy_capability()), ledger=ledger)
    restore_manifest(manifest, registry_with(toy_capability()), ledger=ledger)

    assert len(ledger.consumptions) == 2
    assert all(c["manifest"] == str(manifest) for c in ledger.consumptions)
    assert len(ledger.producers) == 1


def test_no_hook_replay(tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    from tests.toy_components.persistence import (  # noqa: PLC0415
        publish_toy_set,
        registry_with,
        toy_capability,
    )

    from hepflow.api import restore_products  # noqa: PLC0415

    def boom(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("restore must not run hooks")

    monkeypatch.setattr("hepflow.runtime.hooks.manager.HookManager.from_plan", boom)
    restore_products(
        publish_toy_set(tmp_path), registry=registry_with(toy_capability())
    )


@pytest.mark.parametrize("key", ["token", "x509_proxy", "Password", "api_secret"])
def test_no_credentials_in_evidence(key: str) -> None:
    evidence = _evidence()
    evidence["executions"]["stage.Fill::p0"]["params"] = {key: "abc"}

    with pytest.raises(errors.InvalidBinding, match="credential"):
        import_evidence(EvidenceLedger(), evidence)


def test_build_evidence_collects_the_partition_execution_and_resources() -> None:
    recorder = ProvenanceRecorder()
    with recorder.operation_context(
        node_id="stage.Fill",
        impl="hep.hist",
        role="transform",
        dataset="Zmumu",
        partition={"id": "p0"},
    ):
        recorder.record_operation(inputs={"resources": ["jets"]})
    with recorder.operation_context(
        node_id="stage.Other",
        impl="hep.x",
        role="transform",
        dataset="Zmumu",
        partition={"id": "p1"},
    ):
        recorder.record_operation()
    recorder.record_resource_record(
        "jets", {"kind": "dataset", "path": "/store/jets.root"}
    )

    evidence = build_evidence(
        recorder.store, identity=IDENTITY, attempt_id="a1", status="succeeded"
    )

    assert list(evidence["executions"]) == ["stage.Fill::p0"]
    assert list(evidence["resources"]) == ["jets"]
    assert evidence["attempt"] == {"id": "a1", "status": "succeeded"}
