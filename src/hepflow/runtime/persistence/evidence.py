"""Evidence of how persisted products were produced, and its import.

Evidence is written next to a product set at save time (the producing partition's
executions and the resources they read) and imported when the set is restored.
Importing the same record again changes nothing; the same identity and attempt with
other content conflicts; a retry (another attempt) is a distinct record; a failed
attempt is kept apart and is never a successful producer. Restoring is a consumption
event, never a new scientific producer invocation.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from typing import Any

from hepflow.runtime.persistence.errors import ConflictingProvenance, InvalidBinding
from hepflow.runtime.provenance.model import ExecutionRecord, ResolvedResourceRecord
from hepflow.runtime.provenance.store import ProvenanceStore

EVIDENCE_SCHEMA_VERSION = "1"
_CREDENTIAL_KEY = re.compile(r"token|secret|passw|credential|x509|proxy|api_?key", re.I)

RecordKey = tuple[str, str, str | None, str | None, str]


@dataclass(slots=True)
class EvidenceLedger:
    """Imported evidence of a run: producers, failed attempts and consumptions."""

    producers: dict[RecordKey, dict[str, Any]] = field(default_factory=dict)
    failed_attempts: dict[RecordKey, dict[str, Any]] = field(default_factory=dict)
    consumptions: list[dict[str, Any]] = field(default_factory=list)


def build_evidence(
    store: ProvenanceStore,
    *,
    identity: dict[str, Any],
    attempt_id: str,
    status: str,
) -> dict[str, Any]:
    """Evidence for one product: its partition's executions and the resources they read."""
    partition = identity.get("partition_id")
    executions = {
        execution_id: execution.to_record()
        for execution_id, execution in sorted(store.executions().items())
        if partition is not None and execution.partition == partition
    }
    resource_ids = sorted(
        {
            resource_id
            for execution in executions.values()
            for operation in execution.get("operations", [])
            for resource_id in operation.get("inputs", {}).get("resources", [])
        }
    )
    resources = store.resources()
    return {
        "schema_version": EVIDENCE_SCHEMA_VERSION,
        "identity": dict(identity),
        "attempt": {"id": attempt_id, "status": status},
        "executions": executions,
        "resources": {
            resource_id: resources[resource_id].to_record()
            for resource_id in resource_ids
            if resource_id in resources
        },
    }


def forbid_credentials(data: Any, *, where: str, **identity: str) -> None:
    """Refuse credential-like keys anywhere in ``data`` (manifests, params, evidence)."""
    for path, key in _walk_keys(data, ""):
        if _CREDENTIAL_KEY.search(str(key)):
            raise InvalidBinding(
                f"{where} contains a credential-like key {key!r} at {path or '/'}",
                **identity,  # type: ignore[arg-type]
            )


def import_evidence(
    ledger: EvidenceLedger,
    evidence: dict[str, Any],
    *,
    store: ProvenanceStore | None = None,
) -> str:
    """Import one product's evidence; return ``"imported"`` or ``"unchanged"``."""
    identity = dict(evidence.get("identity") or {})
    who = {
        "producer_node": str(identity.get("producer_node", "")),
        "output_name": str(identity.get("output_name", "")),
        "kind": str(identity.get("product_kind", "")),
    }
    forbid_credentials(evidence, where="evidence", **who)
    attempt = dict(evidence.get("attempt") or {})
    key: RecordKey = (
        who["producer_node"],
        who["output_name"],
        identity.get("dataset_name"),
        identity.get("partition_id"),
        str(attempt.get("id", "")),
    )
    succeeded = attempt.get("status") == "succeeded"
    target = ledger.producers if succeeded else ledger.failed_attempts
    content = _digest(evidence)
    existing = target.get(key)
    if existing is not None:
        if existing["digest"] == content:
            return "unchanged"
        raise ConflictingProvenance(
            f"attempt {key[4]!r} of this product was imported with different content",
            **who,
        )
    target[key] = {"digest": content, "evidence": evidence}
    if succeeded and store is not None:
        _register(store, evidence, who)
    return "imported"


def record_consumption(
    ledger: EvidenceLedger, *, manifest: str, identities: list[dict[str, Any]]
) -> None:
    """Note that a run read a published set. It is not a producer invocation."""
    ledger.consumptions.append({"manifest": manifest, "products": identities})


def _register(
    store: ProvenanceStore, evidence: dict[str, Any], who: dict[str, str]
) -> None:
    for record in dict(evidence.get("executions") or {}).values():
        execution = ExecutionRecord.from_obj(record)
        for operation in execution.operations:
            store.register_operation_record(execution, operation)
    for resource_id, record in dict(evidence.get("resources") or {}).items():
        try:
            store.register_resource_record(
                resource_id, ResolvedResourceRecord.from_obj(resource_id, record)
            )
        except ValueError as exc:
            raise ConflictingProvenance(str(exc), **who) from exc


def _digest(evidence: dict[str, Any]) -> str:
    payload = json.dumps(evidence, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _walk_keys(data: Any, prefix: str) -> list[tuple[str, str]]:
    found: list[tuple[str, str]] = []
    if isinstance(data, dict):
        for key, value in data.items():
            path = f"{prefix}/{key}"
            found.append((path, str(key)))
            found.extend(_walk_keys(value, path))
    elif isinstance(data, list | tuple):
        for index, value in enumerate(data):
            found.extend(_walk_keys(value, f"{prefix}[{index}]"))
    return found
