"""The ten named persistence errors (``docs/execution/persistence.md``).

Each carries the producer node, the output name and the product kind, and a stable
``code`` equal to the name in the schema.
"""

from __future__ import annotations


class PersistenceError(Exception):
    """Base class: a product could not be published or restored."""

    code = "persistence_error"

    def __init__(
        self,
        message: str,
        *,
        producer_node: str,
        output_name: str,
        kind: str,
    ) -> None:
        self.producer_node = producer_node
        self.output_name = output_name
        self.kind = kind
        super().__init__(
            f"{self.code}: {message} "
            f"(producer {producer_node!r}, output {output_name!r}, kind {kind!r})"
        )


class UnknownCapability(PersistenceError):
    code = "unknown_capability"


class UnsupportedVersion(PersistenceError):
    code = "unsupported_version"


class MissingReader(PersistenceError):
    code = "missing_reader"


class InvalidBinding(PersistenceError):
    code = "invalid_binding"


class KindMismatch(PersistenceError):
    code = "kind_mismatch"


class VariantMismatch(PersistenceError):
    code = "variant_mismatch"


class MissingPayload(PersistenceError):
    code = "missing_payload"


class SizeOrHashMismatch(PersistenceError):
    code = "size_or_hash_mismatch"


class MissingEvidence(PersistenceError):
    code = "missing_evidence"


class ConflictingProvenance(PersistenceError):
    code = "conflicting_provenance"


class ManifestNotPublished(FileNotFoundError):
    """There is no ``manifest.json``: nothing was published."""


ERROR_CODES = (
    "unknown_capability",
    "unsupported_version",
    "missing_reader",
    "invalid_binding",
    "kind_mismatch",
    "variant_mismatch",
    "missing_payload",
    "size_or_hash_mismatch",
    "missing_evidence",
    "conflicting_provenance",
)
