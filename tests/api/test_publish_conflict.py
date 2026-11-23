from __future__ import annotations

import hepflow.api
from hepflow.compiler.dataset_units import PublishConflict


def test_publish_conflict_is_public_api() -> None:
    # Per-dataset compiles and runs raise it for a changed shared plan or binding; the CLI
    # reports it as an error message instead of a traceback.
    assert hepflow.api.PublishConflict is PublishConflict
    assert "PublishConflict" in hepflow.api.__all__
    assert issubclass(PublishConflict, RuntimeError)
