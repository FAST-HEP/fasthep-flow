"""A toy persistence capability: a JSON dict of counts, one payload file."""

from __future__ import annotations

import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from hepflow.model.persistence import SavedPayload, SaveOutcome


def save_toy_counts(
    value: Any, *, root: Path, params: dict[str, Any], identity: dict[str, Any]
) -> SaveOutcome:
    root.mkdir(parents=True, exist_ok=True)
    (root / "counts.json").write_text(
        json.dumps(value, sort_keys=True, indent=params.get("indent")),
        encoding="utf-8",
    )
    return SaveOutcome(payloads=(SavedPayload("counts.json", role="data"),))


def load_toy_counts(
    root: Path,
    payloads: Sequence[Path],
    *,
    params: dict[str, Any],
    variant: str | None,
    locator: str | None,
) -> Any:
    return json.loads(Path(payloads[0]).read_text(encoding="utf-8"))
