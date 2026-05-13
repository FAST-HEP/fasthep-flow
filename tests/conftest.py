from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml


@pytest.fixture
def toy_registry() -> dict[str, Any]:
    path = Path(__file__).parent / "fixtures" / "toy_registry.yaml"
    return yaml.safe_load(path.read_text(encoding="utf-8"))["registry"]


@pytest.fixture
def toy_author(toy_registry: dict[str, Any]) -> dict[str, Any]:
    return {
        "version": "1.0",
        "registry": toy_registry,
        "sources": {
            "events": {
                "kind": "toy.source",
                "stream_type": "event_stream",
            }
        },
        "analysis": {
            "stages": [
                {
                    "id": "Scale",
                    "op": "toy.scale",
                    "params": {"factor": 2},
                    "write": [
                        {
                            "kind": "toy.write",
                            "path": "output.json",
                            "when": "final",
                        }
                    ],
                }
            ]
        },
    }


@pytest.fixture
def toy_author_path(tmp_path: Path, toy_author: dict[str, Any]) -> Path:
    path = tmp_path / "author.yaml"
    path.write_text(yaml.safe_dump(toy_author, sort_keys=False), encoding="utf-8")
    return path
