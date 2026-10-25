from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from hepflow.compiler.hooks import software as software_module

# A test module that sets ``REAL_SOFTWARE_CAPTURE = True`` runs the real distribution scan
# and git capture of the flow.software hook; every other test gets a cheap stub so that
# compiles stay fast. The per-process cache is cleared around each test either way.
_STUB_GIT_NOTE = "git capture stubbed in tests"


def _stub_scan(root: Path | None, *, git: bool) -> dict[str, Any]:
    state = {"root": str(root) if root else None, "revision": None, "dirty": None}
    return {
        "distributions": [],
        "direct_urls": [],
        "editable_paths": {},
        "git": {"root": {**state, "note": _STUB_GIT_NOTE}, "editable": {}}
        if git
        else None,
    }


@pytest.fixture(autouse=True)
def _software_capture(
    request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch
) -> Any:
    software_module.clear_software_cache()
    if not getattr(request.module, "REAL_SOFTWARE_CAPTURE", False):
        monkeypatch.setattr(software_module, "_scan_environment", _stub_scan)
    yield
    software_module.clear_software_cache()


@pytest.fixture
def toy_registry() -> dict[str, Any]:
    path = Path(__file__).parent / "fixtures" / "toy_registry.yaml"
    return yaml.safe_load(path.read_text(encoding="utf-8"))["registry"]


@pytest.fixture
def toy_workflow(toy_registry: dict[str, Any]) -> dict[str, Any]:
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
def toy_workflow_path(tmp_path: Path, toy_workflow: dict[str, Any]) -> Path:
    path = tmp_path / "workflow.yaml"
    path.write_text(yaml.safe_dump(toy_workflow, sort_keys=False), encoding="utf-8")
    return path
