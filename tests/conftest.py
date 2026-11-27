from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from hepflow.compiler.hooks import software as software_module
from hepflow.utils import write_yaml
from tests.builders import modified

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
    """tests/fixtures/workflows/toy_workflow.yaml with the toy registry; a fresh copy per test."""
    path = Path(__file__).parent / "fixtures" / "workflows" / "toy_workflow.yaml"
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    return {"version": document.pop("version"), "registry": toy_registry, **document}


@pytest.fixture
def write_workflow(tmp_path: Path, toy_workflow: dict[str, Any]) -> Any:
    """``write_workflow(workflow=None, *, filename="workflow.yaml", **changes) -> Path``.

    Writes ``workflow`` (default: the toy workflow) below ``tmp_path`` with ``changes``
    applied (see ``tests.builders.modified``).
    """

    def write(
        workflow: dict[str, Any] | None = None,
        *,
        filename: str = "workflow.yaml",
        **changes: Any,
    ) -> Path:
        base = toy_workflow if workflow is None else workflow
        path = tmp_path / filename
        write_yaml(modified(base, **changes), path)
        return path

    return write


@pytest.fixture
def toy_workflow_path(tmp_path: Path, toy_workflow: dict[str, Any]) -> Path:
    path = tmp_path / "workflow.yaml"
    path.write_text(yaml.safe_dump(toy_workflow, sort_keys=False), encoding="utf-8")
    return path
