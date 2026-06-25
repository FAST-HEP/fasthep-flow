from __future__ import annotations

from hepflow.model import (
    ComponentSpec,
    ExecutionHook,
    FlowIssue,
    IssueLevel,
    OperationResult,
    ProductRef,
)
from hepflow.model.component_spec import RuntimeComponentSpec


def test_component_spec_public_alias_preserves_runtime_spec_path() -> None:
    assert ComponentSpec is RuntimeComponentSpec


def test_component_author_import_surface_is_available() -> None:
    assert ComponentSpec.from_obj(
        {
            "name": "example.component",
            "kind": "transform",
            "requires": {
                "symbols": [
                    {"from": "params.keep", "kind": "field_list"},
                ],
            },
            "provides": {
                "symbols": [
                    {"from": "params.outputs", "kind": "field_list"},
                ],
            },
        }
    ).name == "example.component"
    assert ExecutionHook is not None
    assert FlowIssue is not None
    assert IssueLevel.ERROR.name == "ERROR"
    assert OperationResult is not None
    assert ProductRef is not None
