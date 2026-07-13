from __future__ import annotations

from hepflow.model import (
    ComponentSpec,
    ExecutionHook,
    FlowIssue,
    IssueLevel,
    OperationResult,
    ProductRef,
    ResolvedResource,
)
from hepflow.model.component_spec import RuntimeComponentSpec
from hepflow.runtime import ComponentContext


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


def test_component_runtime_public_surface_is_available() -> None:
    resource = ResolvedResource(
        id="cms.pileup.2024",
        kind="correctionlib",
        value=object(),
        path="/cvmfs/example/puWeights.json.gz",
    )
    ctx = ComponentContext(
        {
            "resources": {"cms.pileup.2024": resource},
            "dataset": {"name": "dy", "eventtype": "mc"},
            "node_id": "stage.PileupWeights",
            "partition": {"id": "events__dy__0"},
        }
    )

    assert ctx.resources["cms.pileup.2024"] is resource
    assert ctx.dataset["eventtype"] == "mc"
    assert ctx.node_id == "stage.PileupWeights"
    assert ctx.partition_id == "events__dy__0"
    assert hasattr(ctx.provenance, "record_operation")
