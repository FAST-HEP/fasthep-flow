from __future__ import annotations

from typing import Any, Literal

import pytest
import yaml

from hepflow.model.io import OutputResult
from hepflow.model.plan import (
    ExecutionNode,
    ExecutionPartition,
    ExecutionPlan,
    PlanInputRef,
)
from hepflow.model.products import ProductBoundaryPolicy, ProductHandlerEntry
from hepflow.registry.loaders import (
    resolve_runtime_registry,
    runtime_registry_from_config,
)
from hepflow.registry.runtime import RuntimeRegistry
from hepflow.runtime.boundary import (
    extract_boundary_products,
    format_partition_boundary,
    plan_partition_boundary,
)


def test_product_handler_boundary_policy_defaults_are_backward_compatible() -> None:
    registry = runtime_registry_from_config(
        {"product_handlers": {"event_stream": {"merge": "builtins:list"}}}
    )

    assert registry.product_handlers["event_stream"].boundary == ProductBoundaryPolicy()


@pytest.mark.parametrize(
    ("representation", "retain"),
    [
        ("value", True),
        ("reference", True),
        ("materialize", False),
    ],
)
def test_product_handler_boundary_policy_loads_explicit_modes(
    representation: str,
    retain: bool,
) -> None:
    registry = runtime_registry_from_config(
        {
            "product_handlers": {
                "thing": {
                    "boundary": {
                        "retain": retain,
                        "representation": representation,
                    }
                }
            }
        }
    )

    policy = registry.product_handlers["thing"].boundary
    assert policy.retain is retain
    assert policy.representation == representation


def test_product_handler_boundary_policy_rejects_invalid_representation() -> None:
    with pytest.raises(ValueError, match="representation must be one of"):
        runtime_registry_from_config(
            {
                "product_handlers": {
                    "thing": {
                        "boundary": {
                            "retain": True,
                            "representation": "large-memory-object",
                        }
                    }
                }
            }
        )


def test_product_handler_boundary_policy_rejects_invalid_retain_type() -> None:
    with pytest.raises(TypeError, match="retain must be boolean"):
        runtime_registry_from_config(
            {
                "product_handlers": {
                    "thing": {
                        "boundary": {
                            "retain": "yes",
                            "representation": "value",
                        }
                    }
                }
            }
        )


def test_generic_artifact_policy_is_available_from_resolved_registry() -> None:
    registry = resolve_runtime_registry({})

    assert registry.product_handlers["artifact"].boundary == ProductBoundaryPolicy(
        retain=True,
        representation="reference",
    )


def test_histogram_boundary_plan_retains_histograms_not_intermediate_streams() -> None:
    plan = _histogram_plan()
    registry = _boundary_registry()

    boundary = plan_partition_boundary(plan, runtime_registry=registry)

    assert [(spec.node_id, spec.output_name, spec.kind) for spec in boundary] == [
        ("stage.RecoilHist", "hist", "histogram"),
        ("stage.HTHist", "hist", "histogram"),
        ("stage.MHTHist", "hist", "histogram"),
        ("stage.nJetHist", "hist", "histogram"),
        ("stage.nBJetHist", "hist", "histogram"),
    ]
    assert all("retained_product" in spec.reasons for spec in boundary)
    assert "read.events.stream" not in format_partition_boundary(boundary)
    assert "stage.MainSelection.stream" not in format_partition_boundary(boundary)


def test_wider_scope_consumer_retains_transient_event_stream() -> None:
    plan = ExecutionPlan()
    _add_node(
        plan,
        "stage.Select",
        outputs={"stream": "event_stream"},
    )
    _add_node(
        plan,
        "sink.DatasetSummary",
        role="sink",
        inputs=[("stage.Select", "stream", "target")],
        input_scope="dataset",
        outputs={"artifact": "artifact"},
        params={"when": "dataset_end"},
    )

    boundary = plan_partition_boundary(plan, runtime_registry=_boundary_registry())

    stream_spec = next(spec for spec in boundary if spec.output_name == "stream")
    assert stream_spec.node_id == "stage.Select"
    assert stream_spec.kind == "event_stream"
    assert stream_spec.policy == ProductBoundaryPolicy()
    assert stream_spec.reasons == ("dataset_consumer:sink.DatasetSummary",)


def test_artifact_reference_boundary_product_wraps_output_result_identity() -> None:
    plan = ExecutionPlan()
    _add_node(
        plan,
        "write.skim",
        role="sink",
        outputs={"artifact": "artifact"},
    )
    registry = resolve_runtime_registry({})
    boundary = plan_partition_boundary(plan, runtime_registry=registry)
    partition = _partition()
    output = OutputResult(kind="root_tree", path="artifacts/files/part001.root")
    value_store: dict[tuple[str, str], Any] = {
        ("write.skim", "artifact"): output,
        ("stage.Select", "stream"): {"pt": [1]},
    }

    products = extract_boundary_products(
        plan,
        value_store,
        partition=partition,
        boundary=boundary,
        runtime_registry=registry,
    )

    assert len(products) == 1
    product = products[0]
    assert product.node_id == "write.skim"
    assert product.output_name == "artifact"
    assert product.kind == "artifact"
    assert product.dataset == "toy"
    assert product.partition_id == "events__toy__0"
    assert product.representation == "reference"
    assert product.value is output
    assert value_store[("write.skim", "artifact")] is output


def test_extract_boundary_products_wraps_only_selected_values() -> None:
    plan = _histogram_plan()
    registry = _boundary_registry()
    boundary = plan_partition_boundary(plan, runtime_registry=registry)
    value_store = {
        ("stage.MainSelection", "stream"): {"pt": [1]},
        ("stage.RecoilHist", "hist"): "recoil",
        ("stage.HTHist", "hist"): "ht",
    }

    products = extract_boundary_products(
        plan,
        value_store,
        partition=_partition(),
        boundary=boundary,
        runtime_registry=registry,
    )

    assert [(product.node_id, product.value) for product in products] == [
        ("stage.RecoilHist", "recoil"),
        ("stage.HTHist", "ht"),
    ]
    assert ("stage.MainSelection", "stream") in value_store


def test_extract_boundary_products_rejects_unimplemented_materialize_mode() -> None:
    plan = ExecutionPlan()
    _add_node(plan, "stage.Large", outputs={"large": "large_product"})
    registry = RuntimeRegistry(
        product_handlers={
            "large_product": ProductHandlerEntry(
                boundary=ProductBoundaryPolicy(
                    retain=True,
                    representation="materialize",
                )
            )
        }
    )
    boundary = plan_partition_boundary(plan, runtime_registry=registry)

    with pytest.raises(NotImplementedError, match="Boundary materialization"):
        extract_boundary_products(
            plan,
            {("stage.Large", "large"): object()},
            partition=_partition(),
            boundary=boundary,
            runtime_registry=registry,
        )


def test_histogram_execution_policy_is_partition_scoped(
    toy_workflow: dict[str, Any],
    tmp_path,
) -> None:
    workflow = {
        **toy_workflow,
        "analysis": {
            "stages": [
                {
                    "id": "RecoilHist",
                    "op": "hep.hist",
                    "params": {"name": "recoil"},
                }
            ]
        },
    }
    workflow_path = tmp_path / "workflow.yaml"
    workflow_path.write_text(yaml.safe_dump(workflow, sort_keys=False), encoding="utf-8")

    from hepflow.api import compile_workflow_file  # noqa: PLC0415

    plan = compile_workflow_file(workflow_path, outdir=tmp_path / "build", chunk_size=2)
    hist_node = plan.get_node("stage.RecoilHist")

    assert hist_node.outputs == {"hist": "histogram"}
    assert hist_node.input_scope == "partition"
    assert hist_node.output_scope == "partition"
    assert hist_node.partitioning.mode == "dataset_chunks"


def _histogram_plan() -> ExecutionPlan:
    plan = ExecutionPlan()
    _add_node(plan, "read.events", role="source", outputs={"stream": "event_stream"})
    previous = "read.events"
    for node_id in [
        "stage.ProjectEvents",
        "stage.DefineWeights",
        "stage.DefineBJets",
        "stage.MainSelection",
    ]:
        _add_node(
            plan,
            node_id,
            inputs=[(previous, "stream", "stream")],
            outputs={"stream": "event_stream"},
        )
        previous = node_id
    for hist_id in [
        "stage.RecoilHist",
        "stage.HTHist",
        "stage.MHTHist",
        "stage.nJetHist",
        "stage.nBJetHist",
    ]:
        _add_node(
            plan,
            hist_id,
            inputs=[(previous, "stream", "stream")],
            outputs={"hist": "histogram"},
        )
    return plan


def _add_node(
    plan: ExecutionPlan,
    node_id: str,
    *,
    role: Literal["source", "transform", "observer", "sink"] = "transform",
    inputs: list[tuple[str, str, str]] | None = None,
    outputs: dict[str, str],
    input_scope: Literal["partition", "dataset", "global"] = "partition",
    output_scope: Literal["partition", "dataset", "global"] = "partition",
    params: dict[str, Any] | None = None,
) -> None:
    plan.add_node(
        ExecutionNode(
            id=node_id,
            graph_node_id=node_id,
            role=role,
            impl=node_id,
            inputs=[
                PlanInputRef(
                    node_id=upstream,
                    output_name=output,
                    input_name=input_name,
                )
                for upstream, output, input_name in inputs or []
            ],
            outputs=outputs,
            input_scope=input_scope,
            output_scope=output_scope,
            params=dict(params or {}),
        )
    )


def _boundary_registry() -> RuntimeRegistry:
    return RuntimeRegistry(
        product_handlers={
            "event_stream": ProductHandlerEntry(),
            "histogram": ProductHandlerEntry(
                boundary=ProductBoundaryPolicy(retain=True, representation="value")
            ),
        }
    )


def _partition() -> ExecutionPartition:
    return ExecutionPartition(
        id="events__toy__0",
        dataset="toy",
        file="toy://events",
        source="events",
        part="0_0",
        start=0,
        stop=1,
    )
