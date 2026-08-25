from __future__ import annotations

import gc
import json
import weakref
from typing import Any, Literal

import pytest
import yaml

from hepflow.api import compile_workflow_file, run_plan_file
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
    PartitionBoundaryResult,
    boundary_products_to_value_store,
    extract_boundary_products,
    format_partition_boundary,
    plan_partition_boundary,
)
from hepflow.runtime.engine import execute_plan_locally


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


def test_boundary_products_to_value_store_contains_only_boundary_values() -> None:
    plan = _histogram_plan()
    registry = _boundary_registry()
    boundary = plan_partition_boundary(plan, runtime_registry=registry)
    value_store = {
        ("read.events", "stream"): {"pt": [1]},
        ("stage.ProjectEvents", "stream"): {"pt": [1]},
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
    compact_store = boundary_products_to_value_store(products)

    assert compact_store == {
        ("stage.RecoilHist", "hist"): "recoil",
        ("stage.HTHist", "hist"): "ht",
    }


def test_transient_partition_values_are_collectible_after_boundary_extraction() -> None:
    plan = _histogram_plan()
    registry = _boundary_registry()
    boundary = plan_partition_boundary(plan, runtime_registry=registry)
    transient = _Sentinel("stream")
    retained = _Sentinel("hist")
    transient_ref = weakref.ref(transient)
    retained_ref = weakref.ref(retained)
    value_store = {
        ("stage.MainSelection", "stream"): transient,
        ("stage.RecoilHist", "hist"): retained,
    }

    products = extract_boundary_products(
        plan,
        value_store,
        partition=_partition(),
        boundary=boundary,
        runtime_registry=registry,
    )
    del value_store
    del transient
    gc.collect()

    assert transient_ref() is None
    assert retained_ref() is retained
    assert products[0].value is retained


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


def test_local_histogram_only_execution_prunes_event_streams_before_merge(
    toy_workflow: dict[str, Any],
    tmp_path,
) -> None:
    workflow = _histogram_workflow(
        toy_workflow,
        product_handlers={
            "event_stream": {
                "boundary": {"retain": False, "representation": "value"},
                "merge": "tests.toy_components.transforms:fail_toy_event_stream_merge",
            },
            "histogram": {
                "boundary": {"retain": True, "representation": "value"},
                "merge": "tests.toy_components.transforms:merge_toy_histograms",
                "materialize": "tests.toy_components.transforms:materialize_toy_histogram",
            },
        },
    )
    workflow_path = tmp_path / "workflow.yaml"
    workflow_path.write_text(yaml.safe_dump(workflow, sort_keys=False), encoding="utf-8")
    build_dir = tmp_path / "build"
    compile_workflow_file(workflow_path, outdir=build_dir, chunk_size=1)

    result = run_plan_file(build_dir / "compile" / "plan.yaml", outdir=build_dir)

    assert result.success is True
    outputs = result.outputs["value_store"]
    assert isinstance(outputs, list)
    assert all(isinstance(item, PartitionBoundaryResult) for item in outputs)
    assert [
        [(product.node_id, product.output_name) for product in item.products]
        for item in outputs
    ] == [
        [("stage.RecoilHist", "hist")],
        [("stage.RecoilHist", "hist")],
        [("stage.RecoilHist", "hist")],
        [("stage.RecoilHist", "hist")],
    ]
    histogram_path = build_dir / "artifacts" / "histograms" / "stage.RecoilHist.json"
    assert json.loads(histogram_path.read_text(encoding="utf-8")) == {"entries": 4}


def test_local_execution_retains_wider_scope_event_stream_and_invokes_merge(
    toy_workflow: dict[str, Any],
    tmp_path,
) -> None:
    workflow = {
        **_partitioned_workflow(toy_workflow),
        "registry": {
            **toy_workflow["registry"],
            "product_handlers": {
                "event_stream": {
                    "boundary": {"retain": False, "representation": "value"},
                    "merge": "tests.toy_components.transforms:merge_toy_event_streams",
                }
            },
        },
    }
    workflow["analysis"]["stages"][0]["write"] = [
        {"kind": "toy.write", "path": "dataset.json", "when": "dataset"},
    ]
    workflow_path = tmp_path / "workflow.yaml"
    workflow_path.write_text(yaml.safe_dump(workflow, sort_keys=False), encoding="utf-8")
    build_dir = tmp_path / "build"
    compile_workflow_file(workflow_path, outdir=build_dir, chunk_size=1)

    result = run_plan_file(build_dir / "compile" / "plan.yaml", outdir=build_dir)

    assert result.success is True
    outputs = result.outputs["value_store"]
    assert isinstance(outputs, list)
    assert all(
        ("stage.Scale", "stream") in item.value_store()
        for item in outputs
        if isinstance(item, PartitionBoundaryResult)
    )
    payload = json.loads(
        (build_dir / "artifacts" / "files" / "dataset.json").read_text(
            encoding="utf-8"
        )
    )
    assert payload["scaled_pt"] == [24, 36, 42, 56]


def test_partition_writer_manifest_uses_boundary_artifact_references(
    tmp_path,
) -> None:
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
    output = OutputResult(
        kind="root_tree",
        path="artifacts/files/skim/toy/0_0.root",
        metadata={
                "writer_manifest": {
                    "kind": "root_tree",
                    "name": "skim",
                    "node_id": "write.skim",
                    "input_node": "stage.Select",
                    "tree": "Events",
                    "path": "artifacts/files/skim/toy/0_0.root",
                    "path_type": "relative_to_outdir",
                    "dataset": "toy",
                "partition": 0,
                "attempt": 0,
                "entries": 1,
                "size_bytes": 12,
                "format": "root",
            }
        },
    )
    value_store = {
        ("write.skim", "artifact"): output,
        ("stage.Select", "stream"): _Sentinel("stream"),
    }
    products = extract_boundary_products(
        plan,
        value_store,
        partition=partition,
        boundary=boundary,
        runtime_registry=registry,
    )
    compact_store = boundary_products_to_value_store(products)

    from hepflow.runtime.writer_manifests import write_writer_manifests  # noqa: PLC0415

    write_writer_manifests(
        plan,
        stores=[compact_store],
        partitions=[partition],
        outdir=tmp_path,
    )

    assert compact_store == {("write.skim", "artifact"): output}
    manifest = json.loads(
        (tmp_path / "artifacts" / "files" / "skim" / "manifest.json").read_text(
            encoding="utf-8"
        )
    )
    assert manifest["datasets"]["toy"]["files"][0]["path"] == (
        "artifacts/files/skim/toy/0_0.root"
    )


def test_execute_plan_locally_returns_partition_boundary_results(
    toy_workflow: dict[str, Any],
    tmp_path,
) -> None:
    workflow = _histogram_workflow(toy_workflow)
    workflow_path = tmp_path / "workflow.yaml"
    workflow_path.write_text(yaml.safe_dump(workflow, sort_keys=False), encoding="utf-8")
    plan = compile_workflow_file(workflow_path, outdir=tmp_path / "build", chunk_size=2)

    results = execute_plan_locally(
        plan,
        registry_cfg=plan.registry,
        ctx={"outdir": str(tmp_path / "build")},
        partitions=plan.partitions,
    )

    assert all(isinstance(item, PartitionBoundaryResult) for item in results)
    assert [
        list(item.value_store())
        for item in results
        if isinstance(item, PartitionBoundaryResult)
    ] == [[("stage.RecoilHist", "hist")], [("stage.RecoilHist", "hist")]]


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


def _partitioned_workflow(toy_workflow: dict[str, Any]) -> dict[str, Any]:
    return {
        **toy_workflow,
        "data": {
            "datasets": [
                {
                    "name": "toydata",
                    "files": ["toy://events"],
                    "nevents": 4,
                }
            ]
        },
    }


def _histogram_workflow(
    toy_workflow: dict[str, Any],
    *,
    product_handlers: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        **_partitioned_workflow(toy_workflow),
        "registry": {
            **toy_workflow["registry"],
            "product_handlers": product_handlers
            or {
                "histogram": {
                    "boundary": {"retain": True, "representation": "value"},
                    "merge": "tests.toy_components.transforms:merge_toy_histograms",
                }
            },
        },
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


class _Sentinel:
    def __init__(self, name: str) -> None:
        self.name = name
