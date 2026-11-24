"""Workflows without sources: valid while no stage needs an event stream, else a named error."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from hepflow.api import run_workflow_file
from hepflow.compiler.lower_graph import lower_workflow_to_graph
from hepflow.compiler.normalize import normalize_workflow
from hepflow.compiler.spec_resolver import SpecResolver

HERE = "tests.compiler.test_sourceless_workflows"
WRITE = {"kind": "toy.write", "path": "o.json", "when": "final"}
PRODUCT = {"id": "Product", "op": "toy.product", "params": {"dataset": "s"}}
SCALE = {"id": "Scale", "op": "toy.scale", "params": {"factor": 2}}


def _registry() -> dict[str, Any]:
    return {
        "transforms": {
            "toy.product": {
                "spec": "tests.toy_components.transforms:TOY_PRODUCT_SPEC",
                "impl": "tests.toy_components.transforms:run_toy_product",
            },
            "toy.scale": {
                "spec": "tests.toy_components.transforms:TOY_SCALE_SPEC",
                "impl": "tests.toy_components.transforms:run_toy_scale",
            },
        },
        "sinks": {
            "toy.write": {
                "spec": "tests.toy_components.sinks:TOY_WRITE_SPEC",
                "impl": "tests.toy_components.sinks:run_toy_write",
            }
        },
        "observers": {
            "toy.observe": {
                "spec": "tests.toy_components.observers:TOY_OBSERVER_SPEC",
                "impl": "tests.toy_components.observers:run_toy_observer",
            }
        },
        "product_handlers": {
            "toy_product": {
                "merge": "tests.toy_components.products:merge_toy_products",
                "materialize": "tests.toy_components.products:materialize_toy_product",
            }
        },
    }


def _workflow(stages: list[dict[str, Any]], **extra: Any) -> dict[str, Any]:
    return {
        "version": "1.0",
        "registry": _registry(),
        "analysis": {"stages": stages},
        **extra,
    }


def _graph(workflow: dict[str, Any]) -> Any:
    return lower_workflow_to_graph(normalize_workflow(workflow))


def _sources(graph: Any) -> list[str]:
    return [n for n, d in graph.nodes(data=True) if d["payload"].role == "source"]


def test_empty_sources_with_a_product_stage_lowers_without_a_source() -> None:
    graph = _graph(_workflow([{**PRODUCT, "from": []}], sources={}))

    assert _sources(graph) == []
    assert list(graph.nodes) == ["stage.Product"]


def test_absent_sources_drop_the_implicit_default_when_no_stage_needs_a_stream() -> None:
    workflow = _workflow([PRODUCT])
    normalized = normalize_workflow(workflow)

    assert normalized["sources"]["events"]["implicit"] is True
    assert _sources(lower_workflow_to_graph(normalized)) == []


def test_absent_sources_keep_the_default_for_a_workflow_that_reads_events() -> None:
    graph = _graph(_workflow([SCALE]))

    assert _sources(graph) == ["read.events"]
    assert "implicit" not in graph.nodes["read.events"]["payload"].params
    assert graph.has_edge("read.events", "stage.Scale")


def test_an_explicit_source_is_never_dropped() -> None:
    sources = {"events": {"kind": "toy.source", "stream_type": "event_stream"}}

    assert _sources(_graph(_workflow([PRODUCT], sources=sources))) == ["read.events"]


def test_a_stage_that_names_the_default_source_keeps_it() -> None:
    stage = {**PRODUCT, "from": "events"}

    assert _sources(_graph(_workflow([stage]))) == ["read.events"]


@pytest.mark.parametrize(
    "stage",
    [
        SCALE,
        {**SCALE, "from": []},
    ],
    ids=["implicit-input", "empty-from"],
)
def test_a_stage_that_needs_a_stream_without_a_source_is_an_error_naming_it(
    stage: dict[str, Any],
) -> None:
    with pytest.raises(ValueError, match=r"Stage 'Scale' \(op 'toy\.scale'\) needs an event stream") as raised:
        _graph(_workflow([stage], sources={}))

    assert "sources:" in str(raised.value)


def test_the_first_stage_that_needs_a_stream_is_named() -> None:
    stages = [{**PRODUCT, "from": []}, SCALE, {**SCALE, "id": "Later"}]

    with pytest.raises(ValueError, match="Stage 'Scale' "):
        _graph(_workflow(stages, sources={}))


def test_a_stream_stage_after_a_stream_stage_is_unchanged() -> None:
    graph = _graph(_workflow([SCALE, {**SCALE, "id": "Second"}]))

    assert graph.has_edge("stage.Scale", "stage.Second")


def test_a_source_less_workflow_runs_with_an_observer_and_a_writer(tmp_path: Path) -> None:
    workflow = _workflow(
        [{**PRODUCT, "write": [WRITE]}],
        observers=[{"kind": "toy.observe", "at": ["stage.Product"]}],
    )
    path = tmp_path / "workflow.yaml"
    path.write_text(yaml.safe_dump(workflow, sort_keys=False), encoding="utf-8")

    run_workflow_file(path, outdir=tmp_path / "out")

    assert (tmp_path / "out" / "artifacts" / "files" / "o.json").exists()


@pytest.mark.parametrize("sources", [None, {}])
def test_a_source_less_workflow_runs_to_completion(tmp_path: Path, sources: Any) -> None:
    extra = {} if sources is None else {"sources": sources}
    workflow = _workflow([{**PRODUCT, "write": [WRITE]}], **extra)
    path = tmp_path / "workflow.yaml"
    path.write_text(yaml.safe_dump(workflow, sort_keys=False), encoding="utf-8")

    run_workflow_file(path, outdir=tmp_path / "out")

    assert (tmp_path / "out" / "artifacts" / "files" / "o.json").exists()


NO_INPUT = {"name": "no_input", "input": None, "result": {"p": "toy_product"}, "execution": {"scope": "global"}}
STREAM_INPUT = {"name": "stream_in", "input": {"name": "stream", "kind": "event_stream", "required": True}}
NO_INPUT_KEY = {"name": "no_key"}


def test_a_transform_reads_a_stream_unless_its_spec_says_input_none() -> None:
    resolver = SpecResolver(
        transforms={
            "a": {"spec": f"{HERE}:NO_INPUT"},
            "b": {"spec": f"{HERE}:STREAM_INPUT"},
            "c": {"spec": f"{HERE}:NO_INPUT_KEY"},
        }
    )

    assert resolver.stage_needs_stream("a") is False
    assert resolver.stage_needs_stream("b") is True
    assert resolver.stage_needs_stream("c") is True
    assert resolver.stage_needs_stream("unregistered") is True
