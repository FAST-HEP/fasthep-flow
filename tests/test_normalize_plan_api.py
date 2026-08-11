from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

import hepflow.api as api
from hepflow.api import (
    compile_workflow_file,
    make_plan_file,
    normalise_workflow_file,
    run_plan_file,
    run_workflow_file,
)
from hepflow.compiler.d2_graph import lowered_graph_to_d2
from hepflow.compiler.includes import load_workflow_with_includes
from hepflow.compiler.lower_graph import lower_workflow_to_graph
from hepflow.compiler.normalize import normalize_workflow
from hepflow.compiler.plan import build_plan_from_normalized
from hepflow.utils import read_yaml


def test_public_api_exports_stable_facade_symbols() -> None:
    assert api.__all__ == [
        "InitResult",
        "compile_workflow_file",
        "diff_plan_files",
        "init_project",
        "load_plan_file",
        "load_workflow_yaml",
        "make_plan_file",
        "normalise_workflow_file",
        "normalize_workflow_file",
        "provenance_artifact_text",
        "provenance_graph_text",
        "provenance_summary_text",
        "run_plan_file",
        "run_workflow_file",
    ]
    for name in api.__all__:
        assert hasattr(api, name)


def test_normalize_preserves_generic_toy_source(toy_workflow: dict[str, Any]) -> None:
    normalized = normalize_workflow(toy_workflow)

    assert normalized["sources"]["events"]["kind"] == "toy.source"
    assert normalized["sources"]["events"]["stream_type"] == "event_stream"


def test_explicit_empty_sources_are_preserved_for_product_workflows() -> None:
    normalized = normalize_workflow(
        {
            "version": "1.0",
            "registry": _toy_product_registry(),
            "sources": {},
            "analysis": {
                "stages": [
                    {
                        "id": "Product",
                        "op": "toy.product",
                        "from": [],
                        "params": {"dataset": "sample"},
                    }
                ]
            },
        }
    )

    assert normalized["sources"] == {}


def test_source_less_product_plan_preserves_datasets_and_spec_outputs() -> None:
    workflow = normalize_workflow(
        {
            "version": "1.0",
            "registry": _toy_product_registry(),
            "sources": {},
            "data": {
                "datasets": {
                    "sample": {
                        "files": ["sample.root"],
                        "implementation": "fasthep",
                    }
                }
            },
            "analysis": {
                "stages": [
                    {
                        "id": "Product",
                        "op": "toy.product",
                        "from": [],
                        "params": {"dataset": "sample"},
                    }
                ]
            },
        }
    )

    graph = lower_workflow_to_graph(workflow)
    graph_node = graph.nodes["stage.Product"]["payload"]
    _, plan = build_plan_from_normalized(workflow)

    assert graph_node.outputs == {"product": "toy_product"}
    assert plan.partitions == []
    assert plan.context["datasets"]["sample"]["files"] == ["sample.root"]
    assert plan.context["datasets"]["sample"]["meta"] == {
        "implementation": "fasthep"
    }


def test_transform_stage_accepts_multiple_product_inputs() -> None:
    workflow = normalize_workflow(
        {
            "version": "1.0",
            "registry": _toy_product_registry(),
            "sources": {},
            "analysis": {
                "stages": [
                    {
                        "id": "Reference",
                        "op": "toy.product",
                        "from": [],
                        "params": {"dataset": "reference"},
                    },
                    {
                        "id": "Target",
                        "op": "toy.product",
                        "from": [],
                        "params": {"dataset": "target"},
                    },
                    {
                        "id": "Compare",
                        "op": "toy.product_pair",
                        "from": [
                            {
                                "node": "Reference",
                                "port": "product",
                                "as": "left",
                            },
                            {"node": "Target", "port": "product", "as": "right"},
                        ],
                    },
                ]
            },
        }
    )

    _, plan = build_plan_from_normalized(workflow)
    compare = plan.get_node("stage.Compare")

    assert compare.outputs == {"pair": "toy_pair"}
    assert [ref.input_name for ref in compare.inputs] == ["left", "right"]
    assert [ref.output_name for ref in compare.inputs] == ["product", "product"]


def test_top_level_sinks_errors_with_supported_syntax(
    toy_workflow: dict[str, Any],
) -> None:
    workflow = dict(toy_workflow)
    workflow["sinks"] = [
        {
            "kind": "toy.write",
            "from": "stage.Scale",
            "path": "output.json",
        }
    ]

    with pytest.raises(
        ValueError,
        match=r"Top-level 'sinks' is not supported.*analysis\.stages\[\]\.write",
    ):
        normalize_workflow(workflow)


def test_include_handling_then_normalization(
    tmp_path: Path, toy_registry: dict[str, Any]
) -> None:
    include_path = tmp_path / "registry.yaml"
    include_path.write_text(
        yaml.safe_dump({"registry": toy_registry}, sort_keys=False),
        encoding="utf-8",
    )
    workflow_path = tmp_path / "workflow.yaml"
    workflow_path.write_text(
        yaml.safe_dump(
            {
                "include": ["registry.yaml"],
                "sources": {"events": {"kind": "toy.source"}},
                "analysis": {"stages": []},
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    loaded = load_workflow_with_includes(workflow_path)
    normalized = normalize_workflow(loaded.doc)

    assert "toy.source" in normalized["registry"]["sources"]


def test_include_dataset_mapping_then_normalization(tmp_path: Path) -> None:
    include_path = tmp_path / "datasets.yaml"
    include_path.write_text(
        yaml.safe_dump(
            {
                "datasets": {
                    "DoubleMuon": {
                        "eventtype": "data",
                        "files": ["root://example.test/events.root"],
                    }
                }
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    workflow_path = tmp_path / "workflow.yaml"
    workflow_path.write_text(
        yaml.safe_dump(
            {
                "include": ["datasets.yaml"],
                "data": {"defaults": {"eventtype": "mc", "tree_primary": "Events"}},
                "sources": {"events": {"kind": "root_tree", "tree": "Events"}},
                "analysis": {"stages": []},
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    loaded = load_workflow_with_includes(workflow_path)
    normalized = normalize_workflow(loaded.doc)

    assert normalized["data"]["datasets"] == [
        {
            "name": "DoubleMuon",
            "files": ["root://example.test/events.root"],
            "nevents": None,
            "eventtype": "data",
            "group": "DoubleMuon",
            "meta": {},
        }
    ]


def test_root_tree_source_preserves_reader_options(toy_workflow: dict[str, Any]) -> None:
    workflow = {
        **toy_workflow,
        "sources": {
            "events": {
                "kind": "root_tree",
                "tree": "Events",
                "branches": ["Muon_pt"],
                "start": 10,
                "stop": 20,
            }
        },
    }

    normalized = normalize_workflow(workflow)

    assert normalized["sources"]["events"] == {
        "tree": "Events",
        "stream_type": "event_stream",
        "kind": "root_tree",
        "branches": ["Muon_pt"],
        "start": 10,
        "stop": 20,
    }


def test_lowering_and_plan_creation_write_graph_artifacts(
    tmp_path: Path,
    toy_workflow_path: Path,
) -> None:
    build_dir = tmp_path / "build"
    normalise_workflow_file(toy_workflow_path, outdir=build_dir)
    plan = make_plan_file(build_dir / "compile" / "normalized.yaml", outdir=build_dir)

    assert [node.id for node in plan.nodes] == [
        "read.events",
        "stage.Scale",
        "write.Scale.0",
    ]
    assert plan.get_node("write.Scale.0").params["when"] == "run_end"
    assert (build_dir / "compile" / "plan.yaml").exists()
    assert (build_dir / "graph" / "graph.mmd").exists()
    assert (build_dir / "graph" / "graph.dot").exists()
    assert (build_dir / "graph" / "graph.d2").exists()
    assert (build_dir / "graph" / "graph.json").exists()
    graph_d2 = (build_dir / "graph" / "graph.d2").read_text(encoding="utf-8")
    assert '"read.events"' in graph_d2
    assert "#### events" in graph_d2
    assert "#### Scale" in graph_d2
    assert "Type: toy.source" in graph_d2
    assert "Type: toy.scale" in graph_d2
    assert "class: source" in graph_d2
    assert "class: transform" in graph_d2
    assert "\\n" not in graph_d2
    assert not (build_dir / "plan.yaml").exists()
    assert not (build_dir / "normalized.yaml").exists()
    assert not (build_dir / "graph.mmd").exists()


def test_opt_in_component_defaults_materialize_in_normalized_and_plan(
    tmp_path: Path,
) -> None:
    workflow = {
        "version": "1.0",
        "use": {"profiles": ["tests.toy_components:registry"]},
        "sources": {"events": {"kind": "toy.source"}},
        "analysis": {
            "stages": [
                {
                    "id": "Defaulted",
                    "op": "toy.defaulted",
                    "params": {"required": "configured"},
                }
            ]
        },
    }
    workflow_path = tmp_path / "workflow.yaml"
    workflow_path.write_text(yaml.safe_dump(workflow, sort_keys=False), encoding="utf-8")
    build_dir = tmp_path / "build"

    normalized = normalise_workflow_file(workflow_path, outdir=build_dir)
    plan = make_plan_file(build_dir / "compile" / "normalized.yaml", outdir=build_dir)

    expected_sort = {"by": "pt", "order": "descending"}
    assert normalized["analysis"]["stages"][0]["params"] == {
        "required": "configured",
        "mode": "nominal",
        "sort": expected_sort,
    }
    assert plan.get_node("stage.Defaulted").params == {
        "required": "configured",
        "mode": "nominal",
        "sort": expected_sort,
    }


def test_stage_id_default_materializes_in_normalized_and_plan(
    tmp_path: Path,
) -> None:
    workflow = {
        "version": "1.0",
        "use": {"profiles": ["tests.toy_components:registry"]},
        "sources": {"events": {"kind": "toy.source"}},
        "analysis": {
            "stages": [
                {
                    "id": "FlagFromStageId",
                    "op": "toy.stage_id_output",
                    "params": {},
                },
                {
                    "id": "FlagWithOverride",
                    "op": "toy.stage_id_output",
                    "params": {"output": "explicit_flag"},
                },
            ]
        },
    }
    workflow_path = tmp_path / "workflow.yaml"
    workflow_path.write_text(yaml.safe_dump(workflow, sort_keys=False), encoding="utf-8")
    build_dir = tmp_path / "build"

    normalized = normalise_workflow_file(workflow_path, outdir=build_dir)
    plan = make_plan_file(build_dir / "compile" / "normalized.yaml", outdir=build_dir)

    assert normalized["analysis"]["stages"][0]["params"] == {
        "output": "FlagFromStageId",
    }
    assert normalized["analysis"]["stages"][1]["params"] == {
        "output": "explicit_flag",
    }
    assert plan.get_node("stage.FlagFromStageId").params == {
        "output": "FlagFromStageId",
    }
    assert plan.get_node("stage.FlagWithOverride").params == {
        "output": "explicit_flag",
    }
    assert plan.data_flow["origins"]["FlagFromStageId"] == {
        "kind": "produced",
        "node": "stage.FlagFromStageId",
    }
    assert plan.data_flow["origins"]["explicit_flag"] == {
        "kind": "produced",
        "node": "stage.FlagWithOverride",
    }


def test_param_template_default_materializes_in_normalized_and_plan(
    tmp_path: Path,
) -> None:
    workflow = {
        "version": "1.0",
        "use": {"profiles": ["tests.toy_components:registry"]},
        "sources": {"events": {"kind": "toy.source"}},
        "analysis": {
            "stages": [
                {
                    "id": "DerivedMuon",
                    "op": "toy.template_output",
                    "params": {"source": "Muon"},
                },
                {
                    "id": "DerivedElectron",
                    "op": "toy.template_output",
                    "params": {"source": "Electron", "output": "explicit_Electron"},
                },
            ]
        },
    }
    workflow_path = tmp_path / "workflow.yaml"
    workflow_path.write_text(yaml.safe_dump(workflow, sort_keys=False), encoding="utf-8")
    build_dir = tmp_path / "build"

    normalized = normalise_workflow_file(workflow_path, outdir=build_dir)
    plan = make_plan_file(build_dir / "compile" / "normalized.yaml", outdir=build_dir)

    assert normalized["analysis"]["stages"][0]["params"] == {
        "source": "Muon",
        "output": "derived_Muon",
    }
    assert normalized["analysis"]["stages"][1]["params"] == {
        "source": "Electron",
        "output": "explicit_Electron",
    }
    assert plan.get_node("stage.DerivedMuon").params == {
        "source": "Muon",
        "output": "derived_Muon",
    }
    assert plan.get_node("stage.DerivedElectron").params == {
        "source": "Electron",
        "output": "explicit_Electron",
    }
    assert plan.data_flow["origins"]["derived_Muon"] == {
        "kind": "produced",
        "node": "stage.DerivedMuon",
    }
    assert plan.data_flow["origins"]["explicit_Electron"] == {
        "kind": "produced",
        "node": "stage.DerivedElectron",
    }


def test_compile_graph_d2_uses_readable_observer_labels(
    toy_workflow: dict[str, Any],
) -> None:
    workflow = dict(toy_workflow)
    workflow["observers"] = [
        {
            "kind": "hep.schema_snapshot",
            "at": ["stage.Scale", "write.Scale.0"],
        }
    ]
    graph = lower_workflow_to_graph(normalize_workflow(workflow))

    graph_d2 = lowered_graph_to_d2(graph)

    assert "#### Schema snapshot" in graph_d2
    assert "Type: hep.schema_snapshot" in graph_d2
    assert "Target: Scale" in graph_d2
    assert "observe.hep_schema_snapshot.0.stage_Scale" in graph_d2
    assert "#### observe.hep_schema_snapshot" not in graph_d2
    assert '"stage.Scale" -> "observe.hep_schema_snapshot.0.stage_Scale": "observes stream"' in graph_d2
    assert "class: report" in graph_d2


def test_lower_graph_normalizes_sink_when_alias(toy_workflow: dict[str, Any]) -> None:
    toy_workflow = dict(toy_workflow)
    graph = lower_workflow_to_graph(normalize_workflow(toy_workflow))

    sink = graph.nodes["write.Scale.0"]["payload"]
    assert sink.params["when"] == "run_end"


def test_output_layout_is_normalized_and_resolved_for_writer(
    toy_workflow: dict[str, Any],
) -> None:
    workflow = dict(toy_workflow)
    workflow["outputs"] = {
        "small": {
            "tree": "events",
            "keep": ["Muon_Pt"],
        }
    }
    workflow["analysis"] = {
        "stages": [
            {
                "id": "Scale",
                "op": "toy.scale",
                "params": {"factor": 2},
                "write": {
                    "kind": "toy.write",
                    "path": "small.root",
                    "use": "small",
                },
            }
        ]
    }

    normalized = normalize_workflow(workflow)
    graph = lower_workflow_to_graph(normalized)

    assert normalized["outputs"]["small"] == {
        "tree": "events",
        "keep": ["Muon_Pt"],
    }
    sink = graph.nodes["write.Scale.0"]["payload"]
    assert sink.params == {
        "path": "small.root",
        "tree": "events",
        "keep": ["Muon_Pt"],
        "when": "partition_end",
    }
    assert sink.meta["output_layout"] == "small"


def test_writer_use_rejects_unknown_output_layout(toy_workflow: dict[str, Any]) -> None:
    workflow = dict(toy_workflow)
    workflow["analysis"] = {
        "stages": [
            {
                "id": "Scale",
                "op": "toy.scale",
                "params": {"factor": 2},
                "write": {
                    "kind": "toy.write",
                    "path": "small.root",
                    "use": "missing",
                },
            }
        ]
    }

    with pytest.raises(ValueError, match="unknown output layout 'missing'"):
        lower_workflow_to_graph(normalize_workflow(workflow))


def test_public_api_compile_and_run_roundtrip(
    toy_workflow_path: Path, tmp_path: Path
) -> None:
    build_dir = tmp_path / "api-build"

    plan = compile_workflow_file(toy_workflow_path, outdir=build_dir)
    assert plan.get_node("stage.Scale").impl == "toy.scale"

    result = run_plan_file(build_dir / "compile" / "plan.yaml", outdir=build_dir)
    assert result.success is True
    assert (build_dir / "run_summary.yaml").exists()
    assert (build_dir / "artifacts" / "files" / "output.json").exists()

    one_shot_dir = tmp_path / "one-shot"
    result = run_workflow_file(toy_workflow_path, outdir=one_shot_dir)
    assert result.success is True
    assert (one_shot_dir / "compile" / "normalized.yaml").exists()
    assert (one_shot_dir / "compile" / "plan.yaml").exists()
    assert (one_shot_dir / "run_summary.yaml").exists()


def test_public_api_accepts_str_and_path_inputs_and_serializes_paths_as_strings(
    toy_workflow_path: Path,
    tmp_path: Path,
) -> None:
    build_dir = tmp_path / "mixed-paths"

    normalized = normalise_workflow_file(str(toy_workflow_path), outdir=build_dir)
    assert_no_path_objects(normalized)

    plan = make_plan_file(
        str(build_dir / "compile" / "normalized.yaml"),
        outdir=str(build_dir),
    )
    assert_no_path_objects(plan.to_dict())

    result = run_plan_file(build_dir / "compile" / "plan.yaml", outdir=str(build_dir))
    assert result.success is True

    for path in [
        build_dir / "compile" / "normalized.yaml",
        build_dir / "compile" / "plan.yaml",
        build_dir / "run_summary.yaml",
    ]:
        payload = read_yaml(path)
        assert_no_path_objects(payload)

    plan_yaml = read_yaml(build_dir / "compile" / "plan.yaml")
    sink_node = next(node for node in plan_yaml["nodes"] if node["role"] == "sink")
    assert isinstance(sink_node["params"]["path"], str)


def assert_no_path_objects(value: object) -> None:
    if isinstance(value, Path):
        raise AssertionError(f"Found Path object in serialized payload: {value!r}")
    if isinstance(value, dict):
        for item in value.values():
            assert_no_path_objects(item)
        return
    if isinstance(value, list | tuple):
        for item in value:
            assert_no_path_objects(item)


def _toy_product_registry() -> dict[str, Any]:
    return {
        "transforms": {
            "toy.product": {
                "spec": "tests.toy_components.transforms:TOY_PRODUCT_SPEC",
                "impl": "tests.toy_components.transforms:run_toy_product",
            },
            "toy.product_pair": {
                "spec": "tests.toy_components.transforms:TOY_PRODUCT_PAIR_SPEC",
                "impl": "tests.toy_components.transforms:run_toy_product_pair",
            },
        },
        "product_handlers": {
            "toy_product": {
                "merge": "tests.toy_components.products:merge_toy_products",
                "materialize": "tests.toy_components.products:materialize_toy_product",
            }
        },
    }
