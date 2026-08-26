from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest
import yaml

from hepflow.api import compile_workflow_file
from hepflow.compiler.normalize import normalize_workflow
from hepflow.compiler.plan import build_plan_from_normalized


def test_duplicate_attached_render_out_fails(toy_registry: dict[str, Any]) -> None:
    workflow = _workflow_with_hist_renders(
        toy_registry,
        [
            {"style": "hist_plot", "out": "RecoilDebug.png"},
            {"style": "hist_plot", "out": "RecoilDebug.png"},
        ],
    )

    with pytest.raises(ValueError, match=r"RecoilDebug\.png") as exc:
        build_plan_from_normalized(normalize_workflow(workflow))

    message = str(exc.value)
    assert "render.Hist.0" in message
    assert "render.Hist.1" in message
    assert "out: RecoilDebug.png" in message


def test_different_render_paths_compile(toy_registry: dict[str, Any]) -> None:
    workflow = _workflow_with_hist_renders(
        toy_registry,
        [
            {"style": "hist_plot", "out": "recoil_pt.png"},
            {"style": "hist_plot", "out": "recoil_phi.png"},
        ],
    )

    _graph, plan = build_plan_from_normalized(normalize_workflow(workflow))

    assert {"render.Hist.0", "render.Hist.1"} <= set(plan.node_index)


def test_normalized_equivalent_render_paths_fail(
    toy_registry: dict[str, Any],
) -> None:
    workflow = _workflow_with_hist_renders(
        toy_registry,
        [
            {"style": "hist_plot", "out": "plots/a.png"},
            {"style": "hist_plot", "out": "plots/../plots/a.png"},
        ],
    )

    with pytest.raises(ValueError, match=r"artifacts/plots/plots/a\.png") as exc:
        build_plan_from_normalized(normalize_workflow(workflow))

    assert "render.Hist.0" in str(exc.value)
    assert "render.Hist.1" in str(exc.value)


def test_render_expansion_output_collision_fails(
    toy_registry: dict[str, Any],
) -> None:
    workflow = _workflow_with_top_level_render_variations(toy_registry)

    with pytest.raises(ValueError, match=r"RecoilDebug\.png") as exc:
        build_plan_from_normalized(normalize_workflow(workflow))

    message = str(exc.value)
    assert "render.RecoilRender_nominal.0" in message
    assert "render.RecoilRender_up.0" in message


def test_static_sink_path_collision_fails(toy_registry: dict[str, Any]) -> None:
    workflow = _workflow_with_static_writes(
        toy_registry,
        [
            {"kind": "toy.write", "path": "events.json", "when": "final"},
            {"kind": "toy.write", "path": "events.json", "when": "final"},
        ],
    )

    with pytest.raises(ValueError, match=r"artifacts/files/events\.json") as exc:
        build_plan_from_normalized(normalize_workflow(workflow))

    assert "write.Scale.0" in str(exc.value)
    assert "write.Scale.1" in str(exc.value)


def test_partition_writer_generated_part_paths_remain_valid(
    toy_registry: dict[str, Any],
) -> None:
    workflow = _workflow_with_static_writes(
        toy_registry,
        [
            {"kind": "toy.write", "path": "events.json", "when": "partition"},
            {"kind": "toy.write", "path": "other.json", "when": "partition"},
        ],
    )

    _graph, plan = build_plan_from_normalized(
        normalize_workflow(workflow), chunk_size=5
    )

    assert {"write.Scale.0", "write.Scale.1"} <= set(plan.node_index)
    assert len(plan.partitions) == 2


def test_partition_writer_manifest_identity_collision_fails(
    toy_registry: dict[str, Any],
) -> None:
    workflow = _workflow_with_static_writes(
        toy_registry,
        [
            {"kind": "toy.write", "path": "one/events.json", "when": "partition"},
            {"kind": "toy.write", "path": "two/events.json", "when": "partition"},
        ],
    )

    with pytest.raises(
        ValueError, match=r"artifacts/files/events/manifest\.json"
    ) as exc:
        build_plan_from_normalized(normalize_workflow(workflow), chunk_size=5)

    assert "write.Scale.0" in str(exc.value)
    assert "write.Scale.1" in str(exc.value)


def test_reserved_run_artifact_collision_fails(toy_registry: dict[str, Any]) -> None:
    workflow = _workflow_with_hist_renders(
        toy_registry,
        [
            {
                "style": "hist_plot",
                "out": "../../compile/plan.yaml",
            },
        ],
    )

    with pytest.raises(ValueError, match="reserved run artifact") as exc:
        build_plan_from_normalized(normalize_workflow(workflow))

    assert "compile/plan.yaml" in str(exc.value)
    assert "render.Hist.0" in str(exc.value)


def test_duplicate_recoil_debug_workflow_fails_before_render_output(
    toy_registry: dict[str, Any],
    tmp_path: Path,
) -> None:
    workflow = _workflow_with_hist_renders(
        toy_registry,
        [
            {"style": "hist_plot", "out": "RecoilDebug.png"},
            {"style": "hist_plot", "out": "RecoilDebug.png"},
        ],
    )
    workflow_path = tmp_path / "workflow.yaml"
    workflow_path.write_text(
        yaml.safe_dump(workflow, sort_keys=False), encoding="utf-8"
    )

    with pytest.raises(ValueError, match=r"RecoilDebug\.png"):
        compile_workflow_file(workflow_path, outdir=tmp_path / "build")

    assert not (tmp_path / "build" / "artifacts" / "plots" / "RecoilDebug.png").exists()


def _workflow_with_hist_renders(
    registry: dict[str, Any],
    renders: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        **_base_workflow(_registry_with_render_sink(registry)),
        "styles": {
            "hist_plot": {
                "op": "hep.render.hist1d",
                "axes": {"x": {"name": "pt"}},
            }
        },
        "analysis": {
            "stages": [
                {
                    "id": "Hist",
                    "op": "hep.hist",
                    "params": {
                        "input": "pt",
                        "axes": [{"name": "pt", "bins": 10, "low": 0, "high": 10}],
                    },
                    "render": renders,
                }
            ]
        },
    }


def _workflow_with_top_level_render_variations(
    registry: dict[str, Any],
) -> dict[str, Any]:
    return {
        **_base_workflow(_registry_with_render_sink(registry)),
        "analysis": {
            "stages": [
                {
                    "id": "Hist",
                    "op": "hep.hist",
                    "params": {
                        "input": "pt",
                        "axes": [{"name": "pt", "bins": 10, "low": 0, "high": 10}],
                    },
                },
                {
                    "id": "RecoilRender",
                    "op": "hep.render.hist1d",
                    "from": [{"node": "Hist", "port": "hist", "as": "target"}],
                    "out": "{variation}/../RecoilDebug.png",
                    "variations": {
                        "values": ["nominal", "up"],
                    },
                },
            ]
        },
    }


def _workflow_with_static_writes(
    registry: dict[str, Any],
    writes: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        **_base_workflow(registry),
        "analysis": {
            "stages": [
                {
                    "id": "Scale",
                    "op": "toy.scale",
                    "params": {"source": "pt", "output": "scaled_pt"},
                    "write": writes,
                }
            ]
        },
    }


def _base_workflow(registry: dict[str, Any]) -> dict[str, Any]:
    return {
        "version": "1.0",
        "registry": registry,
        "data": {
            "datasets": [{"name": "sample", "files": ["sample.root"], "nevents": 10}]
        },
        "sources": {
            "events": {
                "kind": "toy.source",
                "stream_type": "event_stream",
                "branches": ["pt"],
            }
        },
    }


def _registry_with_render_sink(registry: dict[str, Any]) -> dict[str, Any]:
    updated = deepcopy(registry)
    updated["sinks"]["hep.render.hist1d"] = {
        "spec": "tests.toy_components.sinks:TOY_COMPARISON_SINK_SPEC",
        "impl": "tests.toy_components.sinks:run_toy_write",
    }
    return updated
