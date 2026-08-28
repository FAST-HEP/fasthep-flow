from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from hepflow.api import compile_workflow_file
from hepflow.compiler.normalize import normalize_workflow
from hepflow.model.execution import resolve_node_resource_intent


def test_missing_execution_block_gives_defaults(toy_workflow: dict[str, Any]) -> None:
    normalized = normalize_workflow(toy_workflow)

    assert normalized["execution"] == {
        "backend": "local",
        "strategy": "default",
        "profiles": [],
        "resources": {},
        "pools": {},
        "environment": {},
        "staging": {"mode": "shared"},
        "config": {},
    }


def test_global_execution_normalization_preserves_metadata(
    toy_workflow: dict[str, Any],
) -> None:
    workflow = {
        **toy_workflow,
        "execution": {
            "backend": "dask",
            "strategy": "htcondor",
            "profiles": ["bristol_htcondor"],
            "resources": {
                "default": {
                    "cpus": 1,
                    "memory": "4GB",
                    "disk": "10GB",
                },
                "gpu": {
                    "cpus": 4,
                    "memory": "16GB",
                    "disk": "20GB",
                    "gpus": 1,
                },
            },
            "config": {
                "workers": 100,
                "walltime": "02:00:00",
                "queue": "workday",
            },
        },
    }

    normalized = normalize_workflow(workflow)

    assert normalized["execution"] == {
        **workflow["execution"],
        "environment": {},
        "staging": {"mode": "shared"},
        "pools": {
            "default": {
                "resources": "default",
                "workers": 100,
                "config": {},
            }
        },
    }


@pytest.mark.parametrize(
    ("execution", "message"),
    [
        ([], "execution must be a mapping"),
        ({"backend": 1}, "execution.backend must be a string"),
        ({"strategy": 1}, "execution.strategy must be a string"),
        ({"profiles": ["ok", 1]}, "must be a non-empty string"),
        ({"resources": []}, "execution.resources must be a mapping"),
        ({"pools": []}, "execution.pools must be a mapping"),
        ({"resources": {"gpu": 1}}, "must be a mapping"),
        (
            {"resources": {"gpu": {1: "bad"}}},
            "keys must be non-empty strings",
        ),
        (
            {"resources": {"gpu": {"gpus": []}}},
            "execution.resources\\['gpu'\\].gpus must be an integer or string",
        ),
        (
            {
                "resources": {"default": {}},
                "pools": {"gpu": {"resources": "gpu"}},
            },
            "references missing resource class 'gpu'",
        ),
        (
            {
                "resources": {"default": {}},
                "pools": {"default": {"resources": "default", "workers": 0}},
            },
            "workers must be a positive integer",
        ),
        (
            {
                "resources": {"default": {}},
                "pools": {"default": {"resources": 1}},
            },
            "resources must be a string",
        ),
        (
            {
                "resources": {"default": {}},
                "pools": {"default": {"resources": "default", "config": []}},
            },
            "config must be a mapping",
        ),
        ({"config": []}, "execution.config must be a mapping"),
        ({"environment": []}, "execution.environment must be a mapping"),
        ({"staging": []}, "execution.staging must be a mapping"),
        (
            {"staging": {"mode": "elsewhere"}},
            "execution.staging.mode must be 'shared' or 'transfer'",
        ),
        (
            {
                "environment": {
                    "type": "packed-pixi",
                    "mode": "prefix",
                    "environment": "default",
                }
            },
            "execution.environment.environment is ambiguous",
        ),
    ],
)
def test_invalid_global_execution_errors(
    toy_workflow: dict[str, Any],
    execution: Any,
    message: str,
) -> None:
    workflow = {**toy_workflow, "execution": execution}

    with pytest.raises(ValueError, match=message):
        normalize_workflow(workflow)


def test_stage_execution_prefer_fallback_modifiers_preserved(
    toy_workflow: dict[str, Any],
) -> None:
    workflow = _with_stage_execution(
        _with_resources(toy_workflow),
        {
            "prefer": "gpu",
            "fallback": "default",
            "timeout": "10m",
            "modifiers": ["gpu.preload", "cuda.jit"],
        },
    )

    normalized = normalize_workflow(workflow)

    assert normalized["analysis"]["stages"][0]["execution"] == {
        "require": None,
        "prefer": "gpu",
        "fallback": "default",
        "timeout": "10m",
        "modifiers": [
            {"name": "gpu.preload", "params": {}},
            {"name": "cuda.jit", "params": {}},
        ],
    }


def test_stage_execution_expanded_modifiers_preserve_params_and_order(
    toy_workflow: dict[str, Any],
) -> None:
    workflow = _with_stage_execution(
        toy_workflow,
        {
            "modifiers": [
                {
                    "name": "gpu.preload",
                    "params": {"fields": ["Jet_Pt", "Jet_Eta"]},
                },
                {"name": "cuda.jit", "params": {"mode": "eager"}},
            ]
        },
    )

    normalized = normalize_workflow(workflow)

    assert normalized["analysis"]["stages"][0]["execution"]["modifiers"] == [
        {"name": "gpu.preload", "params": {"fields": ["Jet_Pt", "Jet_Eta"]}},
        {"name": "cuda.jit", "params": {"mode": "eager"}},
    ]


def test_stage_execution_require_preserved(toy_workflow: dict[str, Any]) -> None:
    workflow = _with_stage_execution(_with_resources(toy_workflow), {"require": "gpu"})

    normalized = normalize_workflow(workflow)

    assert normalized["analysis"]["stages"][0]["execution"] == {
        "require": "gpu",
        "prefer": None,
        "fallback": None,
        "timeout": None,
        "modifiers": [],
    }


def test_execution_resources_and_pools_normalize(
    toy_workflow: dict[str, Any],
) -> None:
    workflow = {
        **toy_workflow,
        "execution": {
            "resources": {
                "default": {"cpus": 1, "memory": "4GB"},
                "gpu": {"cpus": 4, "memory": "16GB", "gpus": 1},
            },
            "pools": {
                "default": {"resources": "default", "workers": 100},
                "gpu": {"resources": "gpu", "workers": "2"},
            },
        },
    }

    normalized = normalize_workflow(workflow)

    assert normalized["execution"]["pools"] == {
        "default": {"resources": "default", "workers": 100, "config": {}},
        "gpu": {"resources": "gpu", "workers": 2, "config": {}},
    }


def test_implicit_default_pool_from_config_workers(
    toy_workflow: dict[str, Any],
) -> None:
    workflow = {**toy_workflow, "execution": {"config": {"workers": 4}}}

    normalized = normalize_workflow(workflow)

    assert normalized["execution"]["resources"] == {"default": {}}
    assert normalized["execution"]["pools"] == {
        "default": {"resources": "default", "workers": 4, "config": {}}
    }


@pytest.mark.parametrize(
    ("execution", "message"),
    [
        ([], "stage execution must be a mapping"),
        (
            {"require": "gpu", "prefer": "default"},
            "stage execution cannot define both require and prefer",
        ),
        ({"modifiers": "gpu.preload"}, "stage execution.modifiers must be a list"),
        (
            {"modifiers": [1]},
            r"stage execution\.modifiers\[0\] must be a string or mapping",
        ),
        (
            {"modifiers": [{"params": {}}]},
            r"stage execution\.modifiers\[0\] mapping must define name",
        ),
        (
            {"modifiers": [{"name": 1}]},
            r"stage execution\.modifiers\[0\]\.name must be a non-empty string",
        ),
        (
            {"modifiers": [{"name": "gpu.preload", "params": []}]},
            r"stage execution\.modifiers\[0\]\.params must be a mapping",
        ),
        (
            {"modifiers": ["gpu.preload", {"name": "gpu.preload"}]},
            "stage execution.modifiers contains duplicate modifier 'gpu.preload'",
        ),
        ({"timeout": []}, "stage execution.timeout must be a string or integer"),
    ],
)
def test_invalid_stage_execution_errors(
    toy_workflow: dict[str, Any],
    execution: Any,
    message: str,
) -> None:
    workflow = _with_stage_execution(toy_workflow, execution)

    with pytest.raises(ValueError, match=message):
        normalize_workflow(workflow)


def test_execution_metadata_propagates_to_plan(
    toy_workflow: dict[str, Any],
    tmp_path: Path,
) -> None:
    workflow = _with_stage_execution(
        {
            **toy_workflow,
            "registry": {
                "backends": {"dask": {"impl": "hepflow.backends:Local"}},
            },
            "execution": {
                "backend": "dask",
                "strategy": "htcondor",
                "profiles": ["bristol_htcondor"],
                "resources": {
                    "default": {"cpus": 1, "memory": "4GB"},
                    "gpu": {"cpus": 4, "memory": "16GB", "gpus": 1},
                },
                "pools": {
                    "default": {"resources": "default", "workers": 100},
                    "gpu": {"resources": "gpu", "workers": 2},
                },
                "config": {"workers": 100, "walltime": "02:00:00"},
            },
        },
        {"prefer": "gpu", "fallback": "default", "modifiers": ["gpu.preload"]},
    )
    workflow_path = tmp_path / "workflow.yaml"
    workflow_path.write_text(yaml.safe_dump(workflow, sort_keys=False), encoding="utf-8")

    plan = compile_workflow_file(workflow_path, outdir=tmp_path / "build")
    plan_yaml = plan.to_dict()

    assert plan_yaml["execution"]["resources"] == workflow["execution"]["resources"]
    assert plan_yaml["execution"]["environment"] == {}
    assert plan_yaml["execution"]["pools"] == {
        "default": {"resources": "default", "workers": 100, "config": {}},
        "gpu": {"resources": "gpu", "workers": 2, "config": {}},
    }
    stage_node = next(node for node in plan_yaml["nodes"] if node["id"] == "stage.Scale")
    assert stage_node["meta"]["execution"] == {
        "require": None,
        "prefer": "gpu",
        "fallback": "default",
        "timeout": None,
        "modifiers": [{"name": "gpu.preload", "params": {}}],
    }


def test_packed_pixi_worker_environment_spec_written_at_compile(
    toy_workflow: dict[str, Any],
    tmp_path: Path,
) -> None:
    workflow = {
        **toy_workflow,
        "registry": {
            "backends": {"dask": {"impl": "hepflow.backends:Local"}},
        },
        "execution": {
            "backend": "dask",
            "strategy": "htcondor",
            "environment": {
                "type": "packed-pixi",
                "mode": "prefix",
            },
        },
    }
    workflow_path = tmp_path / "workflow.yaml"
    workflow_path.write_text(yaml.safe_dump(workflow, sort_keys=False), encoding="utf-8")

    compile_workflow_file(workflow_path, outdir=tmp_path / "build")

    worker_env = yaml.safe_load(
        (tmp_path / "build" / "compile" / "worker_environment.json").read_text(
            encoding="utf-8"
        )
    )
    assert worker_env == {
        "type": "packed-pixi",
        "mode": "prefix",
        "source": "current",
    }


def test_packed_pixi_prefix_source_current_normalizes_explicitly(
    toy_workflow: dict[str, Any],
) -> None:
    workflow = {
        **toy_workflow,
        "execution": {
            "environment": {
                "type": "packed-pixi",
                "mode": "prefix",
                "source": "current",
            }
        },
    }

    normalized = normalize_workflow(workflow)

    assert normalized["execution"]["environment"] == {
        "type": "packed-pixi",
        "mode": "prefix",
        "source": "current",
    }


def test_stage_execution_unknown_resource_class_errors(
    toy_workflow: dict[str, Any],
) -> None:
    workflow = _with_stage_execution(
        _with_resources(toy_workflow),
        {"prefer": "missing"},
    )

    with pytest.raises(
        ValueError,
        match=r"execution\.prefer references unknown resource class 'missing'",
    ):
        normalize_workflow(workflow)


def test_stage_execution_resource_without_pool_errors_when_pools_defined(
    toy_workflow: dict[str, Any],
) -> None:
    workflow = _with_stage_execution(
        {
            **toy_workflow,
            "execution": {
                "resources": {
                    "default": {"cpus": 1},
                    "gpu": {"cpus": 4, "gpus": 1},
                },
                "pools": {"default": {"resources": "default", "workers": 2}},
            },
        },
        {"require": "gpu"},
    )

    with pytest.raises(
        ValueError,
        match="no execution pool provides it",
    ):
        normalize_workflow(workflow)


def test_node_resource_intent_resolves_gpu_resources(
    toy_workflow: dict[str, Any],
    tmp_path: Path,
) -> None:
    workflow = _with_stage_execution(
        _with_resources(toy_workflow),
        {"prefer": "gpu", "fallback": "default", "timeout": "10m"},
    )
    workflow_path = tmp_path / "workflow.yaml"
    workflow_path.write_text(yaml.safe_dump(workflow, sort_keys=False), encoding="utf-8")
    plan = compile_workflow_file(workflow_path, outdir=tmp_path / "build")

    intent = resolve_node_resource_intent(plan, "stage.Scale")

    assert intent.prefer == "gpu"
    assert intent.fallback == "default"
    assert intent.preferred_resource == {
        "cpus": 4,
        "memory": "16GB",
        "disk": "20GB",
        "gpus": 1,
    }
    assert intent.fallback_resource == {
        "cpus": 1,
        "memory": "4GB",
        "disk": "10GB",
    }


def test_node_resource_intent_resolves_required_gpu_resources(
    toy_workflow: dict[str, Any],
    tmp_path: Path,
) -> None:
    workflow = _with_stage_execution(_with_resources(toy_workflow), {"require": "gpu"})
    workflow_path = tmp_path / "workflow.yaml"
    workflow_path.write_text(yaml.safe_dump(workflow, sort_keys=False), encoding="utf-8")
    plan = compile_workflow_file(workflow_path, outdir=tmp_path / "build")

    intent = resolve_node_resource_intent(plan, plan.get_node("stage.Scale"))

    assert intent.require == "gpu"
    assert intent.required_resource == {
        "cpus": 4,
        "memory": "16GB",
        "disk": "20GB",
        "gpus": 1,
    }


def test_node_resource_intent_lists_candidate_pools(
    toy_workflow: dict[str, Any],
    tmp_path: Path,
) -> None:
    workflow = _with_stage_execution(
        {
            **toy_workflow,
            "execution": {
                "resources": {
                    "default": {"cpus": 1},
                    "gpu": {"cpus": 4, "gpus": 1},
                },
                "pools": {
                    "default": {"resources": "default", "workers": 10},
                    "gpu-small": {"resources": "gpu", "workers": 2},
                },
            },
        },
        {"prefer": "gpu", "fallback": "default"},
    )
    workflow_path = tmp_path / "workflow.yaml"
    workflow_path.write_text(yaml.safe_dump(workflow, sort_keys=False), encoding="utf-8")
    plan = compile_workflow_file(workflow_path, outdir=tmp_path / "build")

    intent = resolve_node_resource_intent(plan, "stage.Scale")

    assert intent.candidate_pools == [
        {"name": "default", "resources": "default", "workers": 10, "config": {}},
        {"name": "gpu-small", "resources": "gpu", "workers": 2, "config": {}},
    ]


def test_workflow_without_stage_execution_has_no_node_resource_intent(
    toy_workflow_path: Path,
    tmp_path: Path,
) -> None:
    plan = compile_workflow_file(toy_workflow_path, outdir=tmp_path / "build")
    node = plan.get_node("stage.Scale")

    assert "execution" not in node.meta
    assert resolve_node_resource_intent(plan, node).to_dict() == {
        "require": None,
        "prefer": None,
        "fallback": None,
        "required_resource": None,
        "preferred_resource": None,
        "fallback_resource": None,
        "candidate_pools": [],
    }


def _with_stage_execution(
    toy_workflow: dict[str, Any],
    execution: Any,
) -> dict[str, Any]:
    workflow = {
        **toy_workflow,
        "analysis": {
            **toy_workflow["analysis"],
            "stages": [dict(toy_workflow["analysis"]["stages"][0])],
        },
    }
    workflow["analysis"]["stages"][0]["execution"] = execution
    return workflow


def _with_resources(toy_workflow: dict[str, Any]) -> dict[str, Any]:
    return {
        **toy_workflow,
        "execution": {
            "backend": "local",
            "strategy": "default",
            "profiles": [],
            "resources": {
                "default": {"cpus": 1, "memory": "4GB", "disk": "10GB"},
                "gpu": {
                    "cpus": 4,
                    "memory": "16GB",
                    "disk": "20GB",
                    "gpus": 1,
                },
            },
            "pools": {},
            "config": {},
        },
    }
