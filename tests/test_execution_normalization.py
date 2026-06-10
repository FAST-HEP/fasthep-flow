from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from hepflow.api import compile_author_file
from hepflow.compiler.normalize import normalize_author
from hepflow.model.execution import resolve_node_resource_intent


def test_missing_execution_block_gives_defaults(toy_author: dict[str, Any]) -> None:
    normalized = normalize_author(toy_author)

    assert normalized["execution"] == {
        "backend": "local",
        "strategy": "default",
        "profiles": [],
        "resources": {},
        "config": {},
    }


def test_global_execution_normalization_preserves_metadata(
    toy_author: dict[str, Any],
) -> None:
    author = {
        **toy_author,
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

    normalized = normalize_author(author)

    assert normalized["execution"] == author["execution"]


@pytest.mark.parametrize(
    ("execution", "message"),
    [
        ([], "execution must be a mapping"),
        ({"backend": 1}, "execution.backend must be a string"),
        ({"strategy": 1}, "execution.strategy must be a string"),
        ({"profiles": ["ok", 1]}, "must be a non-empty string"),
        ({"resources": []}, "execution.resources must be a mapping"),
        ({"resources": {"gpu": 1}}, "must be a mapping"),
        (
            {"resources": {"gpu": {1: "bad"}}},
            "keys must be non-empty strings",
        ),
        (
            {"resources": {"gpu": {"gpus": []}}},
            "execution.resources\\['gpu'\\].gpus must be an integer or string",
        ),
        ({"config": []}, "execution.config must be a mapping"),
    ],
)
def test_invalid_global_execution_errors(
    toy_author: dict[str, Any],
    execution: Any,
    message: str,
) -> None:
    author = {**toy_author, "execution": execution}

    with pytest.raises(ValueError, match=message):
        normalize_author(author)


def test_stage_execution_prefer_fallback_modifiers_preserved(
    toy_author: dict[str, Any],
) -> None:
    author = _with_stage_execution(
        _with_resources(toy_author),
        {
            "prefer": "gpu",
            "fallback": "default",
            "timeout": "10m",
            "modifiers": ["gpu.preload", "cuda.jit"],
        },
    )

    normalized = normalize_author(author)

    assert normalized["analysis"]["stages"][0]["execution"] == {
        "require": None,
        "prefer": "gpu",
        "fallback": "default",
        "timeout": "10m",
        "modifiers": ["gpu.preload", "cuda.jit"],
    }


def test_stage_execution_require_preserved(toy_author: dict[str, Any]) -> None:
    author = _with_stage_execution(_with_resources(toy_author), {"require": "gpu"})

    normalized = normalize_author(author)

    assert normalized["analysis"]["stages"][0]["execution"] == {
        "require": "gpu",
        "prefer": None,
        "fallback": None,
        "timeout": None,
        "modifiers": [],
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
        ({"modifiers": ["ok", 1]}, "must be a non-empty string"),
        ({"timeout": []}, "stage execution.timeout must be a string or integer"),
    ],
)
def test_invalid_stage_execution_errors(
    toy_author: dict[str, Any],
    execution: Any,
    message: str,
) -> None:
    author = _with_stage_execution(toy_author, execution)

    with pytest.raises(ValueError, match=message):
        normalize_author(author)


def test_execution_metadata_propagates_to_plan(
    toy_author: dict[str, Any],
    tmp_path: Path,
) -> None:
    author = _with_stage_execution(
        {
            **toy_author,
            "execution": {
                "backend": "dask",
                "strategy": "htcondor",
                "profiles": ["bristol_htcondor"],
                "resources": {
                    "default": {"cpus": 1, "memory": "4GB"},
                    "gpu": {"cpus": 4, "memory": "16GB", "gpus": 1},
                },
                "config": {"workers": 100, "walltime": "02:00:00"},
            },
        },
        {"prefer": "gpu", "fallback": "default", "modifiers": ["gpu.preload"]},
    )
    author_path = tmp_path / "author.yaml"
    author_path.write_text(yaml.safe_dump(author, sort_keys=False), encoding="utf-8")

    plan = compile_author_file(author_path, outdir=tmp_path / "build")
    plan_yaml = plan.to_dict()

    assert plan_yaml["execution"] == author["execution"]
    stage_node = next(node for node in plan_yaml["nodes"] if node["id"] == "stage.Scale")
    assert stage_node["meta"]["execution"] == {
        "require": None,
        "prefer": "gpu",
        "fallback": "default",
        "timeout": None,
        "modifiers": ["gpu.preload"],
    }


def test_stage_execution_unknown_resource_class_errors(
    toy_author: dict[str, Any],
) -> None:
    author = _with_stage_execution(
        _with_resources(toy_author),
        {"prefer": "missing"},
    )

    with pytest.raises(
        ValueError,
        match=r"execution\.prefer references unknown resource class 'missing'",
    ):
        normalize_author(author)


def test_node_resource_intent_resolves_gpu_resources(
    toy_author: dict[str, Any],
    tmp_path: Path,
) -> None:
    author = _with_stage_execution(
        _with_resources(toy_author),
        {"prefer": "gpu", "fallback": "default", "timeout": "10m"},
    )
    author_path = tmp_path / "author.yaml"
    author_path.write_text(yaml.safe_dump(author, sort_keys=False), encoding="utf-8")
    plan = compile_author_file(author_path, outdir=tmp_path / "build")

    intent = resolve_node_resource_intent(plan, "stage.Scale")

    assert intent.prefer == "gpu"
    assert intent.fallback == "default"
    assert intent.preferred_resources == {
        "cpus": 4,
        "memory": "16GB",
        "disk": "20GB",
        "gpus": 1,
    }
    assert intent.fallback_resources == {
        "cpus": 1,
        "memory": "4GB",
        "disk": "10GB",
    }


def test_node_resource_intent_resolves_required_gpu_resources(
    toy_author: dict[str, Any],
    tmp_path: Path,
) -> None:
    author = _with_stage_execution(_with_resources(toy_author), {"require": "gpu"})
    author_path = tmp_path / "author.yaml"
    author_path.write_text(yaml.safe_dump(author, sort_keys=False), encoding="utf-8")
    plan = compile_author_file(author_path, outdir=tmp_path / "build")

    intent = resolve_node_resource_intent(plan, plan.get_node("stage.Scale"))

    assert intent.require == "gpu"
    assert intent.required_resources == {
        "cpus": 4,
        "memory": "16GB",
        "disk": "20GB",
        "gpus": 1,
    }


def test_workflow_without_stage_execution_has_no_node_resource_intent(
    toy_author_path: Path,
    tmp_path: Path,
) -> None:
    plan = compile_author_file(toy_author_path, outdir=tmp_path / "build")
    node = plan.get_node("stage.Scale")

    assert "execution" not in node.meta
    assert resolve_node_resource_intent(plan, node).to_dict() == {
        "require": None,
        "prefer": None,
        "fallback": None,
        "required_resources": None,
        "preferred_resources": None,
        "fallback_resources": None,
    }


def _with_stage_execution(
    toy_author: dict[str, Any],
    execution: Any,
) -> dict[str, Any]:
    author = {
        **toy_author,
        "analysis": {
            **toy_author["analysis"],
            "stages": [dict(toy_author["analysis"]["stages"][0])],
        },
    }
    author["analysis"]["stages"][0]["execution"] = execution
    return author


def _with_resources(toy_author: dict[str, Any]) -> dict[str, Any]:
    return {
        **toy_author,
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
            "config": {},
        },
    }
