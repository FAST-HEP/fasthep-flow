from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
import yaml

from hepflow.api import compile_workflow_file
from hepflow.model.persistence import PersistenceSettings
from hepflow.model.plan import ExecutionPlan
from hepflow.model.products import BoundaryOutputSpec, ProductBoundaryPolicy
from hepflow.registry.loaders import runtime_registry_from_config
from hepflow.runtime.engine import execute_plan_locally
from hepflow.runtime.persistence.manifest import read_manifest
from hepflow.runtime.persistence.select import resolve_persistence

TOY = "tests.toy_components.persistence"
KIND_CFG = {
    "persistence": {
        "formats": {
            "toy.counts": {
                "version": "1",
                "container": "json",
                "payload_roles": ["data"],
                "fidelity": "exact",
                "save": f"{TOY}:save_toy_counts",
                "load": f"{TOY}:load_toy_counts",
            }
        }
    }
}


def _registry(*, representation: str = "value", with_capability: bool = True) -> Any:
    handler: dict[str, Any] = {
        "boundary": {"retain": True, "representation": representation},
        "combine": "tests.toy_components.transforms:combine_toy_histograms",
        "merge": "tests.toy_components.transforms:merge_toy_histograms",
    }
    if with_capability:
        handler.update(KIND_CFG)
    return runtime_registry_from_config({"product_handlers": {"histogram": handler}})


def _spec(kind: str = "histogram", node: str = "stage.H", out: str = "hist") -> Any:
    return BoundaryOutputSpec(
        node_id=node, output_name=out, kind=kind, policy=ProductBoundaryPolicy()
    )


def test_mode_off_selects_nothing() -> None:
    selection = resolve_persistence(
        [_spec()], _registry(), PersistenceSettings(mode="off")
    )
    assert selection.persisted == {}
    assert selection.not_persisted == []


def test_mode_all_persists_kinds_with_a_capability() -> None:
    selection = resolve_persistence(
        [_spec()], _registry(), PersistenceSettings(mode="all")
    )
    assert selection.persisted[("stage.H", "hist")].name == "toy.counts"


def test_mode_all_lists_a_kind_without_a_capability() -> None:
    selection = resolve_persistence(
        [_spec()], _registry(with_capability=False), PersistenceSettings(mode="all")
    )
    assert selection.persisted == {}
    assert selection.not_persisted == [
        {
            "node": "stage.H",
            "output": "hist",
            "kind": "histogram",
            "reason": "no capability",
        }
    ]


def test_mode_selected_needs_an_explicit_choice_or_a_kind_default() -> None:
    settings = PersistenceSettings(mode="selected")
    assert resolve_persistence([_spec()], _registry(), settings).persisted == {}
    by_kind = resolve_persistence(
        [_spec()], _registry(representation="persist"), settings
    )
    assert ("stage.H", "hist") in by_kind.persisted
    explicit = resolve_persistence(
        [_spec()],
        _registry(),
        PersistenceSettings(mode="selected", products={"stage.H:hist": True}),
    )
    assert ("stage.H", "hist") in explicit.persisted


def test_explicit_choice_overrides_mode() -> None:
    settings = PersistenceSettings(mode="all", products={"stage.H:hist": False})
    assert resolve_persistence([_spec()], _registry(), settings).persisted == {}


def test_explicit_choice_without_capability_fails() -> None:
    settings = PersistenceSettings(mode="selected", products={"stage.H:hist": True})
    with pytest.raises(ValueError, match=r"stage\.H:hist.*no persistence capability"):
        resolve_persistence([_spec()], _registry(with_capability=False), settings)


def test_mode_off_overrides_a_kind_default() -> None:
    selection = resolve_persistence(
        [_spec()], _registry(representation="persist"), PersistenceSettings(mode="off")
    )
    assert selection.persisted == {}


def _workflow(toy_workflow: dict[str, Any], persistence: dict[str, Any] | None) -> Any:
    execution: dict[str, Any] = {"persistence": persistence} if persistence else {}
    return {
        **toy_workflow,
        "data": {
            "datasets": [{"name": "toydata", "files": ["toy://events"], "nevents": 4}]
        },
        "execution": execution,
        "registry": {
            **toy_workflow["registry"],
            "product_handlers": {
                "histogram": {
                    "boundary": {"retain": True, "representation": "value"},
                    "combine": "tests.toy_components.transforms:combine_toy_histograms",
                    "merge": "tests.toy_components.transforms:merge_toy_histograms",
                    **KIND_CFG,
                }
            },
        },
        "analysis": {
            "stages": [
                {"id": "RecoilHist", "op": "hep.hist", "params": {"name": "recoil"}}
            ]
        },
    }


def _run(
    toy_workflow: dict[str, Any], tmp_path: Path, persistence: dict[str, Any] | None
) -> tuple[ExecutionPlan, Any]:
    path = tmp_path / "workflow.yaml"
    path.write_text(
        yaml.safe_dump(_workflow(toy_workflow, persistence), sort_keys=False),
        encoding="utf-8",
    )
    plan = compile_workflow_file(path, outdir=tmp_path / "build", chunk_size=2)
    results = execute_plan_locally(
        plan,
        registry_cfg=plan.registry,
        ctx={"outdir": str(tmp_path / "build")},
        partitions=plan.partitions,
    )
    return plan, results


def test_mode_all_publishes_one_product_set_per_partition(
    toy_workflow: dict[str, Any], tmp_path: Path
) -> None:
    plan, _ = _run(toy_workflow, tmp_path, {"mode": "all"})

    manifests = sorted(
        (tmp_path / "build" / "artifacts" / "products").rglob("manifest.json")
    )
    assert len(manifests) == len(plan.partitions) == 2
    manifest = read_manifest(manifests[0])
    [entry] = manifest.products
    assert (entry.producer_node, entry.output_name) == ("stage.RecoilHist", "hist")
    assert entry.storage.capability == "toy.counts"
    assert entry.storage.capability_version == "1"
    assert entry.scope == "partition"
    assert entry.dataset_name == "toydata"
    payload = json.loads((manifests[0].parent / entry.payloads[0].uri).read_text())
    assert "entries" in payload


def test_mode_off_changes_nothing(toy_workflow: dict[str, Any], tmp_path: Path) -> None:
    plan, _ = _run(toy_workflow, tmp_path, None)

    assert "persistence" not in plan.execution
    assert not (tmp_path / "build" / "artifacts" / "products").exists()


def test_persistence_root_is_configurable(
    toy_workflow: dict[str, Any], tmp_path: Path
) -> None:
    target = tmp_path / "elsewhere"
    _run(toy_workflow, tmp_path, {"mode": "all", "root": f"file://{target}"})

    assert len(list(target.rglob("manifest.json"))) == 2


def test_remote_root_is_refused_with_a_clear_error(
    toy_workflow: dict[str, Any], tmp_path: Path
) -> None:
    with pytest.raises(NotImplementedError, match="root://"):
        _run(toy_workflow, tmp_path, {"mode": "all", "root": "root://eos.example//p"})


def test_backend_summary_lists_persisted_and_unpersisted_products(
    toy_workflow: dict[str, Any], tmp_path: Path
) -> None:
    from hepflow.backends.local import LocalBackend  # noqa: PLC0415

    path = tmp_path / "workflow.yaml"
    path.write_text(
        yaml.safe_dump(_workflow(toy_workflow, {"mode": "all"}), sort_keys=False),
        encoding="utf-8",
    )
    plan = compile_workflow_file(path, outdir=tmp_path / "build", chunk_size=2)

    result = LocalBackend().run(plan, ctx={"outdir": str(tmp_path / "build")})

    persisted = result.summary["persistence"]["persisted"]
    assert [(p["node"], p["capability"]) for p in persisted] == [
        ("stage.RecoilHist", "toy.counts")
    ]


def test_published_evidence_names_the_producing_partition(
    toy_workflow: dict[str, Any], tmp_path: Path
) -> None:
    _run(toy_workflow, tmp_path, {"mode": "all"})

    manifests = sorted(
        (tmp_path / "build" / "artifacts" / "products").rglob("manifest.json")
    )
    evidence = json.loads((manifests[0].parent / "evidence.json").read_text())
    assert evidence["identity"]["producer_node"] == "stage.RecoilHist"
    assert evidence["attempt"]["status"] == "succeeded"
    # toy operations record no executions; the extraction itself is covered in
    # test_evidence_import.test_build_evidence_collects_the_partition_execution_and_resources
    assert evidence["identity"]["partition_id"] == manifests[0].parent.name
    assert isinstance(evidence["executions"], dict)
