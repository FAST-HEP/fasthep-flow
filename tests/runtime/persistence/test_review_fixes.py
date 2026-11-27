"""Regressions from the whole-branch review of the persistence stage."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml
from tests.toy_components.persistence import (
    persistence_workflow,
    registry_with,
    toy_capability,
)

from hepflow.api import compile_workflow_file
from hepflow.model.persistence import PersistenceSettings
from hepflow.model.plan import ExecutionNode, ExecutionPartition, ExecutionPlan
from hepflow.model.products import BoundaryOutputSpec, BoundaryProduct
from hepflow.runtime import engine
from hepflow.runtime.persistence import errors
from hepflow.runtime.persistence.evidence import (
    EvidenceLedger,
    import_evidence,
)
from hepflow.runtime.persistence.restore import restore_manifest
from hepflow.runtime.persistence.reuse import reuse_partition_products
from hepflow.runtime.persistence.save import (
    persist_boundary_products,
    persistence_root,
    product_folder,
    provisional_plan_hash,
)
from hepflow.runtime.persistence.select import (
    PersistenceSelection,
    resolve_persistence,
)
from hepflow.runtime.provenance.recorder import ProvenanceRecorder


def _plan(*outputs: str) -> ExecutionPlan:
    plan = ExecutionPlan()
    for name in outputs:
        plan.add_node(
            ExecutionNode(
                id=f"stage.{name}",
                graph_node_id=f"stage.{name}",
                role="transform",
                impl="hep.hist",
                outputs={"hist": "histogram"},
            )
        )
    return plan


def _partition() -> ExecutionPartition:
    return ExecutionPartition(
        id="events__toy__0",
        dataset="toy",
        file="toy://events",
        source="events",
        part="0_0",
        start=0,
        stop=2,
    )


def _spec(name: str) -> BoundaryOutputSpec:
    return BoundaryOutputSpec(
        node_id=f"stage.{name}", output_name="hist", kind="histogram"
    )


def _product(name: str, partition: ExecutionPartition) -> BoundaryProduct:
    return BoundaryProduct(
        node_id=f"stage.{name}",
        output_name="hist",
        kind="histogram",
        dataset=partition.dataset,
        partition_id=partition.id,
        representation="persist",
        value={"entries": 1},
    )


def _publish_two(tmp_path: Path, plan: ExecutionPlan) -> tuple[Any, Any, Any]:
    capability = toy_capability()
    partition = _partition()
    products = [_product("A", partition), _product("B", partition)]
    selection = PersistenceSelection(
        persisted={product.key(): capability for product in products}
    )
    settings = PersistenceSettings(mode="all", reuse=True)
    persist_boundary_products(
        products,
        selection,
        plan=plan,
        settings=settings,
        outdir=tmp_path,
    )
    return partition, selection, settings


def _reuse(plan: Any, partition: Any, selection: Any, settings: Any, tmp: Path) -> Any:
    ledger = EvidenceLedger()
    recorder = ProvenanceRecorder()
    result = reuse_partition_products(
        plan,
        partition,
        boundary=[_spec("A"), _spec("B")],
        selection=selection,
        settings=settings,
        registry=registry_with(toy_capability()),
        outdir=tmp,
        ledger=ledger,
        store=recorder.store,
    )
    return result, ledger, recorder


# -- C1 ---------------------------------------------------------------------


def _workflow_file(toy_workflow: dict[str, Any], tmp_path: Path) -> Path:
    path = tmp_path / "workflow.yaml"
    path.write_text(
        yaml.safe_dump(
            persistence_workflow(toy_workflow, {"mode": "all", "reuse": True}),
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return path


def _run(workflow: Path, build: Path, monkeypatch: pytest.MonkeyPatch) -> list[str]:
    calls: list[str] = []
    real = engine.execute_plan_partition

    def counting(plan: Any, *, ctx: dict[str, Any], **kwargs: Any) -> Any:
        calls.append(ctx["partition"]["id"])
        return real(plan, ctx=ctx, **kwargs)

    monkeypatch.setattr(engine, "execute_plan_partition", counting)
    plan = compile_workflow_file(workflow, outdir=build, chunk_size=2)
    engine.execute_plan_locally(
        plan,
        registry_cfg=plan.registry,
        ctx={"outdir": str(build)},
        partitions=plan.partitions,
    )
    monkeypatch.undo()
    return calls


@pytest.mark.parametrize(
    ("section", "value"),
    [("files", ["toy://OTHER"]), ("eventtype", "data"), ("xsec", 12.5)],
)
def test_changed_dataset_input_is_not_reused(
    toy_workflow: dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    section: str,
    value: Any,
) -> None:
    workflow = _workflow_file(toy_workflow, tmp_path)
    build = tmp_path / "build"
    _run(workflow, build, monkeypatch)
    document = yaml.safe_load(workflow.read_text())
    document["data"]["datasets"][0][section] = value
    workflow.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")

    calls = _run(workflow, build, monkeypatch)

    assert len(calls) == 2


def test_plan_hash_ignores_the_persistence_setting_itself() -> None:
    plan = _plan("A")
    before = provisional_plan_hash(plan)
    plan.execution["persistence"] = {"mode": "all", "reuse": True}
    assert provisional_plan_hash(plan) == before


# -- I1 ---------------------------------------------------------------------


@pytest.mark.parametrize("persistence", [None, {"mode": "off"}])
def test_mode_off_leaves_the_run_summary_unchanged(
    toy_workflow: dict[str, Any], tmp_path: Path, persistence: Any
) -> None:
    from hepflow.backends.local import LocalBackend  # noqa: PLC0415

    path = tmp_path / "workflow.yaml"
    path.write_text(
        yaml.safe_dump(
            persistence_workflow(toy_workflow, persistence), sort_keys=False
        ),
        encoding="utf-8",
    )
    plan = compile_workflow_file(path, outdir=tmp_path / "build", chunk_size=2)

    result = LocalBackend().run(plan, ctx={"outdir": str(tmp_path / "build")})

    assert "persistence" not in result.summary


# -- I2, I4 -----------------------------------------------------------------


def test_reuse_imports_nothing_when_one_product_is_missing(tmp_path: Path) -> None:
    plan = _plan("A", "B")
    partition, selection, settings = _publish_two(tmp_path, plan)
    root = persistence_root(settings, tmp_path)
    (
        product_folder(root, "stage.B", "hist", "toy", partition.id) / "manifest.json"
    ).unlink()

    result, ledger, recorder = _reuse(plan, partition, selection, settings, tmp_path)

    assert result is None
    assert ledger.consumptions == []
    assert ledger.producers == {}
    assert recorder.store.executions() == {}


def test_reuse_imports_nothing_for_a_stale_plan(tmp_path: Path) -> None:
    plan = _plan("A", "B")
    partition, selection, settings = _publish_two(tmp_path, plan)
    plan.nodes[0].params["changed"] = True

    result, ledger, _ = _reuse(plan, partition, selection, settings, tmp_path)

    assert result is None
    assert ledger.consumptions == []


def test_reuse_checks_the_identity_of_each_set(tmp_path: Path) -> None:
    plan = _plan("A", "B")
    partition, selection, settings = _publish_two(tmp_path, plan)
    root = persistence_root(settings, tmp_path)
    folder_a = product_folder(root, "stage.A", "hist", "toy", partition.id)
    folder_b = product_folder(root, "stage.B", "hist", "toy", partition.id)
    (folder_b / "manifest.json").write_text(
        (folder_a / "manifest.json").read_text(), encoding="utf-8"
    )

    result, _, _ = _reuse(plan, partition, selection, settings, tmp_path)

    assert result is None


def test_a_corrupt_manifest_is_rerun_not_fatal(tmp_path: Path) -> None:
    plan = _plan("A", "B")
    partition, selection, settings = _publish_two(tmp_path, plan)
    root = persistence_root(settings, tmp_path)
    (
        product_folder(root, "stage.A", "hist", "toy", partition.id) / "manifest.json"
    ).write_text("{not json", encoding="utf-8")

    result, _, _ = _reuse(plan, partition, selection, settings, tmp_path)

    assert result is None


def test_reuse_reports_the_consumption_in_the_run_context(
    toy_workflow: dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workflow = _workflow_file(toy_workflow, tmp_path)
    build = tmp_path / "build"
    _run(workflow, build, monkeypatch)
    plan = compile_workflow_file(workflow, outdir=build, chunk_size=2)
    ctx: dict[str, Any] = {"outdir": str(build)}

    engine.execute_plan_locally(
        plan, registry_cfg=plan.registry, ctx=ctx, partitions=plan.partitions
    )

    consumed = ctx["_persistence"]["consumed"]
    assert len(consumed) == 2
    assert all(item["manifest"].endswith("manifest.json") for item in consumed)


def test_reuse_is_refused_when_the_plan_has_execution_hooks(tmp_path: Path) -> None:
    plan = _plan("A", "B")
    partition, selection, settings = _publish_two(tmp_path, plan)
    plan.execution_hooks = [{"name": "timing"}]

    result, _, _ = _reuse(plan, partition, selection, settings, tmp_path)

    assert result is None


# -- I3 ---------------------------------------------------------------------


def _evidence(output: str) -> dict[str, Any]:
    return {
        "schema_version": "1",
        "identity": {
            "producer_node": "stage.Fill",
            "output_name": output,
            "product_kind": "histogram",
            "dataset_name": "toy",
            "partition_id": "p0",
        },
        "attempt": {"id": "a1", "status": "succeeded"},
        "executions": {
            "stage.Fill::p0": {
                "id": "stage.Fill::p0",
                "node_id": "stage.Fill",
                "partition": "p0",
                "operations": [{"inputs": {"symbols": ["x"]}}],
            }
        },
        "resources": {},
    }


def test_two_outputs_of_one_partition_import_the_execution_once() -> None:
    recorder = ProvenanceRecorder()
    ledger = EvidenceLedger()
    import_evidence(ledger, _evidence("a"), store=recorder.store)
    import_evidence(ledger, _evidence("b"), store=recorder.store)

    [execution] = recorder.store.executions().values()
    assert len(execution.operations) == 1


def test_a_fresh_ledger_on_the_same_store_adds_nothing() -> None:
    recorder = ProvenanceRecorder()
    import_evidence(EvidenceLedger(), _evidence("a"), store=recorder.store)
    import_evidence(EvidenceLedger(), _evidence("a"), store=recorder.store)

    [execution] = recorder.store.executions().values()
    assert len(execution.operations) == 1


# -- I5 ---------------------------------------------------------------------


def test_a_crash_while_republishing_leaves_nothing_published(tmp_path: Path) -> None:
    plan = _plan("A")
    capability = toy_capability()
    partition = _partition()
    product = _product("A", partition)
    selection = PersistenceSelection(persisted={product.key(): capability})
    settings = PersistenceSettings(mode="all")
    [saved] = persist_boundary_products(
        [product], selection, plan=plan, settings=settings, outdir=tmp_path
    )
    manifest = Path(saved.metadata["persisted"])

    def crashing_save(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("killed")

    broken = toy_capability(save=crashing_save)
    with pytest.raises(RuntimeError, match="killed"):
        persist_boundary_products(
            [product],
            PersistenceSelection(persisted={product.key(): broken}),
            plan=plan,
            settings=settings,
            outdir=tmp_path,
        )

    assert not manifest.exists()


def test_evidence_changed_after_publication_is_caught(tmp_path: Path) -> None:
    plan = _plan("A")
    product = _product("A", _partition())
    [saved] = persist_boundary_products(
        [product],
        PersistenceSelection(persisted={product.key(): toy_capability()}),
        plan=plan,
        settings=PersistenceSettings(mode="all"),
        outdir=tmp_path,
    )
    manifest = Path(saved.metadata["persisted"])
    (manifest.parent / "evidence.json").write_text(
        '{"changed": true}', encoding="utf-8"
    )

    with pytest.raises(errors.SizeOrHashMismatch, match="evidence"):
        restore_manifest(manifest, registry_with(toy_capability()))


# -- I6 ---------------------------------------------------------------------


def test_product_folders_do_not_collide(tmp_path: Path) -> None:
    root = tmp_path
    first = product_folder(root, "n", "o", "W+jets", "p")
    second = product_folder(root, "n", "o", "W jets", "p")
    third = product_folder(root, "n", "o", "_W_jets_", "p")
    assert len({first, second, third}) == 3


@pytest.mark.parametrize("name", ["..", ".", ""])
def test_dot_names_cannot_climb(tmp_path: Path, name: str) -> None:
    folder = product_folder(tmp_path, "n", "o", name, "p")
    assert tmp_path in folder.parents
    assert ".." not in folder.parts


# -- I7 ---------------------------------------------------------------------


@pytest.mark.parametrize(
    "resource",
    [
        {"kind": "dataset", "metadata": {"authorization": "x"}},
        {"kind": "dataset", "metadata": {"private_key": "x"}},
        {"kind": "dataset", "metadata": {"cookie": "x"}},
        {"kind": "dataset", "selected": {"path": "https://user:pw@host/x"}},
        {"kind": "dataset", "selected": {"path": "root://eos//f.root?authz=abc"}},
    ],
)
def test_credentials_are_refused_at_save(
    tmp_path: Path, resource: dict[str, Any]
) -> None:
    recorder = ProvenanceRecorder()
    with recorder.operation_context(
        node_id="stage.A",
        impl="x",
        role="transform",
        dataset="toy",
        partition={"id": "events__toy__0"},
    ):
        recorder.record_operation(inputs={"resources": ["r"]})
    recorder.record_resource_record("r", resource)
    product = _product("A", _partition())

    with pytest.raises(errors.InvalidBinding, match="credential"):
        persist_boundary_products(
            [product],
            PersistenceSelection(persisted={product.key(): toy_capability()}),
            plan=_plan("A"),
            settings=PersistenceSettings(mode="all"),
            outdir=tmp_path,
            store=recorder.store,
        )


def test_tokenizer_is_not_a_credential() -> None:
    evidence = _evidence("a")
    evidence["executions"]["stage.Fill::p0"]["params"] = {"tokenizer": "bert"}
    import_evidence(EvidenceLedger(), evidence)


# -- I8, I11 ----------------------------------------------------------------


def test_a_capability_without_save_is_not_listed_as_persisted() -> None:
    capability = toy_capability(save=None)
    selection = resolve_persistence(
        [_spec("A")], registry_with(capability), PersistenceSettings(mode="all")
    )
    assert selection.persisted == {}
    assert selection.not_persisted[0]["reason"] == "no save implementation"


def test_an_unknown_product_key_is_an_error() -> None:
    settings = PersistenceSettings(mode="selected", products={"stage.Typo:hist": True})
    with pytest.raises(ValueError, match=r"stage\.Typo:hist.*no boundary product"):
        resolve_persistence([_spec("A")], registry_with(toy_capability()), settings)


def test_products_with_mode_off_is_an_error() -> None:
    from hepflow.model.persistence import normalize_persistence  # noqa: PLC0415

    with pytest.raises(ValueError, match="mode is off"):
        normalize_persistence({"mode": "off", "products": {"a:b": True}})


# -- M1 ---------------------------------------------------------------------


@pytest.mark.parametrize("root", ["file://rel/p", "file://host/p"])
def test_a_file_root_with_a_host_is_refused(root: str, tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="host"):
        persistence_root(PersistenceSettings(mode="all", root=root), tmp_path)


def test_a_file_root_is_unquoted(tmp_path: Path) -> None:
    root = persistence_root(
        PersistenceSettings(mode="all", root="file:///my%20dir/p"), tmp_path
    )
    assert root == Path("/my dir/p")


# -- second review (stage 2): hash scope and global side products -----------------


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("strategy", "htcondor"),
        ("config", {"worker_start_timeout": 999}),
        ("pools", {"gpu": {"workers": 3}}),
        ("environment", {"type": "shared-prefix"}),
        ("resources", {"big": {"cpus": 8}}),
    ],
)
def test_execution_settings_that_do_not_decide_a_product_keep_the_hash(
    key: str, value: Any
) -> None:
    plan = _plan("A")
    before = provisional_plan_hash(plan)
    plan.execution[key] = value
    assert provisional_plan_hash(plan) == before


def test_partitioning_and_file_access_change_the_hash() -> None:
    plan = _plan("A")
    before = provisional_plan_hash(plan)
    plan.execution["partitioning"] = {"chunk_size": 100}
    chunked = provisional_plan_hash(plan)
    plan.execution["file_access"] = [{"match": "x", "replace": "y"}]
    assert before != chunked != provisional_plan_hash(plan)


def test_global_side_products_are_listed_not_persisted() -> None:
    from hepflow.runtime.persistence.select import (  # noqa: PLC0415
        global_side_product_nodes,
    )

    plan = _plan("A", "B")
    for node, scope in zip(plan.nodes, ("global", "partition"), strict=True):
        node.input_scope = scope  # type: ignore[assignment]
        node.output_scope = scope  # type: ignore[assignment]
    nodes = global_side_product_nodes(plan)

    selection = resolve_persistence(
        [_spec("A"), _spec("B")],
        registry_with(toy_capability()),
        PersistenceSettings(mode="all"),
        global_nodes=nodes,
    )

    assert nodes == {"stage.A"}
    assert list(selection.persisted) == [("stage.B", "hist")]
    assert selection.not_persisted == [
        {
            "node": "stage.A",
            "output": "hist",
            "kind": "histogram",
            "reason": "global side product",
        }
    ]
