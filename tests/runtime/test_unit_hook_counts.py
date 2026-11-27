from __future__ import annotations

import json
import warnings
from pathlib import Path
from typing import Any

import pytest

from hepflow.api import compile_workflow_file, list_workflow_partitions, load_plan_file
from hepflow.compiler.compile_hooks import counting_hook_names
from hepflow.compiler.dataset_units import PublishConflict
from hepflow.compiler.hooks.registry import hook_scope, phase_compile_hook_entries
from hepflow.utils import read_yaml, write_yaml

_TOY = "tests.toy_components.compile_hooks"
_COUNTS = {"toy://a0": 5, "toy://a1": 4}


def _workflow(
    tmp_path: Path,
    toy_workflow: dict[str, Any],
    *,
    hook: dict[str, Any] | None = None,
    alpha: dict[str, Any] | None = None,
    counting: str = "file_counts",
    extra_hooks: dict[str, Any] | None = None,
) -> Path:
    stage = dict(toy_workflow["analysis"]["stages"][0])
    stage.pop("write")
    registry = dict(toy_workflow["registry"])
    registry["compile_hooks"] = dict(extra_hooks or {})
    if hook is not None:
        registry["compile_hooks"][f"toy.{counting}"] = {
            "spec": f"{_TOY}:{counting.upper()}_HOOK_SPEC",
            "impl": f"{_TOY}:{counting}_hook",
            "params": hook,
        }
    workflow = {
        **toy_workflow,
        "registry": registry,
        "analysis": {"stages": [stage]},
        "execution": {"partitioning": {"per_dataset": True}},
        "data": {
            "datasets": [
                {
                    key: value
                    for key, value in {
                        "name": "alpha",
                        "files": ["toy://a0", "toy://a1"],
                        "nevents": 9,
                        "group": "mc",
                        **(alpha or {}),
                    }.items()
                    if value is not None  # a None override removes the key
                },
                {"name": "beta", "files": ["toy://b"], "nevents": 3, "group": "data"},
            ]
        },
    }
    path = tmp_path / "workflow.yaml"
    write_yaml(workflow, path)
    return path


def _unit(out: Path, name: str = "alpha") -> Path:
    return out / "compile" / "datasets" / name


def test_without_a_counting_hook_the_chunk_check_is_unchanged(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    workflow = _workflow(tmp_path, toy_workflow)

    with pytest.raises(ValueError, match="only a dataset-level 'nevents'") as error:
        compile_workflow_file(workflow, outdir=tmp_path / "out", dataset="alpha", chunk_size=4)

    assert "add_per_file" not in str(error.value)


def test_hook_counts_reach_the_manifest_and_chunk_the_partitions(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    workflow = _workflow(tmp_path, toy_workflow, hook={"counts": _COUNTS, "add_per_file": True})
    out = tmp_path / "out"

    compile_workflow_file(workflow, outdir=out, dataset="alpha", chunk_size=4)

    manifest = read_yaml(_unit(out) / "manifest.yaml")
    assert manifest["file_nevents"] == [5, 4]
    assert manifest["count_origin"] == ["hook", "hook"]
    assert manifest["counted"] == [True, True]
    plan = load_plan_file(out / "compile" / "plan.yaml", dataset="alpha")
    assert [(p.part, p.start, p.stop) for p in plan.partitions] == [
        ("1", 0, 4),
        ("2", 4, 5),
        ("3", 0, 4),
    ]
    assert read_yaml(_unit(out) / "report.compile.yaml")["partitions"] == 3


def test_a_counting_hook_without_per_file_entries_fails_naming_the_option(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    workflow = _workflow(tmp_path, toy_workflow, hook={"counts": _COUNTS})

    with pytest.raises(ValueError, match=r"toy\.file_counts.*add_per_file"):
        compile_workflow_file(workflow, outdir=tmp_path / "out", dataset="alpha", chunk_size=4)

    assert not _unit(tmp_path / "out").exists()


def test_uncounted_files_warn_and_suggest_the_hook(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    workflow = _workflow(
        tmp_path,
        toy_workflow,
        hook={"counts": {}, "add_per_file": True},
        alpha={"nevents": None},
    )

    with pytest.warns(UserWarning, match=r"no per-file event counts.*toy\.file_counts"):
        compile_workflow_file(workflow, outdir=tmp_path / "out", dataset="alpha", chunk_size=4)

    manifest = read_yaml(_unit(tmp_path / "out") / "manifest.yaml")
    assert manifest["counted"] == [False, False]


def test_the_hook_sees_only_the_selected_dataset(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    workflow = _workflow(tmp_path, toy_workflow, hook={"counts": _COUNTS, "add_per_file": True})
    out = tmp_path / "out"

    compile_workflow_file(workflow, outdir=out, dataset="alpha", chunk_size=4)

    metadata = json.loads((_unit(out) / "dataset_metadata.json").read_text())
    assert list(metadata["datasets"]) == ["alpha"]


def test_an_identical_recompile_does_not_conflict_on_hook_counts(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    workflow = _workflow(tmp_path, toy_workflow, hook={"counts": _COUNTS, "add_per_file": True})
    out = tmp_path / "out"
    compile_workflow_file(workflow, outdir=out, dataset="alpha", chunk_size=4)
    before = (_unit(out) / "manifest.yaml").read_bytes()

    compile_workflow_file(workflow, outdir=out, dataset="alpha", chunk_size=4)

    assert (_unit(out) / "manifest.yaml").read_bytes() == before


def test_a_changed_declared_count_still_conflicts(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    out = tmp_path / "out"
    first = _workflow(tmp_path, toy_workflow, hook={"counts": _COUNTS, "add_per_file": True})
    compile_workflow_file(first, outdir=out, dataset="alpha", chunk_size=4)
    changed = _workflow(
        tmp_path,
        toy_workflow,
        hook={"counts": _COUNTS, "add_per_file": True},
        alpha={"file_nevents": [5, 5], "nevents": 10},
    )

    with pytest.raises(PublishConflict):
        compile_workflow_file(changed, outdir=out, dataset="alpha", chunk_size=4)


def test_a_declared_count_is_kept_when_the_hook_disagrees(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    workflow = _workflow(
        tmp_path,
        toy_workflow,
        hook={"counts": {"toy://a0": 6, "toy://a1": 4}, "add_per_file": True},
        alpha={"file_nevents": [5, 4], "nevents": 9},
    )

    with pytest.warns(UserWarning, match="declared count is kept"):
        compile_workflow_file(workflow, outdir=tmp_path / "out", dataset="alpha", chunk_size=4)

    manifest = read_yaml(_unit(tmp_path / "out") / "manifest.yaml")
    assert manifest["file_nevents"] == [5, 4]
    assert manifest["count_origin"] == ["declared", "declared"]


def test_listing_agrees_with_the_compiled_partitions(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    workflow = _workflow(tmp_path, toy_workflow, hook={"counts": _COUNTS, "add_per_file": True})
    out = tmp_path / "out"
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        compile_workflow_file(workflow, outdir=out, dataset="alpha", chunk_size=4)

    assert len(load_plan_file(out / "compile" / "plan.yaml", dataset="alpha").partitions) == 3


def test_hook_scope_is_plan_for_parameter_hooks_and_dataset_otherwise() -> None:
    assert hook_scope({"kind": "parameter"}) == "plan"
    assert hook_scope({"kind": "phase"}) == "dataset"
    assert hook_scope({}) == "dataset"


def test_phase_hooks_can_be_selected_by_scope() -> None:
    registry = {
        "compile_hooks": {
            "toy.file_counts": {
                "spec": f"{_TOY}:FILE_COUNTS_HOOK_SPEC",
                "impl": f"{_TOY}:file_counts_hook",
            }
        }
    }

    assert [n for n, _, _ in phase_compile_hook_entries(registry, when="after_datasets")] == [
        "toy.file_counts"
    ]
    assert phase_compile_hook_entries(registry, when="after_datasets", scope="plan") == []
    assert counting_hook_names(registry) == ["toy.file_counts"]
    assert counting_hook_names({}) == []


def test_partial_hook_counts_leave_the_other_files_whole(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    workflow = _workflow(tmp_path, toy_workflow, hook={"counts": {"toy://a0": 5}, "add_per_file": True})
    out = tmp_path / "out"

    with pytest.warns(UserWarning, match="1 of 2 files have no event count"):
        compile_workflow_file(workflow, outdir=out, dataset="alpha", chunk_size=4)

    manifest = read_yaml(_unit(out) / "manifest.yaml")
    assert manifest["file_nevents"] == [5, None]
    assert manifest["counted"] == [True, False]
    assert manifest["count_origin"] == ["hook", None]
    with pytest.warns(UserWarning, match="1 of 2 files have no event count"):
        plan = load_plan_file(out / "compile" / "plan.yaml", dataset="alpha")
    assert [(p.file, p.start, p.stop) for p in plan.partitions] == [
        ("toy://a0", 0, 4),
        ("toy://a0", 4, 5),
        ("toy://a1", None, None),
    ]


def test_the_compile_api_returns_the_partitions_a_job_loads(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    workflow = _workflow(tmp_path, toy_workflow, hook={"counts": _COUNTS, "add_per_file": True})
    out = tmp_path / "out"

    returned = compile_workflow_file(workflow, outdir=out, dataset="alpha", chunk_size=4)

    loaded = load_plan_file(out / "compile" / "plan.yaml", dataset="alpha")
    assert returned.partitions == loaded.partitions
    assert len(returned.partitions) == 3
    assert returned.context["datasets"]["alpha"]["file_nevents"] == [5, 4]


def test_listing_says_that_hook_counts_are_known_only_after_compile(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    workflow = _workflow(tmp_path, toy_workflow, hook={"counts": _COUNTS, "add_per_file": True})

    with pytest.raises(ValueError, match=r"only a dataset-level.*known only after `compile`"):
        list_workflow_partitions(workflow, dataset="alpha", chunk_size=4)


def test_after_compile_hooks_see_only_the_selected_dataset(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    workflow = _workflow(
        tmp_path,
        toy_workflow,
        alpha={"file_nevents": [5, 4], "nevents": 9},
        extra_hooks={
            "toy.seen_datasets": {
                "spec": f"{_TOY}:SEEN_DATASETS_HOOK_SPEC",
                "impl": f"{_TOY}:seen_datasets_hook",
            }
        },
    )
    out = tmp_path / "out"

    compile_workflow_file(workflow, outdir=out, dataset="alpha", chunk_size=4)

    assert json.loads((_unit(out) / "seen_datasets.json").read_text()) == ["alpha"]


def test_the_hint_names_add_per_file_only_for_a_hook_that_has_it(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    workflow = _workflow(tmp_path, toy_workflow, hook={}, counting="plain_counts")

    with pytest.raises(ValueError, match=r"toy\.plain_counts.*returned no per-file counts") as error:
        compile_workflow_file(workflow, outdir=tmp_path / "out", dataset="alpha", chunk_size=4)

    assert "add_per_file" not in str(error.value)
