from __future__ import annotations

import multiprocessing
import re
from pathlib import Path

import pytest
import yaml

from hepflow.build_layout import BuildPaths
from hepflow.compiler.dataset_units import PublishConflict, dataset_manifest
from hepflow.runtime.run_records import (
    build_binding,
    claim_attempt_dir,
    new_attempt_id,
    partition_summary,
    publish_binding,
    write_partition_summary,
)


def _plan(tmp_path: Path, marker: str = "a") -> Path:
    plan = tmp_path / "plan.yaml"
    plan.write_text(
        yaml.safe_dump({"marker": marker, "partitions": []}), encoding="utf-8"
    )
    return plan


def _manifest(chunk_size: int | None = 2, files: tuple[str, ...] = ("a.root",)) -> dict:
    record = {
        "name": "alpha",
        "files": list(files),
        "nevents": 4,
        "file_nevents": [4] * len(files),
    }
    return dataset_manifest(record, chunk_size=chunk_size)


def test_paths_of_a_run_root_follow_the_variation(tmp_path: Path) -> None:
    nominal = BuildPaths(root=tmp_path)
    varied = BuildPaths(root=tmp_path, variation="up")

    assert nominal.run_root("alpha") == tmp_path / "runs" / "alpha"
    assert (
        varied.partition_run_dir("alpha", 3) == tmp_path / "runs" / "up" / "alpha" / "3"
    )
    assert (
        varied.binding_file("alpha")
        == tmp_path / "runs" / "up" / "alpha" / "binding.json"
    )
    with pytest.raises(ValueError, match="cannot name a run directory"):
        nominal.run_root("../x")


def test_records_of_a_partition_run_live_below_its_directory(tmp_path: Path) -> None:
    run_dir = tmp_path / "runs" / "alpha" / "1"
    paths = BuildPaths(root=tmp_path, run_dir=run_dir)

    assert paths.provenance_manifest() == run_dir / "provenance" / "manifest.json"
    assert paths.run_summary() == run_dir / "run_summary.yaml"
    assert paths.writer_manifest_dir("skim") == run_dir / "writers" / "skim"
    assert paths.artifact("files", "x") == tmp_path / "artifacts" / "files" / "x"


def test_binding_is_created_once_and_accepts_the_same_binding(tmp_path: Path) -> None:
    binding = build_binding(_plan(tmp_path), manifest=_manifest(), variation=None)
    target = tmp_path / "runs" / "alpha" / "binding.json"

    assert publish_binding(target, binding) is True
    assert publish_binding(target, binding) is False
    assert sorted(p.name for p in target.parent.iterdir()) == ["binding.json"]


def test_binding_fails_naming_the_field_when_only_the_plan_changed(
    tmp_path: Path,
) -> None:
    target = tmp_path / "binding.json"
    publish_binding(
        target,
        build_binding(_plan(tmp_path, "a"), manifest=_manifest(), variation=None),
    )

    other = build_binding(_plan(tmp_path, "b"), manifest=_manifest(), variation=None)
    with pytest.raises(PublishConflict, match="plan_hash"):
        publish_binding(target, other)


@pytest.mark.parametrize(
    ("manifest", "field"),
    [
        (_manifest(chunk_size=3), "chunk_size"),
        (_manifest(files=("b.root",)), "manifest_hash"),
    ],
)
def test_binding_fails_on_another_manifest_or_chunk_size(
    tmp_path: Path, manifest: dict, field: str
) -> None:
    plan = _plan(tmp_path)
    target = tmp_path / "binding.json"
    publish_binding(target, build_binding(plan, manifest=_manifest(), variation=None))

    with pytest.raises(PublishConflict, match=field):
        publish_binding(target, build_binding(plan, manifest=manifest, variation=None))


def test_binding_fails_for_another_variation(tmp_path: Path) -> None:
    plan = _plan(tmp_path)
    target = tmp_path / "binding.json"
    publish_binding(target, build_binding(plan, manifest=_manifest(), variation=None))

    with pytest.raises(PublishConflict, match="variation"):
        publish_binding(
            target, build_binding(plan, manifest=_manifest(), variation="up")
        )


def _publish_in_process(args: tuple[str, str]) -> bool:
    plan, target = args
    binding = build_binding(Path(plan), manifest=_manifest(), variation=None)
    return publish_binding(target, binding)


def test_parallel_jobs_create_one_binding(tmp_path: Path) -> None:
    plan = _plan(tmp_path)
    target = tmp_path / "runs" / "alpha" / "binding.json"

    with multiprocessing.get_context("fork").Pool(6) as pool:
        created = pool.map(_publish_in_process, [(str(plan), str(target))] * 12)

    assert created.count(True) == 1
    assert sorted(p.name for p in target.parent.iterdir()) == ["binding.json"]


def test_attempt_ids_are_utc_and_unique(tmp_path: Path) -> None:
    assert re.fullmatch(r"\d{4}-\d\d-\d\dT\d{6}Z-[0-9a-f]{4}", new_attempt_id())

    ids = {claim_attempt_dir(tmp_path / "attempts")[0] for _ in range(50)}

    assert len(ids) == 50
    assert len(list((tmp_path / "attempts").iterdir())) == 50


def test_partition_summary_is_replaced_atomically_by_a_rerun(tmp_path: Path) -> None:
    run_dir = tmp_path / "runs" / "alpha" / "1"
    paths = BuildPaths(root=tmp_path, run_dir=run_dir)
    binding = build_binding(_plan(tmp_path), manifest=_manifest(), variation=None)

    for attempt in ("a1", "a2"):
        summary = partition_summary(
            dataset="alpha",
            partition="1",
            partitions=2,
            binding=binding,
            attempt_id=attempt,
            run={"success": True},
        )
        write_partition_summary(paths, summary)

    written = yaml.safe_load(paths.run_summary().read_text(encoding="utf-8"))
    assert written["attempt"] == "a2"
    assert (written["dataset"], written["partition"], written["partitions"]) == (
        "alpha",
        1,
        2,
    )
    assert written["plan_hash"] == binding.plan_hash
    assert [p.name for p in run_dir.iterdir()] == ["run_summary.yaml"]
