from __future__ import annotations

import shutil
import zlib
from pathlib import Path
from typing import Any

import pytest
import yaml
from tests.runtime.test_partition_records import _compile_with_writer

from hepflow.api import PartitionAccountingError, read_partition_runs, run_plan_file


def _run(tmp_path: Path, toy_workflow: dict[str, Any], numbers: list[int] | None = None) -> Path:
    out, plan = _compile_with_writer(tmp_path, toy_workflow)
    run_plan_file(plan, outdir=out, dataset="beta", partition_numbers=numbers)
    return out


def _summary_file(out: Path, number: int) -> Path:
    return out / "runs" / "beta" / str(number) / "run_summary.yaml"


def _file(out: Path, number: int) -> Path:
    return out / "artifacts" / "files" / "skims" / "part" / "beta" / f"{number}.toy"


def test_summaries_list_their_output_files(tmp_path: Path, toy_workflow: dict[str, Any]) -> None:
    out = _run(tmp_path, toy_workflow)

    summary = yaml.safe_load(_summary_file(out, 2).read_text())

    assert [o["path"] for o in summary["outputs"]] == ["artifacts/files/skims/part/beta/2.toy"]
    assert summary["outputs"][0]["size_bytes"] == _file(out, 2).stat().st_size


def test_a_complete_run_validates(tmp_path: Path, toy_workflow: dict[str, Any]) -> None:
    out = _run(tmp_path, toy_workflow)

    runs = read_partition_runs(out / "runs" / "beta")

    assert runs.partitions_expected == 3
    assert [p.partition for p in runs.partitions] == [1, 2, 3]
    assert runs.missing == ()
    assert runs.binding.chunk_size == 1


def test_a_removed_summary_reports_the_missing_number(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    out = _run(tmp_path, toy_workflow)
    _summary_file(out, 2).unlink()

    with pytest.raises(PartitionAccountingError, match=r"partition 2 has no run_summary"):
        read_partition_runs(out / "runs" / "beta")


def test_a_missing_number_is_returned_when_partial_is_allowed(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    out = _run(tmp_path, toy_workflow, [1, 3])

    with pytest.raises(PartitionAccountingError, match=r"missing partitions: 2 of 1\.\.3"):
        read_partition_runs(out / "runs" / "beta")
    runs = read_partition_runs(out / "runs" / "beta", allow_partial=True)

    assert [p.partition for p in runs.partitions] == [1, 3]
    assert runs.missing == (2,)


def test_an_extra_partition_fails_even_when_partial_is_allowed(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    out = _run(tmp_path, toy_workflow)
    shutil.copytree(out / "runs" / "beta" / "3", out / "runs" / "beta" / "4")
    path = _summary_file(out, 4)
    path.write_text(path.read_text().replace("partition: 3", "partition: 4"))

    with pytest.raises(PartitionAccountingError, match="above the partition count 3"):
        read_partition_runs(out / "runs" / "beta", allow_partial=True)


def test_a_summary_of_another_manifest_fails(tmp_path: Path, toy_workflow: dict[str, Any]) -> None:
    out = _run(tmp_path, toy_workflow)
    path = _summary_file(out, 1)
    summary = yaml.safe_load(path.read_text())
    summary["manifest_hash"] = "0" * 64
    path.write_text(yaml.safe_dump(summary))

    with pytest.raises(PartitionAccountingError, match="partition 1: manifest_hash"):
        read_partition_runs(out / "runs" / "beta")


def test_a_failed_partition_fails(tmp_path: Path, toy_workflow: dict[str, Any]) -> None:
    out = _run(tmp_path, toy_workflow)
    path = _summary_file(out, 1)
    path.write_text(path.read_text().replace("success: true", "success: false"))

    with pytest.raises(PartitionAccountingError, match="partition 1: the run did not succeed"):
        read_partition_runs(out / "runs" / "beta")


def test_an_output_that_differs_from_its_summary_is_named(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    out = _run(tmp_path, toy_workflow)
    _file(out, 2).write_bytes(b"from another attempt, longer than before")
    _file(out, 3).unlink()

    with pytest.raises(PartitionAccountingError) as raised:
        read_partition_runs(out / "runs" / "beta")

    assert any("2.toy has" in p and "bytes" in p for p in raised.value.problems)
    assert any("3.toy is missing" in p for p in raised.value.problems)
    read_partition_runs(out / "runs" / "beta", verify="none")


def test_adler32_catches_a_changed_file_of_the_same_size(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    out = _run(tmp_path, toy_workflow)
    with pytest.raises(PartitionAccountingError, match="no adler32 in the summary"):
        read_partition_runs(out / "runs" / "beta", verify="adler32")
    for number in (1, 2, 3):
        path = _summary_file(out, number)
        summary = yaml.safe_load(path.read_text())
        summary["outputs"][0]["adler32"] = f"{zlib.adler32(_file(out, number).read_bytes()):08x}"
        path.write_text(yaml.safe_dump(summary))
    read_partition_runs(out / "runs" / "beta", verify="adler32")
    data = bytearray(_file(out, 1).read_bytes())
    data[0] = (data[0] + 1) % 256
    _file(out, 1).write_bytes(bytes(data))

    read_partition_runs(out / "runs" / "beta")
    with pytest.raises(PartitionAccountingError, match="adler32"):
        read_partition_runs(out / "runs" / "beta", verify="adler32")


def test_a_leftover_temporary_file_is_reported(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    out = _run(tmp_path, toy_workflow)
    _file(out, 2).with_name("2.tmp-2026-11-19T140322Z-a3f9.toy").write_bytes(b"x")

    with pytest.raises(PartitionAccountingError, match="leftover temporary file"):
        read_partition_runs(out / "runs" / "beta")


def test_a_run_without_data_files_is_complete(tmp_path: Path, toy_workflow: dict[str, Any]) -> None:
    from tests.runtime.test_partition_records import _compile  # noqa: PLC0415

    out, plan = _compile(tmp_path, toy_workflow, "beta")
    run_plan_file(plan, outdir=out, dataset="beta")

    runs = read_partition_runs(out / "runs" / "beta")

    assert [p.outputs for p in runs.partitions] == [(), (), ()]


def test_a_directory_outside_runs_needs_an_outdir(tmp_path: Path) -> None:
    (tmp_path / "x").mkdir()
    (tmp_path / "x" / "binding.json").write_text(
        '{"variation": null, "plan_hash": "a", "manifest_hash": "b", "chunk_size": 1}'
    )

    with pytest.raises(ValueError, match="pass outdir"):
        read_partition_runs(tmp_path / "x")
