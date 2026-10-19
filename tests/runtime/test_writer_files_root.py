from __future__ import annotations

import json
import zlib
from pathlib import Path
from typing import Any

from hepflow.model.io import OutputResult
from hepflow.model.plan import ExecutionNode, ExecutionPlan
from hepflow.runtime.handlers import _run_writer_like_sink
from hepflow.runtime.writer_manifests import write_writer_manifests

DESTINATION = "root://eosuser.cern.ch//eos/user/x/y/skim/"


def _fake_root_writer(calls: list[dict[str, Any]]) -> Any:
    """A writer that records the path it is given and reports it like carpenter."""

    def writer(
        target: Any, *, path: str, ctx: Any = None, meta: Any = None
    ) -> OutputResult:
        output = Path(path)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_bytes(b"x" * 7)
        calls.append({"path": path})
        partition = dict(ctx.get("partition") or {})
        manifest = {
            "kind": "root_tree",
            "name": meta.get("writer_name") or Path(path).stem,
            "node_id": "write.Selected",
            "input_node": "stage.Selected",
            "tree": "Events",
            "format": "ttree",
            # What carpenter records when the file is outside outdir.
            "path": str(output.resolve()),
            "path_type": "absolute",
            "dataset": partition.get("dataset"),
            "partition": 0,
            "attempt": 0,
            "entries": 3,
            "size_bytes": 7,
        }
        return OutputResult(
            kind="artifact",
            path=str(output),
            format="root",
            producer_node="write.Selected",
            output_name="artifact",
            dataset_name=partition.get("dataset"),
            partition_id=partition.get("id"),
            partition_index=0,
            metadata={"writer_manifest": manifest},
        )

    return writer


def _run(
    tmp_path: Path, extra_ctx: dict[str, Any], path: str = "{dataset}/{part}.root"
) -> tuple[OutputResult, list[dict[str, Any]]]:
    calls: list[dict[str, Any]] = []
    result = _run_writer_like_sink(
        sink_name="fake.root",
        impl=_fake_root_writer(calls),
        target={"x": 1},
        params={"path": path, "when": "partition_end"},
        ctx={
            "outdir": str(tmp_path / "build"),
            "partition": {"id": "events__ds__0", "dataset": "ds", "part": "0_0"},
            "dataset_name": "ds",
            **extra_ctx,
        },
        meta={"node_id": "write.Selected"},
    )
    assert isinstance(result, OutputResult)
    return result, calls


def test_writer_writes_below_output_files_root_and_records_relative_path(
    tmp_path: Path,
) -> None:
    root = tmp_path / "jobdir"

    result, calls = _run(
        tmp_path, {"output_files_root": str(root), "output_destination": DESTINATION}
    )

    assert calls[0]["path"] == str(root / "ds" / "0_0.root")
    assert (root / "ds" / "0_0.root").is_file()
    assert not (tmp_path / "build").exists()
    manifest = result.metadata["writer_manifest"]
    assert manifest["path"] == "ds/0_0.root"
    assert manifest["path_type"] == "relative_to_files_root"
    assert manifest["destination"] == DESTINATION
    assert manifest["adler32"] == f"{zlib.adler32(b'x' * 7):08x}"


def test_writer_without_files_root_keeps_default_layout(tmp_path: Path) -> None:
    result, calls = _run(tmp_path, {}, path="skims/x.root")

    expected = tmp_path / "build" / "artifacts" / "files" / "skims" / "x" / "ds"
    assert calls[0]["path"] == str(expected / "0_0.root")
    manifest = result.metadata["writer_manifest"]
    assert manifest["path_type"] == "absolute"
    assert "destination" not in manifest
    assert "adler32" not in manifest


def test_writer_outside_files_root_keeps_its_manifest(tmp_path: Path) -> None:
    other = tmp_path / "elsewhere" / "ds" / "0_0.root"

    result, _ = _run(
        tmp_path,
        {"output_files_root": str(tmp_path / "jobdir")},
        path=str(other),
    )

    manifest = result.metadata["writer_manifest"]
    assert manifest["path_type"] == "absolute"
    assert "destination" not in manifest


def test_relative_output_files_root_resolves_against_working_directory(
    tmp_path: Path, monkeypatch: Any
) -> None:
    monkeypatch.chdir(tmp_path)

    result, calls = _run(tmp_path, {"output_files_root": "."})

    assert calls[0]["path"] == "ds/0_0.root"
    assert (tmp_path / "ds" / "0_0.root").is_file()
    assert result.metadata["writer_manifest"]["path"] == "ds/0_0.root"


def test_run_manifest_records_destination_and_relative_paths(tmp_path: Path) -> None:
    root = tmp_path / "jobdir"
    result, _ = _run(
        tmp_path, {"output_files_root": str(root), "output_destination": DESTINATION}
    )
    plan = ExecutionPlan()
    plan.add_node(
        ExecutionNode(
            id="write.Selected",
            graph_node_id="write.Selected",
            role="sink",
            impl="root_tree",
            outputs={"artifact": "artifact"},
        )
    )

    write_writer_manifests(
        plan,
        stores=[{("write.Selected", "artifact"): result}],
        outdir=tmp_path / "build",
    )

    # Named after the node because the path stem is only a token.
    published = tmp_path / "build" / "artifacts" / "files" / "Selected" / "manifest.json"
    payload = json.loads(published.read_text(encoding="utf-8"))
    assert payload["destination"] == DESTINATION
    [entry] = payload["datasets"]["ds"]["files"]
    assert entry["path"] == "ds/0_0.root"
    assert entry["path_type"] == "relative_to_files_root"
    assert entry["adler32"] == f"{zlib.adler32(b'x' * 7):08x}"
    assert "destination" not in entry


def test_writer_with_token_stem_is_named_after_its_node(tmp_path: Path) -> None:
    result, _ = _run(tmp_path, {}, path="{dataset}/{part}.root")

    assert result.metadata["writer_manifest"]["name"] == "Selected"


def test_writer_with_plain_stem_keeps_the_stem_as_name(tmp_path: Path) -> None:
    result, _ = _run(tmp_path, {}, path="skims/hinv_tthmva.root")

    assert result.metadata["writer_manifest"]["name"] == "hinv_tthmva"
