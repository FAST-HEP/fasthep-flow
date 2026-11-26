from __future__ import annotations

import json
import zlib
from pathlib import Path
from typing import Any

from hepflow.model.io import OutputResult
from hepflow.model.plan import ExecutionNode, ExecutionPlan
from hepflow.runtime.engine import _store_node_outputs
from hepflow.runtime.handlers import _run_writer_like_sink_located
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
    """Run the writer and store its result the way the engine does (the funnel)."""
    calls: list[dict[str, Any]] = []
    ctx = {
        "outdir": str(tmp_path / "build"),
        "partition": {"id": "events__ds__0", "dataset": "ds", "part": "0_0"},
        "dataset_name": "ds",
        **extra_ctx,
    }
    result, root = _run_writer_like_sink_located(
        sink_name="fake.root",
        impl=_fake_root_writer(calls),
        target={"x": 1},
        params={"path": path, "when": "partition_end"},
        ctx=ctx,
        meta={"node_id": "write.Selected"},
    )
    store: dict[tuple[str, str], Any] = {}
    _store_node_outputs(
        "write.Selected",
        {"artifact": "artifact"},
        result,
        store,
        ctx=ctx,
        artifact_root=root,
        sink="fake.root",
    )
    stored = store[("write.Selected", "artifact")]
    assert isinstance(stored, OutputResult)
    return stored, calls


def test_writer_writes_below_artifact_root_and_records_the_delivered_uri(
    tmp_path: Path,
) -> None:
    root = tmp_path / "jobdir"

    result, calls = _run(
        tmp_path, {"artifact_root": str(root), "output_destination": DESTINATION}
    )

    assert calls[0]["path"] == str(root / "ds" / "0_0.root")
    assert (root / "ds" / "0_0.root").is_file()
    assert not (tmp_path / "build").exists()
    assert result.path == DESTINATION + "ds/0_0.root"
    assert result.path_type == "uri"
    assert result.size_bytes == 7
    assert result.adler32 == f"{zlib.adler32(b'x' * 7):08x}"


def test_writer_without_artifact_root_keeps_default_layout(tmp_path: Path) -> None:
    result, calls = _run(tmp_path, {}, path="skims/x.root")

    expected = tmp_path / "build" / "artifacts" / "files" / "skims" / "x" / "ds"
    assert calls[0]["path"] == str(expected / "0_0.root")
    assert result.path == "artifacts/files/skims/x/ds/0_0.root"
    assert result.path_type == "relative_to_outdir"
    assert result.adler32 is None


def test_writer_outside_artifact_root_is_recorded_absolute(tmp_path: Path) -> None:
    other = tmp_path / "elsewhere" / "ds" / "0_0.root"

    result, _ = _run(
        tmp_path,
        {"artifact_root": str(tmp_path / "jobdir"), "output_destination": DESTINATION},
        path=str(other),
    )

    assert result.path_type == "absolute"
    assert result.adler32 is not None


def test_relative_artifact_root_resolves_against_working_directory(
    tmp_path: Path, monkeypatch: Any
) -> None:
    monkeypatch.chdir(tmp_path)

    result, calls = _run(
        tmp_path, {"artifact_root": ".", "output_destination": DESTINATION}
    )

    assert calls[0]["path"] == str(tmp_path / "ds" / "0_0.root")
    assert (tmp_path / "ds" / "0_0.root").is_file()
    assert result.path == DESTINATION + "ds/0_0.root"


def test_run_manifest_records_the_uri_and_checksum(tmp_path: Path) -> None:
    root = tmp_path / "jobdir"
    result, _ = _run(
        tmp_path, {"artifact_root": str(root), "output_destination": DESTINATION}
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
    published = (
        tmp_path / "build" / "artifacts" / "files" / "Selected" / "manifest.json"
    )
    payload = json.loads(published.read_text(encoding="utf-8"))
    assert payload["schema_version"] == "3"
    assert "destination" not in payload
    [entry] = payload["datasets"]["ds"]["files"]
    assert entry["path"] == DESTINATION + "ds/0_0.root"
    assert entry["path_type"] == "uri"
    assert entry["adler32"] == f"{zlib.adler32(b'x' * 7):08x}"
    assert "destination" not in entry


def test_writer_with_token_stem_is_named_after_its_node(tmp_path: Path) -> None:
    result, _ = _run(tmp_path, {}, path="{dataset}/{part}.root")

    assert result.metadata["writer_manifest"]["name"] == "Selected"


def test_writer_with_plain_stem_keeps_the_stem_as_name(tmp_path: Path) -> None:
    result, _ = _run(tmp_path, {}, path="skims/hinv_tthmva.root")

    assert result.metadata["writer_manifest"]["name"] == "hinv_tthmva"
