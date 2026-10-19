from __future__ import annotations

from pathlib import Path
from typing import Any

from hepflow.model.io import OutputResult
from hepflow.runtime.handlers import _run_writer_like_sink


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


def test_writer_with_token_stem_is_named_after_its_node(tmp_path: Path) -> None:
    result, _ = _run(tmp_path, {}, path="{dataset}/{part}.root")

    assert result.metadata["writer_manifest"]["name"] == "Selected"


def test_writer_with_plain_stem_keeps_the_stem_as_name(tmp_path: Path) -> None:
    result, _ = _run(tmp_path, {}, path="skims/hinv_tthmva.root")

    assert result.metadata["writer_manifest"]["name"] == "hinv_tthmva"
