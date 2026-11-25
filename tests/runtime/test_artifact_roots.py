from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from hepflow.model.io import OutputResult
from hepflow.runtime.context import component_context
from hepflow.runtime.handlers import _run_writer_like_sink, allocate_artifact_root


def _echo_sink(seen: dict[str, Any]) -> Any:
    def sink(target: Any, *, ctx: Any = None, **params: Any) -> OutputResult:
        seen["root"] = ctx.get("artifact_root")
        seen["params"] = dict(params)
        seen["ctx"] = ctx
        return OutputResult(kind="artifact", path=str(params.get("path") or "x"))

    return sink


def _run(tmp_path: Path, params: dict[str, Any], **extra: Any) -> dict[str, Any]:
    seen: dict[str, Any] = {}
    _run_writer_like_sink(
        sink_name="toy",
        impl=_echo_sink(seen),
        target={"x": 1},
        params={"when": "partition_end", **params},
        ctx={
            "outdir": str(tmp_path / "build"),
            "partition": {"id": "p", "dataset": "ds", "part": "0_0"},
            "dataset_name": "ds",
            **extra,
        },
        meta={"node_id": "write.Selected"},
    )
    return seen


def test_writer_root_is_writer_folder_under_files(tmp_path: Path) -> None:
    seen = _run(tmp_path, {"path": "skims/x.root"})

    assert seen["root"] == str(tmp_path / "build" / "artifacts" / "files" / "skims")


def test_token_path_root_is_first_folder(tmp_path: Path) -> None:
    seen = _run(tmp_path, {"path": "{dataset}/{part}.root"})

    assert seen["root"] == str(tmp_path / "build" / "artifacts" / "files" / "ds")


def test_path_directly_below_files_uses_the_family(tmp_path: Path) -> None:
    root = allocate_artifact_root(
        {"path": str(tmp_path / "build" / "artifacts" / "files" / "x.root")},
        ctx={"outdir": str(tmp_path / "build")},
    )

    assert root == tmp_path / "build" / "artifacts" / "files"


def test_path_outside_the_bundle_uses_its_folder(tmp_path: Path) -> None:
    other = tmp_path / "elsewhere" / "x.root"

    seen = _run(tmp_path, {"path": str(other)})

    # partition_end writers get <stem>/<dataset>/<part> below an absolute path too
    assert seen["root"] == str(other.with_suffix("") / "ds")


def test_render_root_is_the_extensionless_output_folder(tmp_path: Path) -> None:
    seen = _run(tmp_path, {"out": "NumberMuons_heatmap.png"})

    plots = tmp_path / "build" / "artifacts" / "plots"
    assert seen["root"] == str(plots / "NumberMuons_heatmap")
    assert seen["params"]["output_path"] == str(plots / "NumberMuons_heatmap.png")
    assert "output_dir" not in seen["params"]


def test_backend_root_moves_only_the_files_family(tmp_path: Path) -> None:
    files = _run(
        tmp_path, {"path": "skims/x.root"}, artifact_root=str(tmp_path / "job")
    )
    plots = _run(tmp_path, {"out": "a.png"}, artifact_root=str(tmp_path / "job"))

    assert files["root"] == str(tmp_path / "job" / "skims")
    assert plots["root"] == str(tmp_path / "build" / "artifacts" / "plots" / "a")


def test_relative_backend_root_resolves_against_cwd_and_does_not_nest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)

    seen = _run(tmp_path, {"path": "skims/x.root"}, artifact_root=".")

    assert seen["root"] == str(tmp_path / "skims")
    assert seen["params"]["path"] == str(tmp_path / "skims" / "x" / "ds" / "0_0.root")


def test_sink_receives_its_own_root_without_nesting_twice(tmp_path: Path) -> None:
    job = tmp_path / "job"

    seen = _run(tmp_path, {"path": "skims/x.root"}, artifact_root=str(job))

    assert seen["root"] == str(job / "skims")
    assert seen["params"]["path"] == str(job / "skims" / "x" / "ds" / "0_0.root")


def test_artifact_path_joins_below_the_root() -> None:
    ctx = component_context({"artifact_root": "/r/w"})

    assert ctx.artifact_path("a/b.png") == Path("/r/w/a/b.png")
    assert ctx.artifact_path(".") == Path("/r/w")


@pytest.mark.parametrize("name", ["../x", "a/../../x", "/abs/x"])
def test_artifact_path_rejects_escape(name: str) -> None:
    with pytest.raises(ValueError, match=r"artifact_root|absolute"):
        component_context({"artifact_root": "/r/w"}).artifact_path(name)


def test_artifact_path_needs_a_root() -> None:
    with pytest.raises(ValueError, match="artifact_root"):
        component_context({}).artifact_path("x")
