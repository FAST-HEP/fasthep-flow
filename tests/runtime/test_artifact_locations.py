from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from hepflow.model.io import ArtifactManifest, OutputResult
from hepflow.model.products import OperationResult
from hepflow.runtime import artifacts
from hepflow.runtime.engine import _store_node_outputs


def _norm(uri: str, outdir: str | Path) -> str:
    return artifacts.normalise_location(uri, outdir)


def test_relative_outdir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)

    assert _norm("out/artifacts/plots/X.png", "out") == "artifacts/plots/X.png"


def test_absolute_outdir(tmp_path: Path) -> None:
    outdir = tmp_path / "out"

    assert _norm(str(outdir / "artifacts" / "X.png"), outdir) == "artifacts/X.png"


def test_outdir_with_parent_reference(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "work").mkdir()
    monkeypatch.chdir(tmp_path / "work")

    assert _norm("../sub/out/a.png", "../sub/out") == "a.png"


def test_path_outside_outdir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)

    assert _norm("elsewhere/a.png", "out") == (tmp_path / "elsewhere" / "a.png").as_posix()
    assert _norm("/data/a.png", "out") == "/data/a.png"


def test_outdir_relative_input_is_cwd_relative(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A relative input is read from the sink's cwd, never from the outdir."""
    monkeypatch.chdir(tmp_path)

    assert _norm("artifacts/x.png", "out") == (tmp_path / "artifacts" / "x.png").as_posix()


def test_symlinked_outdir_is_not_resolved(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    real = tmp_path / "real"
    real.mkdir()
    (tmp_path / "link").symlink_to(real)
    monkeypatch.chdir(tmp_path)

    assert _norm("link/a.png", "link") == "a.png"


@pytest.mark.parametrize(
    "uri", ["root://host//eos/a.root", "http://h/a.png", "file:///tmp/a.png"]
)
def test_uri_unchanged(uri: str, tmp_path: Path) -> None:
    assert _norm(uri, tmp_path) == uri


def test_outdir_uri_input_unchanged(tmp_path: Path) -> None:
    assert _norm("a/b.png", "root://host//eos/out") == "a/b.png"


def test_trailing_slash(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)

    assert _norm("out/a.png", "out/") == "a.png"


@pytest.mark.parametrize("uri", ["/data/a.png", "root://host//eos/a.root"])
def test_idempotent_for_absolute_and_uri(uri: str, tmp_path: Path) -> None:
    once = _norm(uri, tmp_path / "out")

    assert _norm(once, tmp_path / "out") == once


def _ctx(outdir: str) -> dict[str, Any]:
    return {"outdir": outdir}


def _store(result: Any, kind: str, ctx: dict[str, Any] | None) -> Any:
    store: dict[tuple[str, str], Any] = {}
    _store_node_outputs("n", {"artifact": kind}, result, store, ctx=ctx)
    return store[("n", "artifact")]


def _part(path: str) -> OutputResult:
    return OutputResult(kind="artifact", path=path, producer_node="n")


@pytest.fixture
def in_tmp(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.chdir(tmp_path)
    return tmp_path


def test_funnel_dict(in_tmp: Path) -> None:
    out = _store({"path": "out/a.png"}, "artifact", _ctx("out"))

    assert out.path == "a.png"


def test_funnel_output_result(in_tmp: Path) -> None:
    out = _store(_part("out/a.png"), "artifact", _ctx("out"))

    assert out.path == "a.png"


def test_funnel_manifest_parts(in_tmp: Path) -> None:
    manifest = ArtifactManifest(
        product_kind="artifact",
        format=None,
        producer_node="n",
        output_name="artifact",
        dataset_name=None,
        parts=(_part("out/a.png"), _part("out/b.png")),
    )

    out = _store(manifest, "artifact", _ctx("out"))

    assert [part.path for part in out.parts] == ["a.png", "b.png"]


def test_funnel_list(in_tmp: Path) -> None:
    out = _store([{"path": "out/a.png"}, _part("out/b.png")], "artifact", _ctx("out"))

    assert [item["path"] if isinstance(item, dict) else item.path for item in out] == [
        "a.png",
        "b.png",
    ]


def test_funnel_operation_result_product(in_tmp: Path) -> None:
    result = OperationResult(products={"artifact": _part("out/a.png")}, product_refs=[])

    out = _store(result, "artifact", _ctx("out"))

    assert out.path == "a.png"


def test_funnel_skips_without_outdir(in_tmp: Path) -> None:
    assert _store(_part("out/a.png"), "artifact", None).path == "out/a.png"
    assert _store(_part("out/a.png"), "artifact", {}).path == "out/a.png"


def test_funnel_skips_other_kinds(in_tmp: Path) -> None:
    value = _part("out/a.png")

    assert _store(value, "histogram", _ctx("out")).path == "out/a.png"
