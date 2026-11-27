from __future__ import annotations

import zlib
from pathlib import Path
from typing import Any

import pytest
from tests.builders import write_file

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

    assert (
        _norm("elsewhere/a.png", "out") == (tmp_path / "elsewhere" / "a.png").as_posix()
    )
    assert _norm("/data/a.png", "out") == "/data/a.png"


def test_outdir_relative_input_is_cwd_relative(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A relative input is read from the sink's cwd, never from the outdir."""
    monkeypatch.chdir(tmp_path)

    assert (
        _norm("artifacts/x.png", "out") == (tmp_path / "artifacts" / "x.png").as_posix()
    )


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
    """A working directory whose ``out`` holds the files the funnel tests report."""
    monkeypatch.chdir(tmp_path)
    for name in ("a.png", "b.png"):
        (tmp_path / "out").mkdir(exist_ok=True)
        (tmp_path / "out" / name).write_bytes(b"x")
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
        producer_node="n",
        output_name="artifact",
        dataset_name=None,
        parts=(_part("out/a.png"), _part("out/b.png")),
    )

    out = _store(manifest, "artifact", _ctx("out"))

    assert [part.path for part in out.parts] == ["a.png", "b.png"]


def test_funnel_list_of_two_files_becomes_a_manifest(in_tmp: Path) -> None:
    out = _store([{"path": "out/a.png"}, _part("out/b.png")], "artifact", _ctx("out"))

    assert isinstance(out, ArtifactManifest)
    assert [part.path for part in out.parts] == ["a.png", "b.png"]


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


def test_relative_return_is_read_against_root(in_tmp: Path) -> None:
    root = in_tmp / "out" / "artifacts" / "files" / "w"

    assert (
        artifacts.normalise_location("x/1.root", "out", root=root)
        == "artifacts/files/w/x/1.root"
    )


def test_relative_return_with_relative_root_reads_cwd_for_the_root(
    in_tmp: Path,
) -> None:
    assert (
        artifacts.normalise_location("1.root", "out", root="out/files/w")
        == "files/w/1.root"
    )


def test_absolute_return_ignores_root(in_tmp: Path) -> None:
    outside = in_tmp / "elsewhere" / "a.png"

    assert (
        artifacts.normalise_location(str(outside), "out", root="out/p")
        == outside.as_posix()
    )


def test_stat_sets_size_bytes(in_tmp: Path) -> None:
    write_file(in_tmp / "out" / "a.png", b"12345")
    ref = _part("a.png")

    artifacts.stat_artifacts(
        ref, outdir="out", node_id="n", sink="s", with_adler32=False
    )

    assert ref.size_bytes == 5
    assert ref.adler32 is None


def test_stat_missing_file_names_sink_node_path(in_tmp: Path) -> None:
    (in_tmp / "out").mkdir(exist_ok=True)

    with pytest.raises(artifacts.ArtifactMissingError) as error:
        artifacts.stat_artifacts(
            _part("gone.png"),
            outdir="out",
            node_id="plot.A",
            sink="render.hist",
            with_adler32=False,
        )

    message = str(error.value)
    assert "render.hist" in message
    assert "plot.A" in message
    assert "gone.png" in message


def test_stat_directory_fails_like_missing(in_tmp: Path) -> None:
    (in_tmp / "out" / "folder").mkdir(parents=True)

    with pytest.raises(artifacts.ArtifactMissingError):
        artifacts.stat_artifacts(
            _part("folder"), outdir="out", node_id="n", sink="s", with_adler32=False
        )


def test_stat_skips_uris(in_tmp: Path) -> None:
    ref = _part("root://eos.example//store/a.root")

    artifacts.stat_artifacts(
        ref, outdir="out", node_id="n", sink="s", with_adler32=True
    )

    assert ref.size_bytes is None
    assert ref.adler32 is None


def test_stat_walks_manifests_and_lists(in_tmp: Path) -> None:
    write_file(in_tmp / "out" / "a.png", b"1")
    write_file(in_tmp / "out" / "b.png", b"22")
    manifest = ArtifactManifest(
        product_kind="artifact",
        producer_node="n",
        output_name="artifact",
        dataset_name=None,
        parts=(_part("a.png"),),
    )
    other = _part("b.png")

    artifacts.stat_artifacts(
        [manifest, other], outdir="out", node_id="n", sink="s", with_adler32=False
    )

    assert manifest.parts[0].size_bytes == 1
    assert other.size_bytes == 2


def test_adler32_only_when_requested(in_tmp: Path) -> None:
    write_file(in_tmp / "out" / "a.png", b"hello world")
    ref = _part("a.png")

    artifacts.stat_artifacts(
        ref, outdir="out", node_id="n", sink="s", with_adler32=True
    )

    assert ref.adler32 == f"{zlib.adler32(b'hello world') & 0xFFFFFFFF:08x}"


def _refs(*names: str) -> list[OutputResult]:
    return [
        OutputResult(kind="artifact", path=name, producer_node="n") for name in names
    ]


def test_delivery_status_recorded_on_each_file() -> None:
    refs = _refs("root://eos//a.root", "root://eos//b.root")

    artifacts.apply_delivery_status(
        refs, {"root://eos//a.root": "ok", "root://eos//b.root": "size_mismatch"}
    )

    assert [ref.delivery for ref in refs] == ["ok", "size_mismatch"]


def test_delivery_status_walks_manifests() -> None:
    manifest = ArtifactManifest(
        product_kind="artifact",
        producer_node="n",
        output_name="artifact",
        dataset_name=None,
        parts=tuple(_refs("u://a", "u://b")),
    )

    artifacts.apply_delivery_status(manifest, {"u://a": "ok", "u://b": "missing"})

    assert [part.delivery for part in manifest.parts] == ["ok", "missing"]


def test_delivery_status_for_an_unknown_location_raises() -> None:
    with pytest.raises(KeyError, match="u://nope"):
        artifacts.apply_delivery_status(_refs("u://a"), {"u://nope": "ok"})


def test_delivery_status_rejects_an_unknown_status() -> None:
    with pytest.raises(ValueError, match="delivery"):
        artifacts.apply_delivery_status(_refs("u://a"), {"u://a": "lost"})


def test_failed_delivery_is_recorded_and_does_not_raise() -> None:
    refs = _refs("u://a")

    artifacts.apply_delivery_status(refs, {"u://a": "error"})

    assert refs[0].delivery == "error"


def test_delivery_counts_cover_every_status() -> None:
    refs = _refs("u://a", "u://b", "u://c")
    artifacts.apply_delivery_status(
        refs, {"u://a": "ok", "u://b": "ok", "u://c": "error"}
    )

    assert artifacts.delivery_counts(refs) == {
        "ok": 2,
        "missing": 0,
        "size_mismatch": 0,
        "checksum_mismatch": 0,
        "error": 1,
    }
