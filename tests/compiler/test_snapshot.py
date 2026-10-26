from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from tests.bundle_helpers import make_project, read_manifest, rows_by_resolved

from hepflow.api import compile_workflow_file
from hepflow.compiler import snapshot as snapshot_module
from hepflow.compiler.bundle import bundle_id
from hepflow.compiler.snapshot import SNAPSHOT_MARKER, looks_secret


def _compile(entry: Path, out: Path, **kwargs: Any) -> dict[str, Any]:
    compile_workflow_file(entry, outdir=out, root=entry.parent, **kwargs)
    return read_manifest(out / "compile")


def test_snapshot_mirrors_the_root_relative_tree(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    entry = make_project(tmp_path, toy_workflow)
    project = entry.parent
    out = tmp_path / "build"

    manifest = _compile(entry, out, snapshot=True)

    inputs = out / "compile" / "inputs"
    assert (inputs / SNAPSHOT_MARKER).is_file()
    for relative in ("workflow.yaml", "parts/sources.yaml", "data/x.json"):
        copy = inputs / relative
        assert copy.is_file()
        assert not copy.is_symlink()
        assert copy.read_bytes() == (project / relative).read_bytes()
    rows = rows_by_resolved(manifest)
    for relative in ("workflow.yaml", "parts/sources.yaml", "data/x.json"):
        assert rows[str(project / relative)]["copied"] is True


def test_every_row_has_copied(tmp_path: Path, toy_workflow: dict[str, Any]) -> None:
    entry = make_project(tmp_path, toy_workflow)

    without = _compile(entry, tmp_path / "a")
    with_snapshot = _compile(entry, tmp_path / "b", snapshot=True)

    assert without["inputs"]
    assert all(row["copied"] is False for row in without["inputs"])
    assert all("copied" in row for row in with_snapshot["inputs"])
    assert not (tmp_path / "a" / "compile" / "inputs").exists()


def test_snapshot_leaves_normalized_plan_and_bundle_id_unchanged(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    entry = make_project(tmp_path, toy_workflow)
    out_a, out_b = tmp_path / "a", tmp_path / "b"

    first = _compile(entry, out_a)
    second = _compile(entry, out_b, snapshot=True)

    for name in ("normalized.yaml", "plan.yaml"):
        text_a = (out_a / "compile" / name).read_text(encoding="utf-8")
        text_b = (out_b / "compile" / name).read_text(encoding="utf-8")
        assert text_a.replace(str(out_a), "<out>") == text_b.replace(
            str(out_b), "<out>"
        )
        assert "copied" not in text_a
    assert first["plan_hash"] == second["plan_hash"]
    assert first["bundle_id"] == second["bundle_id"]
    assert bundle_id(second) == second["bundle_id"]


def test_file_outside_the_root_is_not_copied(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    outside = tmp_path / "outside" / "y.json"
    entry = make_project(
        tmp_path,
        toy_workflow,
        files={"data/x.json": b"{}", str(outside): b'{"y": 2}'},
        resources={"kfactor": "data/x.json", "remote": str(outside)},
    )
    out = tmp_path / "build"

    manifest = _compile(entry, out, snapshot=True)

    row = rows_by_resolved(manifest)[str(outside)]
    assert row["copied"] is False
    assert row["sha256"]
    assert not list((out / "compile" / "inputs").rglob("y.json"))


def test_cvmfs_file_is_linked_not_copied(
    tmp_path: Path, toy_workflow: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    entry = make_project(tmp_path, toy_workflow)
    project = entry.parent
    monkeypatch.setattr(snapshot_module, "CVMFS_PREFIXES", (str(project / "data"),))
    out = tmp_path / "build"

    manifest = _compile(entry, out, snapshot=True)

    assert (
        rows_by_resolved(manifest)[str(project / "data" / "x.json")]["copied"] is False
    )
    link = out / "compile" / "inputs" / "data" / "x.json"
    assert link.is_symlink()
    assert link.resolve() == (project / "data" / "x.json").resolve()


def test_size_limits(
    tmp_path: Path, toy_workflow: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    entry = make_project(
        tmp_path,
        toy_workflow,
        files={
            "data/small.json": b'{"a": 1}',
            "data/big.json": b"[" + b"1," * 20 + b"1]",
        },
        resources={"small": "data/small.json", "big": "data/big.json"},
    )
    project = entry.parent
    monkeypatch.setattr(snapshot_module, "WARN_LIMIT_BYTES", 4)
    monkeypatch.setattr(snapshot_module, "COPY_LIMIT_BYTES", 16)
    out = tmp_path / "build"

    with pytest.warns(UserWarning, match="snapshot") as caught:
        manifest = _compile(entry, out, snapshot=True)

    messages = [str(item.message) for item in caught]
    rows = rows_by_resolved(manifest)
    inputs = out / "compile" / "inputs"
    assert rows[str(project / "data" / "small.json")]["copied"] is True
    assert any("small.json" in m and "copied" in m for m in messages)
    assert rows[str(project / "data" / "big.json")]["copied"] is False
    assert any("big.json" in m and "not copied" in m for m in messages)
    assert (inputs / "data" / "big.json").is_symlink()
    # The workflow files are larger than the patched limit, too.
    assert rows[str(project / "workflow.yaml")]["copied"] is False


def test_binary_file_is_linked_not_copied(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    entry = make_project(
        tmp_path,
        toy_workflow,
        files={"data/payload.bin": b"\x00\x01\x02binary"},
        resources={"payload": "data/payload.bin"},
    )
    project = entry.parent
    out = tmp_path / "build"

    manifest = _compile(entry, out, snapshot=True)

    assert (
        rows_by_resolved(manifest)[str(project / "data/payload.bin")]["copied"] is False
    )
    link = out / "compile" / "inputs" / "data" / "payload.bin"
    assert link.is_symlink()
    assert link.read_bytes() == b"\x00\x01\x02binary"


@pytest.mark.parametrize(
    ("relative", "content"),
    [
        (".env", b"API_KEY=1\n"),
        ("data/token.txt", b"abc\n"),
        ("data/server.pem", b"cert\n"),
        ("private/settings.json", b"{}"),
        ("data/notes.txt", b"-----BEGIN RSA PRIVATE KEY-----\nMIIE\n"),
    ],
)
def test_secrets_are_never_copied_or_linked(
    tmp_path: Path, toy_workflow: dict[str, Any], relative: str, content: bytes
) -> None:
    entry = make_project(
        tmp_path,
        toy_workflow,
        files={relative: content},
        resources={"secret": relative},
    )
    out = tmp_path / "build"

    with pytest.warns(UserWarning, match="credential"):
        manifest = _compile(entry, out, snapshot=True)

    assert rows_by_resolved(manifest)[str(entry.parent / relative)]["copied"] is False
    target = out / "compile" / "inputs" / relative
    assert not target.exists()
    assert not target.is_symlink()


@pytest.mark.parametrize(
    "path",
    [
        "/p/.ssh/config",
        "/p/id_ed25519.pub",
        "/p/x509up_u1234",
        "/p/.netrc",
        "/p/a/client.KEY",
        "/p/my_secret.yaml",
        "/p/.globus/usercert.pem",
        "/p/prod.env",
    ],
)
def test_looks_secret_by_name(path: str) -> None:
    assert looks_secret(Path(path))


@pytest.mark.parametrize(
    "path", ["/p/data/x.json", "/p/corrections/jec.txt", "/p/environment.yaml"]
)
def test_ordinary_files_do_not_look_secret(path: str) -> None:
    assert not looks_secret(Path(path))


def test_stale_snapshot_is_removed(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    entry = make_project(tmp_path, toy_workflow)
    out = tmp_path / "build"

    _compile(entry, out, snapshot=True)
    assert (out / "compile" / "inputs").is_dir()
    _compile(entry, out)

    assert not (out / "compile" / "inputs").exists()


def test_unmarked_inputs_directory_is_never_removed(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    entry = make_project(tmp_path, toy_workflow)
    out = tmp_path / "build"
    mine = out / "compile" / "inputs" / "mine.txt"
    mine.parent.mkdir(parents=True)
    mine.write_text("keep", encoding="utf-8")

    _compile(entry, out)
    assert mine.read_text(encoding="utf-8") == "keep"
    with pytest.raises(FileExistsError, match="inputs"):
        _compile(entry, out, snapshot=True)
    assert mine.read_text(encoding="utf-8") == "keep"


def test_systematics_snapshot_per_variation(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    entry = make_project(tmp_path, toy_workflow, systematics=True)
    out = tmp_path / "build"

    compile_workflow_file(
        entry, outdir=out, root=entry.parent, chunk_size=5, snapshot=True
    )

    for variation in ("nominal", "trigger_eff_up"):
        compile_dir = out / "compile" / variation
        assert (compile_dir / "inputs" / SNAPSHOT_MARKER).is_file()
        assert (compile_dir / "inputs" / "workflow.yaml").is_file()
        assert (compile_dir / "inputs" / "data" / "x.json").is_file()
        rows = read_manifest(compile_dir)["inputs"]
        assert any(row["copied"] for row in rows)
    assert not (out / "compile" / "inputs").exists()
