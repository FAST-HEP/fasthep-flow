"""Small input-handling cases left from fasthep-flow#86."""

from __future__ import annotations

import os
import warnings
from pathlib import Path
from typing import Any

import pytest
import yaml
from tests.runtime.test_per_dataset_run import _write_workflow

from hepflow.api import compile_workflow_file, make_plan_file
from hepflow.compiler.hooks.load_mapping import load_mapping
from hepflow.compiler.hooks.model import ParamCompileHookContext
from hepflow.compiler.input_paths import resolve_input_path


def _load(tmp_path: Path, value: str) -> Any:
    context = ParamCompileHookContext(workflow_dir=str(tmp_path), project_root=str(tmp_path))
    return load_mapping(value=value, options={"formats": ["yaml"]}, context=context)


def test_load_mapping_names_a_directory(tmp_path: Path) -> None:
    (tmp_path / "maps").mkdir()

    with pytest.raises(ValueError, match="'maps' is a directory"):
        _load(tmp_path, "maps")
    with pytest.raises(ValueError, match="is a directory"):
        _load(tmp_path, str(tmp_path / "maps"))


@pytest.mark.parametrize("uri", ["root://eos/x.yaml", "https://example.org/y.yaml"])
def test_load_mapping_rejects_a_uri_with_its_own_message(tmp_path: Path, uri: str) -> None:
    with pytest.raises(ValueError, match=r"is a URI .*local path"):
        _load(tmp_path, uri)


def test_a_fifo_is_recorded_without_opening_it(tmp_path: Path) -> None:
    fifo = tmp_path / "pipe"
    os.mkfifo(fifo)

    for declared in ("pipe", str(fifo)):
        entry = resolve_input_path(declared, role="x", root=tmp_path)

        assert entry.sha256 is None
        assert entry.note == "not a regular file: fifo"


def test_a_device_is_recorded_without_a_hash(tmp_path: Path) -> None:
    entry = resolve_input_path("/dev/null", role="x", root=tmp_path)

    assert (entry.sha256, entry.note) == (None, "not a regular file: device")


@pytest.mark.skipif(os.geteuid() == 0, reason="root can read any file")
def test_an_unreadable_file_under_the_root_names_role_and_path(tmp_path: Path) -> None:
    secret = tmp_path / "secret.yaml"
    secret.write_text("a: 1\n")
    secret.chmod(0)

    try:
        with pytest.raises(PermissionError, match=r"x: input 'secret.yaml' is not readable"):
            resolve_input_path("secret.yaml", role="x", root=tmp_path)
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            entry = resolve_input_path(str(secret), role="x", root=tmp_path)
        assert entry.sha256 is None
        assert entry.note is not None
        assert entry.note.startswith("unavailable")
        assert caught
        assert caught[0].filename == __file__
    finally:
        secret.chmod(0o644)


def test_make_plan_file_rejects_an_old_normalized_file(
    tmp_path: Path, toy_workflow: dict[str, Any]
) -> None:
    workflow = _write_workflow(tmp_path, toy_workflow, per_dataset=False)
    out = tmp_path / "out"
    compile_workflow_file(workflow, outdir=out)
    normalized = yaml.safe_load((out / "compile" / "normalized.yaml").read_text())
    old = tmp_path / "old.yaml"
    old.write_text(yaml.safe_dump({k: v for k, v in normalized.items() if k != "registry"}))
    empty = tmp_path / "empty.yaml"
    empty.write_text("")

    with pytest.raises(ValueError, match=r"old.yaml is not a normalized workflow"):
        make_plan_file(old, outdir=tmp_path / "o1")
    with pytest.raises(ValueError, match="missing analysis, registry"):
        make_plan_file(empty, outdir=tmp_path / "o2")
    make_plan_file(out / "compile" / "normalized.yaml", outdir=tmp_path / "o3")
