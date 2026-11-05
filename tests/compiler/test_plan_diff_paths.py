from __future__ import annotations

import pytest

from hepflow.compiler.plan_diff.paths import (
    PathDisplay,
    display_path,
    is_path_field,
)


def _d(value: str, root: str | None) -> tuple[str, str]:
    shown = display_path(value, root)
    return shown.value, shown.label


def test_display_path_relative_to_root() -> None:
    assert _d("/a/proj/skim/x.yaml", "/a/proj") == ("skim/x.yaml", "relative")
    assert _d("/a/proj", "/a/proj") == (".", "relative")


def test_display_path_sibling_with_shared_prefix_is_external() -> None:
    assert _d("/a/proj2/x", "/a/proj") == ("/a/proj2/x", "external")


def test_display_path_normalises_dotdot_and_trailing_slash() -> None:
    assert _d("/a/proj/../proj/x/", "/a/proj/") == ("x", "relative")


def test_display_path_outside_root_is_external() -> None:
    assert _d("/elsewhere/f.root", "/a/proj") == ("/elsewhere/f.root", "external")
    assert _d("/a/other/../../b", "/a/proj") == ("/b", "external")


@pytest.mark.parametrize(
    "uri", ["root://eos/f.root", "s3://b/k", "file:///a/proj/x", "https://h/p"]
)
def test_display_path_uri_is_untouched(uri: str) -> None:
    assert _d(uri, "/a/proj") == (uri, "uri")


@pytest.mark.parametrize("root", [None, ""])
def test_display_path_without_root_is_labelled_no_root(root: str | None) -> None:
    assert _d("/a/proj/x", root) == ("/a/proj/x", "no_root")


def test_relative_value_stays_relative() -> None:
    assert _d("skim/x.yaml", "/a/proj") == ("skim/x.yaml", "relative")


def test_display_path_never_touches_filesystem(tmp_path) -> None:  # type: ignore[no-untyped-def]
    loop = tmp_path / "loop"
    loop.symlink_to(loop)
    assert _d(f"{tmp_path}/loop/x", str(tmp_path)) == ("loop/x", "relative")
    assert _d("/definitely/not/there/x", "/definitely/not") == ("there/x", "relative")


def test_path_display_to_dict() -> None:
    assert PathDisplay("x", "relative").to_dict() == {"value": "x", "label": "relative"}


@pytest.mark.parametrize(
    "path",
    [
        ("context", "workflow_path"),
        ("context", "project_root"),
        ("provenance", "root"),
        ("provenance", "inputs", "declared"),
        ("provenance", "inputs", "resolved"),
        ("provenance", "registry_layers", "path"),
        ("provenance", "execution_layers", "path"),
        ("provenance", "execution_hook_layers", "path"),
        ("nodes", "meta", "compile_hooks", "anything", "resolved_path"),
    ],
)
def test_path_fields_are_exact_structural_paths(path: tuple[str, ...]) -> None:
    assert is_path_field(path)


@pytest.mark.parametrize(
    "path",
    [
        ("provenance", "inputs", "sha256"),
        ("context", "datasets"),
        ("nodes", "params", "path"),
        ("foo", "path"),
        ("nodes", "meta", "compile_hooks", "p", "q", "resolved_path"),
    ],
)
def test_other_paths_are_not_path_fields(path: tuple[str, ...]) -> None:
    assert not is_path_field(path)
