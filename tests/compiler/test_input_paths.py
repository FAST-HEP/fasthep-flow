from __future__ import annotations

import hashlib
import warnings
from pathlib import Path

import pytest

from hepflow.compiler import input_paths
from hepflow.compiler.input_paths import is_uri, resolve_input_path


def _write(path: Path, content: bytes) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return path


def test_relative_path_resolves_against_root_and_is_hashed(tmp_path: Path) -> None:
    content = b'{"a": 1}\n'
    _write(tmp_path / "data" / "a.json", content)

    result = resolve_input_path("data/a.json", role="resource:x.path", root=tmp_path)

    assert result.role == "resource:x.path"
    assert result.declared == "data/a.json"
    assert result.resolved == str(tmp_path / "data" / "a.json")
    assert result.origin == "root"
    assert result.sha256 == hashlib.sha256(content).hexdigest()
    assert result.size == len(content)
    assert result.note is None


def test_missing_root_relative_path_raises_file_not_found_error(
    tmp_path: Path,
) -> None:
    with pytest.raises(FileNotFoundError) as excinfo:
        resolve_input_path("data/missing.json", role="r", root=tmp_path)

    assert "data/missing.json" in str(excinfo.value)
    assert str(tmp_path) in str(excinfo.value)


def test_absolute_path_is_kept_and_hashed_when_readable(tmp_path: Path) -> None:
    content = b"abc"
    path = _write(tmp_path / "outside" / "b.json", content)
    root = tmp_path / "root"
    root.mkdir()

    result = resolve_input_path(str(path), role="r", root=root)

    assert result.resolved == str(path)
    assert result.origin == "absolute"
    assert result.sha256 == hashlib.sha256(content).hexdigest()


def test_unreadable_absolute_path_warns_and_records_null_hash(
    tmp_path: Path,
) -> None:
    missing = tmp_path / "cvmfs" / "jet_jerc.json.gz"

    with pytest.warns(UserWarning, match="unavailable"):
        result = resolve_input_path(str(missing), role="r", root=tmp_path)

    assert result.resolved == str(missing)
    assert result.origin == "absolute"
    assert result.sha256 is None
    assert result.size is None
    assert result.note is not None
    assert "unavailable" in result.note


@pytest.mark.parametrize(
    "uri",
    ["root://x//store/f.root", "https://h/f", "file:///tmp/f"],
)
def test_uri_is_untouched_and_not_hashed(tmp_path: Path, uri: str) -> None:
    result = resolve_input_path(uri, role="r", root=tmp_path)

    assert result.origin == "uri"
    assert result.resolved == uri
    assert result.declared == uri
    assert result.sha256 is None
    assert result.size is None


def test_is_uri_accepts_scheme_forms_and_rejects_paths() -> None:
    assert is_uri("root://x//store/f.root")
    assert is_uri("https://h/f")
    assert not is_uri("data/a.json")
    assert not is_uri("/abs/a.json")
    assert not is_uri("C:/windows/a.json")
    assert not is_uri("")


def test_tilde_expands_to_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    home = tmp_path / "home"
    _write(home / "x509" / "p.txt", b"proxy")
    monkeypatch.setenv("HOME", str(home))

    result = resolve_input_path("~/x509/p.txt", role="r", root=tmp_path / "root")

    assert result.resolved == str(home / "x509" / "p.txt")
    assert result.origin == "absolute"
    assert result.declared == "~/x509/p.txt"


def test_environment_variables_are_not_expanded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))
    _write(tmp_path / "x", b"x")

    with pytest.raises(FileNotFoundError) as excinfo:
        resolve_input_path("$HOME/x", role="r", root=tmp_path)

    assert "$HOME/x" in str(excinfo.value)


def test_empty_string_raises_value_error(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="empty"):
        resolve_input_path("  ", role="r", root=tmp_path)


def test_directory_resolves_with_null_hash_and_note(tmp_path: Path) -> None:
    (tmp_path / "corrections").mkdir()

    result = resolve_input_path("corrections", role="r", root=tmp_path)

    assert result.resolved == str(tmp_path / "corrections")
    assert result.sha256 is None
    assert result.size is None
    assert result.note == "directory"


def test_falls_back_to_entry_dir_with_deprecation_warning(tmp_path: Path) -> None:
    root = tmp_path / "root"
    entry_dir = tmp_path / "root" / "skim"
    entry_dir.mkdir(parents=True)
    content = b"profile"
    _write(entry_dir / "shared.yaml", content)

    with pytest.warns(FutureWarning, match="entry"):
        result = resolve_input_path(
            "shared.yaml", role="profile", root=root, entry_dir=entry_dir
        )

    assert result.origin == "entry_fallback"
    assert result.resolved == str(entry_dir / "shared.yaml")
    assert result.sha256 == hashlib.sha256(content).hexdigest()


def test_root_wins_over_entry_dir_without_warning(tmp_path: Path) -> None:
    root = tmp_path / "root"
    entry_dir = root / "skim"
    _write(root / "shared.yaml", b"root")
    _write(entry_dir / "shared.yaml", b"entry")

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        result = resolve_input_path(
            "shared.yaml", role="profile", root=root, entry_dir=entry_dir
        )

    assert result.origin == "root"
    assert result.resolved == str(root / "shared.yaml")


def test_dotdot_is_collapsed_without_resolving_symlinks(tmp_path: Path) -> None:
    _write(tmp_path / "data" / "a.json", b"a")
    (tmp_path / "sub").mkdir()

    result = resolve_input_path("sub/../data/a.json", role="r", root=tmp_path)

    assert result.resolved == str(tmp_path / "data" / "a.json")


def test_large_file_is_hashed_in_chunks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    content = b"0123456789"
    _write(tmp_path / "big.bin", content)
    monkeypatch.setattr(input_paths, "CHUNK_SIZE", 4)

    result = resolve_input_path("big.bin", role="r", root=tmp_path)

    assert result.sha256 == hashlib.sha256(content).hexdigest()
    assert result.size == 10
