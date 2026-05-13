from __future__ import annotations

from pathlib import Path


FORBIDDEN_IMPORTS = (
    "fasthep_carpenter",
    "fasthep_curator",
    "fasthep_render",
    "hepflow.legacy",
)


def test_flow_package_and_tests_do_not_import_split_packages_or_legacy() -> None:
    root = Path(__file__).resolve().parents[1]
    files = [
        *root.joinpath("src", "hepflow").rglob("*.py"),
        *root.joinpath("tests").rglob("*.py"),
    ]
    offenders: list[tuple[str, str]] = []

    for path in files:
        text = path.read_text(encoding="utf-8")
        for needle in FORBIDDEN_IMPORTS:
            if needle in text and path.name != "test_architecture.py":
                offenders.append((str(path.relative_to(root)), needle))

    assert offenders == []
