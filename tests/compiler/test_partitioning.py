from __future__ import annotations

from typing import Any

import pytest

from hepflow.compiler.partitioning import resolve_chunk_size


@pytest.mark.parametrize(
    ("cli_chunk_size", "dataset", "execution", "expected"),
    [
        (11, {"chunk_size": 22}, {"partitioning": {"chunk_size": 33}}, 11),
        (None, {"chunk_size": 22}, {"partitioning": {"chunk_size": 33}}, 22),
        (None, {}, {"partitioning": {"chunk_size": 33}}, 33),
        (None, {}, {}, None),
    ],
)
def test_chunk_size_precedence(
    cli_chunk_size: int | None,
    dataset: dict[str, Any],
    execution: dict[str, Any],
    expected: int | None,
) -> None:
    assert resolve_chunk_size(cli_chunk_size, dataset, execution) == expected


@pytest.mark.parametrize("chunk_size", [True, 0, -1])
def test_cli_chunk_size_must_be_a_positive_integer(chunk_size: object) -> None:
    with pytest.raises(ValueError, match="chunk size must be a positive integer"):
        resolve_chunk_size(chunk_size, {}, {})  # type: ignore[arg-type]
