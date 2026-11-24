"""A transform's execution scope comes from its spec, not from its output kinds."""

from __future__ import annotations

import pytest

from hepflow.compiler.plan import _default_execution_policy


def _policy(scope: str | None) -> tuple[str, str, str]:
    input_scope, output_scope, partitioning, _ = _default_execution_policy(
        role="transform", impl="toy.op", scope=scope, chunk_size=None
    )
    return input_scope, output_scope, partitioning.mode


def test_partition_scope_runs_per_partition() -> None:
    assert _policy("partition") == ("partition", "partition", "dataset")


def test_global_scope_runs_once() -> None:
    assert _policy("global") == ("global", "global", "none")


def test_a_transform_without_a_scope_is_an_error_naming_the_op() -> None:
    with pytest.raises(ValueError, match=r"'toy\.op' has no execution scope"):
        _policy(None)
