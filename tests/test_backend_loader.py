from __future__ import annotations

from typing import Any

from hepflow.backends.loaders import load_backend, normalize_backend_override
from hepflow.compiler.lower_graph import lower_author_to_graph
from hepflow.compiler.normalize import normalize_author
from hepflow.compiler.plan import build_execution_plan


def test_local_default_backend_loads(toy_author: dict[str, Any]) -> None:
    normalized = normalize_author(toy_author)
    plan = build_execution_plan(
        lower_author_to_graph(normalized),
        registry=normalized["registry"],
    )

    backend = load_backend(plan)

    assert backend.name == "local.default"


def test_dask_local_backend_loads_without_running(toy_author: dict[str, Any]) -> None:
    normalized = normalize_author(toy_author)
    plan = build_execution_plan(
        lower_author_to_graph(normalized),
        registry=normalized["registry"],
        execution={"backend": "dask", "strategy": "local", "config": {}},
    )

    backend = load_backend(plan)

    assert backend.name == "dask.local"


def test_shorthand_backend_override_splits_backend_and_strategy() -> None:
    assert normalize_backend_override("dask.local", None) == {
        "backend": "dask",
        "strategy": "local",
    }
