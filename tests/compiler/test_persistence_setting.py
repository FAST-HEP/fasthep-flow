from __future__ import annotations

import pytest

from hepflow.compiler.execution import normalize_global_execution
from hepflow.model.persistence import PersistenceSettings


def test_mode_off_leaves_the_plan_execution_unchanged() -> None:
    assert "persistence" not in normalize_global_execution({})
    assert "persistence" not in normalize_global_execution(
        {"persistence": {"mode": "off"}}
    )


@pytest.mark.parametrize("mode", ["all", "selected"])
def test_mode_is_recorded_in_the_execution(mode: str) -> None:
    execution = normalize_global_execution({"persistence": {"mode": mode}})
    assert execution["persistence"] == {"mode": mode}


def test_invalid_mode_rejected() -> None:
    with pytest.raises(ValueError, match="mode must be one of"):
        normalize_global_execution({"persistence": {"mode": "sometimes"}})


def test_unknown_key_rejected() -> None:
    with pytest.raises(ValueError, match="unknown keys"):
        normalize_global_execution({"persistence": {"mode": "all", "cache": True}})


def test_product_choice_needs_node_and_output() -> None:
    with pytest.raises(ValueError, match="<node id>:<output name>"):
        normalize_global_execution(
            {"persistence": {"mode": "selected", "products": {"hist": True}}}
        )


def test_root_may_be_a_uri() -> None:
    settings = PersistenceSettings.from_execution(
        {"persistence": {"mode": "all", "root": "root://eos.example//store/p"}}
    )
    assert settings.root == "root://eos.example//store/p"


def test_reuse_is_recorded_and_must_be_boolean() -> None:
    execution = normalize_global_execution(
        {"persistence": {"mode": "all", "reuse": True}}
    )
    assert execution["persistence"] == {"mode": "all", "reuse": True}
    with pytest.raises(ValueError, match="reuse must be a boolean"):
        normalize_global_execution({"persistence": {"mode": "all", "reuse": "yes"}})
