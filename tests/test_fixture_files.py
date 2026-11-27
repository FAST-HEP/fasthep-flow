"""The toy workflow fixture is the workflow file plus the toy registry."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

FIXTURES = Path(__file__).parent / "fixtures"


def test_the_workflow_file_does_not_carry_a_registry() -> None:
    document = yaml.safe_load(
        (FIXTURES / "workflows" / "toy_workflow.yaml").read_text()
    )

    assert "registry" not in document


def test_the_toy_workflow_fixture_combines_the_file_and_the_registry(
    toy_workflow: dict[str, Any], toy_registry: dict[str, Any]
) -> None:
    document = yaml.safe_load(
        (FIXTURES / "workflows" / "toy_workflow.yaml").read_text()
    )

    assert toy_workflow == {**document, "registry": toy_registry}
    assert list(toy_workflow)[:2] == ["version", "registry"]
    assert toy_workflow["version"] == "1.0"


def test_each_use_of_the_fixture_gets_its_own_copy(
    toy_workflow: dict[str, Any],
) -> None:
    toy_workflow["analysis"]["stages"][0]["params"]["factor"] = 99

    assert (
        yaml.safe_load(
            (FIXTURES / "workflows" / "toy_workflow.yaml").read_text(encoding="utf-8")
        )["analysis"]["stages"][0]["params"]["factor"]
        == 2
    )
