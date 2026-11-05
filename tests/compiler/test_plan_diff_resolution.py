from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from hepflow.compiler.plan_diff.errors import (
    AmbiguousPlans,
    InputNotFound,
    InvalidPlan,
    NoPlanFound,
)
from hepflow.compiler.plan_resolution import resolve_plan_input


def _touch(path: Path, text: str = "context: {}\n") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def _variations(root: Path, names: list[str], **extra: Any) -> list[Path]:
    plans = [_touch(root / "compile" / name / "plan.yaml") for name in names]
    summary = {
        "include_nominal": True,
        "variations": [
            {"name": n, "is_nominal": n == "nominal", "plan": f"compile/{n}/plan.yaml"}
            for n in names
        ],
        **extra,
    }
    _touch(root / "compile" / "systematics.yaml", yaml.safe_dump(summary))
    return plans


def _manifest(root: Path, variations: Any) -> None:
    _touch(
        root / "compile" / "systematics.yaml",
        yaml.safe_dump({"variations": variations}),
    )


def test_existing_file_is_returned_as_supplied(tmp_path: Path) -> None:
    path = _touch(tmp_path / "anything.txt", "{{ not yaml")

    assert resolve_plan_input(path) == path


def test_missing_path_is_input_not_found(tmp_path: Path) -> None:
    with pytest.raises(InputNotFound) as info:
        resolve_plan_input(tmp_path / "nope")
    assert info.value.path == tmp_path / "nope"


def test_build_root_resolves_canonical_plan(tmp_path: Path) -> None:
    plan = _touch(tmp_path / "compile" / "plan.yaml")

    assert resolve_plan_input(tmp_path) == plan


def test_compile_directory_resolves_canonical_plan(tmp_path: Path) -> None:
    plan = _touch(tmp_path / "compile" / "plan.yaml")

    assert resolve_plan_input(tmp_path / "compile") == plan


def test_directory_with_no_plan_is_no_plan_found(tmp_path: Path) -> None:
    (tmp_path / "compile").mkdir()
    with pytest.raises(NoPlanFound):
        resolve_plan_input(tmp_path)
    with pytest.raises(NoPlanFound):
        resolve_plan_input(tmp_path / "compile")


def test_directory_that_is_both_build_root_and_compile_directory_is_invalid(
    tmp_path: Path,
) -> None:
    _touch(tmp_path / "compile" / "plan.yaml")
    _touch(tmp_path / "plan.yaml")

    with pytest.raises(InvalidPlan, match="ambiguous layout"):
        resolve_plan_input(tmp_path)


def test_plan_and_systematics_together_are_conflicting_artifacts(
    tmp_path: Path,
) -> None:
    _variations(tmp_path, ["nominal"])
    _touch(tmp_path / "compile" / "plan.yaml")

    with pytest.raises(InvalidPlan, match="conflicting canonical artifacts"):
        resolve_plan_input(tmp_path)


def test_single_declared_variation_plan_is_selected(tmp_path: Path) -> None:
    (plan,) = _variations(tmp_path, ["only"])

    assert resolve_plan_input(tmp_path) == plan
    assert resolve_plan_input(tmp_path / "compile") == plan


def test_multiple_declared_plans_are_ambiguous_and_list_each(tmp_path: Path) -> None:
    plans = _variations(tmp_path, ["nominal", "up"])

    with pytest.raises(AmbiguousPlans) as info:
        resolve_plan_input(tmp_path)

    assert info.value.candidates == (("nominal", plans[0]), ("up", plans[1]))
    message = str(info.value)
    assert "nominal" in message
    assert str(plans[1]) in message
    assert "explicit plan" in message


def test_nominal_is_never_selected_silently(tmp_path: Path) -> None:
    _variations(tmp_path, ["nominal", "up"])
    with pytest.raises(AmbiguousPlans):
        resolve_plan_input(tmp_path / "compile")


@pytest.mark.parametrize(
    "variations",
    [
        "not a list",
        ["not a mapping"],
        [{"plan": "compile/a/plan.yaml"}],
        [{"name": "a"}],
        [{"name": "a", "plan": 3}],
        [{"name": "", "plan": "compile/a/plan.yaml"}],
        [
            {"name": "a", "plan": "compile/a/plan.yaml"},
            {"name": "a", "plan": "compile/b/plan.yaml"},
        ],
        [
            {"name": "a", "plan": "compile/a/plan.yaml"},
            {"name": "b", "plan": "compile/a/plan.yaml"},
        ],
        [{"name": "a", "plan": "../outside/plan.yaml"}],
        [{"name": "a", "plan": "compile/../../outside/plan.yaml"}],
        [{"name": "a", "plan": "/abs/plan.yaml"}],
    ],
)
def test_malformed_systematics_declarations_are_invalid(
    tmp_path: Path, variations: Any
) -> None:
    _touch(tmp_path / "compile" / "a" / "plan.yaml")
    _touch(tmp_path / "compile" / "b" / "plan.yaml")
    _manifest(tmp_path, variations)

    with pytest.raises(InvalidPlan):
        resolve_plan_input(tmp_path)


def test_unparsable_systematics_yaml_is_invalid(tmp_path: Path) -> None:
    _touch(tmp_path / "compile" / "systematics.yaml", "variations: [unclosed\n")

    with pytest.raises(InvalidPlan):
        resolve_plan_input(tmp_path)


def test_missing_declared_plan_is_input_not_found(tmp_path: Path) -> None:
    _manifest(tmp_path, [{"name": "a", "plan": "compile/a/plan.yaml"}])

    with pytest.raises(InputNotFound):
        resolve_plan_input(tmp_path)


def test_zero_variations_is_no_plan_found(tmp_path: Path) -> None:
    _manifest(tmp_path, [])

    with pytest.raises(NoPlanFound):
        resolve_plan_input(tmp_path)


def test_resolver_does_not_search_recursively(tmp_path: Path) -> None:
    _touch(tmp_path / "compile" / "deep" / "er" / "plan.yaml")

    with pytest.raises(NoPlanFound):
        resolve_plan_input(tmp_path)


def test_error_codes() -> None:
    assert InputNotFound("p").code == "input_not_found"
    assert NoPlanFound("p").code == "no_plan_found"
    assert AmbiguousPlans("p", []).code == "ambiguous_plans"
    assert InvalidPlan("m").code == "invalid_plan"


def test_relative_compile_directory_resolves_declared_plans(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (plan,) = _variations(tmp_path, ["only"])
    monkeypatch.chdir(tmp_path / "compile")

    assert resolve_plan_input(".").resolve() == plan.resolve()
    assert resolve_plan_input("..").resolve() == plan.resolve()


def test_non_utf8_systematics_yaml_is_invalid(tmp_path: Path) -> None:
    path = tmp_path / "compile" / "systematics.yaml"
    path.parent.mkdir(parents=True)
    path.write_bytes(b"\xff\xfe\x00bad")

    with pytest.raises(InvalidPlan):
        resolve_plan_input(tmp_path)
