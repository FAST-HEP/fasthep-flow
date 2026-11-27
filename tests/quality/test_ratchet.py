from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import ModuleType

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts/quality/ratchet.py"


def load() -> ModuleType:
    spec = importlib.util.spec_from_file_location("ratchet", SCRIPT)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


ratchet = load()


@pytest.fixture
def tree(tmp_path: Path) -> Path:
    for name in ("a", "b", "c"):
        path = tmp_path / "src" / f"{name}.py"
        path.parent.mkdir(exist_ok=True)
        path.write_text("def _drop_none(x):\n    return x\n")
    return tmp_path


def test_regressions_flag_new_and_grown_only() -> None:
    found = {"new": 1, "grown": 3, "same": 2, "smaller": 1}
    baseline = {"grown": 2, "same": 2, "smaller": 4, "gone": 1}
    assert [line.split(" ")[0] for line in ratchet.regressions(found, baseline)] == [
        "grown",
        "new",
    ]


def test_repeated_helpers_needs_three_files(tree: Path) -> None:
    assert ratchet.repeated_helpers(tree) == {"src::_drop_none": 3}
    (tree / "src/c.py").unlink()
    assert ratchet.repeated_helpers(tree) == {}


def test_budgets_flag_long_file_and_function(tree: Path) -> None:
    body = "".join(f"    x{i} = {i}\n" for i in range(ratchet.FUNCTION_LINES))
    (tree / "src/long.py").write_text(f"def big():\n{body}    return 1\n" + "\n" * 600)
    found = ratchet.budgets(tree)
    assert found["file::src/long.py"] > ratchet.FILE_LINES
    assert found["lines::src/long.py::big"] > ratchet.FUNCTION_LINES


def test_main_fails_on_new_finding_and_update_refuses_growth(
    tree: Path, tmp_path: Path
) -> None:
    baseline = tmp_path / "baseline.json"
    args = ["repeated-helpers", "--root", str(tree), "--baseline", str(baseline)]
    assert ratchet.main([*args, "--update"]) == 1
    assert not baseline.exists()
    assert ratchet.main([*args, "--update", "--allow-growth"]) == 0
    assert json.loads(baseline.read_text())["repeated-helpers"] == {
        "src::_drop_none": 3
    }
    assert ratchet.main(args) == 0
    (tree / "tests").mkdir()
    for name in ("x", "y", "z"):
        (tree / "tests" / f"{name}.py").write_text("def _plan():\n    return 1\n")
    assert ratchet.main(args) == 1
