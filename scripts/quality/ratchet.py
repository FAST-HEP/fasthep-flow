"""Maintainability ratchet: new dead code, repeated helpers and oversized code fail; old ones are baselined.

Checks (all read-only unless ``--update``):

* ``dead-code``: ``vulture`` (confidence 60) on ``src``; keys are ``path::kind name``.
* ``repeated-helpers``: private functions of one name defined in three or more files, per tree.
* ``budgets``: files over 600 lines, functions over 80 lines or McCabe complexity over 15 (ruff C901).

``baseline.json`` holds today's findings. A check fails on a finding that is not in the baseline or
that grew. ``--update`` rewrites the baseline and refuses growth unless ``--allow-growth`` is given,
which is a human decision made in review.
"""

from __future__ import annotations

import argparse
import ast
import collections
import json
import re
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
BASELINE = Path(__file__).with_name("baseline.json")
FILE_LINES, FUNCTION_LINES, COMPLEXITY, MIN_FILES = 600, 80, 15, 3
VULTURE = re.compile(
    r"^(?P<path>.+?):\d+: unused (?P<kind>\w+) '(?P<name>.+)' \(\d+% confidence\)$"
)
COMPLEX = re.compile(r"`(?P<name>.+)` is too complex \((?P<value>\d+) > ")


def python_files(root: Path, tree: str) -> list[Path]:
    return sorted((root / tree).rglob("*.py"))


def tool(name: str) -> list[str]:
    found = shutil.which(name)
    return [found] if found else [sys.executable, "-m", name]


def dead_code(root: Path) -> dict[str, int]:
    run = subprocess.run(
        [*tool("vulture"), "src", "--min-confidence", "60"],
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
    )
    found = {}
    for line in run.stdout.splitlines():
        if match := VULTURE.match(line):
            found[f"{match['path']}::{match['kind']} {match['name']}"] = 1
    return found


def repeated_helpers(root: Path) -> dict[str, int]:
    found = {}
    for tree in ("src", "tests"):
        files = collections.defaultdict(set)
        for path in python_files(root, tree):
            for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
                is_function = isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
                if (
                    is_function
                    and node.name.startswith("_")
                    and not node.name.startswith("__")
                ):
                    files[node.name].add(path)
        found.update(
            {
                f"{tree}::{name}": len(paths)
                for name, paths in files.items()
                if len(paths) >= MIN_FILES
            }
        )
    return found


def budgets(root: Path) -> dict[str, int]:
    found = {}
    for path in python_files(root, "src"):
        relative = path.relative_to(root)
        source = path.read_text(encoding="utf-8")
        if (lines := source.count("\n") + 1) > FILE_LINES:
            found[f"file::{relative}"] = lines
        for node in ast.walk(ast.parse(source)):
            if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
                if (length := node.end_lineno - node.lineno + 1) > FUNCTION_LINES:
                    found[f"lines::{relative}::{node.name}"] = length
    run = subprocess.run(
        [
            *tool("ruff"),
            "check",
            "src",
            "--isolated",
            "--select",
            "C901",
            "--output-format",
            "json",
            "--config",
            f"lint.mccabe.max-complexity={COMPLEXITY}",
        ],
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
    )
    for item in json.loads(run.stdout or "[]"):
        if match := COMPLEX.search(item["message"]):
            relative = Path(item["filename"]).resolve().relative_to(root)
            found[f"complexity::{relative}::{match['name']}"] = int(match["value"])
    return found


CHECKS = {
    "dead-code": dead_code,
    "repeated-helpers": repeated_helpers,
    "budgets": budgets,
}


def regressions(found: dict[str, int], baseline: dict[str, int]) -> list[str]:
    return [
        f"{key} ({value}{'' if key not in baseline else f' > baseline {baseline[key]}'})"
        for key, value in sorted(found.items())
        if value > baseline.get(key, 0)
    ]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("check", choices=[*CHECKS, "all"])
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--baseline", type=Path, default=BASELINE)
    parser.add_argument(
        "--update", action="store_true", help="rewrite the baseline (refuses growth)"
    )
    parser.add_argument(
        "--allow-growth",
        action="store_true",
        help="with --update: accept new or larger findings",
    )
    args = parser.parse_args(argv)
    root = args.root.resolve()
    saved = json.loads(args.baseline.read_text()) if args.baseline.exists() else {}
    status = 0
    updated = False
    for name in CHECKS if args.check == "all" else [args.check]:
        found = CHECKS[name](root)
        bad = regressions(found, saved.get(name, {}))
        if args.update and (not bad or args.allow_growth):
            saved[name] = dict(sorted(found.items()))
            updated = True
            print(f"{name}: baseline now {len(found)} findings")
        elif bad:
            status = 1
            print(
                f"{name}: {len(bad)} new or grown (baseline {len(saved.get(name, {}))}):"
            )
            print("\n".join(f"  {line}" for line in bad))
        else:
            fixed = len(saved.get(name, {}).keys() - found.keys())
            print(
                f"{name}: ok ({len(found)} baselined"
                + (f", {fixed} fixed: run --update to shrink)" if fixed else ")")
            )
    if updated:
        args.baseline.write_text(json.dumps(saved, indent=2, sort_keys=True) + "\n")
    return status


if __name__ == "__main__":
    raise SystemExit(main())
