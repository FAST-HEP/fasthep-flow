"""Merge accounting: prove that a run root holds every partition once, under one binding.

``read_partition_runs`` is what a production merge calls before it combines the outputs of a
dataset. It reads ``binding.json`` and the ``run_summary.yaml`` of every partition below a run root
``runs/[<variation>/]<dataset>/`` and checks the exact range ``1..K`` without looking at file
names. Each summary lists the data files it wrote (``outputs``); the reader compares them with the
files on disk, which catches a data file and a summary left by different attempts. The
``attempts/`` directories are not read.
"""

from __future__ import annotations

import json
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import yaml

from hepflow.runtime.run_records import BINDING_FIELDS, Binding

Verify = Literal["none", "size", "adler32"]


class PartitionAccountingError(ValueError):
    """A run root is incomplete or inconsistent; ``problems`` lists every finding."""

    def __init__(self, run_root: Path, problems: list[str]) -> None:
        self.run_root = run_root
        self.problems = problems
        super().__init__(
            f"{run_root}: {len(problems)} problem(s):\n" + "\n".join(f"  - {p}" for p in problems)
        )


@dataclass(frozen=True, slots=True)
class PartitionRecord:
    """One validated partition summary."""

    partition: int
    summary: dict[str, Any]
    outputs: tuple[dict[str, Any], ...]


@dataclass(frozen=True, slots=True)
class PartitionRuns:
    """The validated partitions of a run root, in order, and the numbers still missing."""

    binding: Binding
    partitions_expected: int
    partitions: tuple[PartitionRecord, ...]
    missing: tuple[int, ...]


def read_partition_runs(
    run_root: str | Path,
    *,
    outdir: str | Path | None = None,
    allow_partial: bool = False,
    verify: Verify = "size",
) -> PartitionRuns:
    """Validate a run root against its binding and the range ``1..K``.

    ``outdir`` is the directory the summaries' output paths are relative to; by default the parent
    of the ``runs`` directory above ``run_root``. ``verify`` compares each output file with its
    summary: ``size`` (default), ``adler32`` (reads the file; the summary must hold the checksum,
    which writers record when a backend delivers the files) or ``none`` (no check of the files).
    With ``allow_partial`` missing numbers are returned in ``missing`` instead of raising; every
    other problem still raises :class:`PartitionAccountingError`.
    """
    root = Path(run_root)
    base = _outdir(root, outdir)
    problems: list[str] = []
    try:
        binding_data = json.loads((root / "binding.json").read_text(encoding="utf-8"))
        binding = Binding(**{key: binding_data[key] for key in BINDING_FIELDS})
    except (OSError, ValueError, KeyError, TypeError) as error:
        raise PartitionAccountingError(root, [f"binding.json is not usable: {error}"]) from None

    found: dict[int, dict[str, Any]] = {}
    for directory in sorted(p for p in root.iterdir() if p.is_dir()):
        if not directory.name.isdigit():
            problems.append(f"unexpected directory {directory.name!r}")
            continue
        summary_file = directory / "run_summary.yaml"
        if not summary_file.is_file():
            problems.append(f"partition {directory.name} has no run_summary.yaml (incomplete)")
            continue
        try:
            summary = yaml.safe_load(summary_file.read_text(encoding="utf-8"))
        except (OSError, yaml.YAMLError) as error:
            problems.append(f"partition {directory.name}: summary unreadable ({error})")
            continue
        if not isinstance(summary, dict) or summary.get("partition") != int(directory.name):
            problems.append(f"partition {directory.name}: summary names another partition")
            continue
        found[int(directory.name)] = summary

    totals = {s.get("partitions") for s in found.values()}
    if len(totals) > 1:
        problems.append(f"summaries disagree on the partition count: {sorted(map(str, totals))}")
    total = _total(totals)
    for number, summary in sorted(found.items()):
        problems.extend(_check_summary(number, summary, binding, total, base, verify))

    missing: tuple[int, ...] = ()
    if total is not None:
        missing = tuple(n for n in range(1, total + 1) if n not in found)
        if missing and not allow_partial:
            problems.append(f"missing partitions: {_ranges(missing)} of 1..{total}")
    elif not found:
        problems.append("no partition summaries")
    if problems:
        raise PartitionAccountingError(root, problems)
    records = tuple(
        PartitionRecord(n, found[n], tuple(found[n].get("outputs") or ()))
        for n in sorted(found)
    )
    return PartitionRuns(binding, total or 0, records, missing)


def _total(totals: set[Any]) -> int | None:
    values = [t for t in totals if isinstance(t, int) and t > 0]
    return values[0] if len(values) == 1 else None


def _outdir(root: Path, outdir: str | Path | None) -> Path:
    if outdir is not None:
        return Path(outdir)
    for parent in root.resolve().parents:
        if parent.name == "runs":
            return parent.parent
    raise ValueError(f"{root} is not below a 'runs' directory; pass outdir=")


def _check_summary(
    number: int,
    summary: dict[str, Any],
    binding: Binding,
    total: int | None,
    base: Path,
    verify: Verify,
) -> list[str]:
    where = f"partition {number}"
    problems = [
        f"{where}: {key} is {summary.get(key)!r}, the binding has {getattr(binding, key)!r}"
        for key in BINDING_FIELDS
        if summary.get(key) != getattr(binding, key)
    ]
    if total is not None and number > total:
        problems.append(f"{where} is above the partition count {total}")
    if summary.get("success") is not True:
        problems.append(f"{where}: the run did not succeed")
    for output in summary.get("outputs") or []:
        problems.extend(_check_output(where, output, base, verify))
    return problems


def _check_output(where: str, output: dict[str, Any], base: Path, verify: Verify) -> list[str]:
    if verify == "none" or output.get("path_type") == "uri":
        return []
    path = Path(str(output["path"]))
    path = path if path.is_absolute() else base / path
    if not path.is_file():
        return [f"{where}: output {output['path']} is missing"]
    problems: list[str] = []
    stem = path.stem
    leftovers = sorted(p.name for p in path.parent.glob(f"{stem}.tmp-*{path.suffix}"))
    problems.extend(f"{where}: leftover temporary file {name}" for name in leftovers)
    size = output.get("size_bytes")
    if size is not None and path.stat().st_size != size:
        problems.append(
            f"{where}: output {output['path']} has {path.stat().st_size} bytes, "
            f"the summary says {size}"
        )
    elif verify == "adler32":
        found = _adler32(path)
        if output.get("adler32") is None:
            problems.append(f"{where}: output {output['path']} has no adler32 in the summary")
        elif found != output["adler32"]:
            problems.append(
                f"{where}: output {output['path']} has adler32 {found}, "
                f"the summary says {output['adler32']}"
            )
    return problems


def _adler32(path: Path) -> str:
    value = 1
    with path.open("rb") as handle:
        while chunk := handle.read(1 << 20):
            value = zlib.adler32(chunk, value)
    return f"{value & 0xFFFFFFFF:08x}"


def _ranges(numbers: tuple[int, ...]) -> str:
    return ", ".join(str(n) for n in numbers)
