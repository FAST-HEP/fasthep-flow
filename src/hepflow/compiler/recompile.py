"""Recompile a compile bundle from its snapshot and compare the result (fasthep-flow#94).

A bundle is a compile directory holding ``inputs.manifest.yaml`` and the snapshot
``inputs/`` (written with ``snapshot=True``). Recompiling runs the compile hooks again; no
hook result is replayed. The steps are split so that :func:`hepflow.api.compile_from_bundle`
can run the compile itself:

1. :func:`load_bundle` reads the manifest and checks that the snapshot exists;
2. :func:`verify_bundle` checks the sha256 of every file the snapshot does not hold a copy
   of (outside the root, under ``/cvmfs``, over the size cap, binary);
3. the caller compiles ``bundle.workflow_path`` with ``root=bundle.snapshot_root``;
4. :func:`compare_bundle` compares the new manifest with the recorded one.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Literal

import yaml
from fasthep_toolbench.misc import file_sha256

from hepflow.build_layout import BuildPaths, output_variation_from_context
from hepflow.compiler.bundle import bundle_id, replace_root
from hepflow.compiler.inputs_manifest import MANIFEST_NAME
from hepflow.compiler.snapshot import SNAPSHOT_MARKER, snapshot_dir

CheckStatus = Literal["ok", "changed", "unavailable"]
ArtifactStatus = Literal["changed", "added", "missing"]


@dataclass(frozen=True)
class FileCheck:
    """The verification of one file that the snapshot does not hold a copy of."""

    role: str
    path: str
    status: CheckStatus
    expected_sha256: str
    actual_sha256: str | None = None
    note: str | None = None

    @property
    def message(self) -> str:
        if self.status == "changed":
            return (
                f"{self.role}: {self.path} changed since the compile (sha256 differs)"
            )
        if self.status == "unavailable":
            return f"{self.role}: {self.path} is unavailable ({self.note})"
        return f"{self.role}: {self.path} verified"


@dataclass(frozen=True)
class HookArtifactChange:
    """A compile hook artifact whose content differs between the two compiles.

    The comparison is made after replacing the snapshot root by the recorded root in the
    new artifact; the sha256 values are those of the files as written.
    """

    hook: str
    artifact: str
    status: ArtifactStatus
    recorded_sha256: str | None
    recompiled_sha256: str | None


@dataclass(frozen=True)
class BundleReport:
    """What :func:`hepflow.api.compile_from_bundle` found.

    ``plan_hash_match`` is the comparison that matters: ``plan_hash`` does not depend on the
    project root. ``bundle_id`` does, so ``rebased_bundle_id`` is the new bundle's id
    computed with the snapshot root replaced by the recorded root (in the manifest and in
    ``software.json``); ``bundle_id_match`` compares it with the recorded id.
    """

    bundle_dir: str
    outdir: str
    compile_dir: str
    recorded_root: str
    snapshot_root: str
    recorded_plan_hash: str | None
    recompiled_plan_hash: str | None
    recorded_bundle_id: str | None
    recompiled_bundle_id: str | None
    rebased_bundle_id: str | None
    checks: list[FileCheck] = field(default_factory=list)
    hook_artifacts: list[HookArtifactChange] = field(default_factory=list)

    @property
    def plan_hash_match(self) -> bool:
        return (
            self.recorded_plan_hash is not None
            and self.recorded_plan_hash == self.recompiled_plan_hash
        )

    @property
    def bundle_id_match(self) -> bool:
        return (
            self.recorded_bundle_id is not None
            and self.recorded_bundle_id == self.rebased_bundle_id
        )

    @property
    def warnings(self) -> list[str]:
        """One message per file that is unavailable or changed."""
        return [check.message for check in self.checks if check.status != "ok"]

    @property
    def reproduced(self) -> bool:
        """Same ``plan_hash``, every file verified, no hook artifact changed."""
        return self.plan_hash_match and not self.warnings and not self.hook_artifacts

    def to_dict(self) -> dict[str, Any]:
        """A plain mapping (for YAML or JSON output)."""
        return {
            "bundle_dir": self.bundle_dir,
            "outdir": self.outdir,
            "compile_dir": self.compile_dir,
            "recorded_root": self.recorded_root,
            "snapshot_root": self.snapshot_root,
            "reproduced": self.reproduced,
            "plan_hash": {
                "recorded": self.recorded_plan_hash,
                "recompiled": self.recompiled_plan_hash,
                "match": self.plan_hash_match,
            },
            "bundle_id": {
                "recorded": self.recorded_bundle_id,
                "recompiled": self.recompiled_bundle_id,
                "rebased": self.rebased_bundle_id,
                "match": self.bundle_id_match,
            },
            "warnings": self.warnings,
            "checks": [asdict(check) for check in self.checks],
            "hook_artifacts": [asdict(change) for change in self.hook_artifacts],
        }


class BundleVerificationError(ValueError):
    """Raised by a strict recompile when a non-copied file is unavailable or changed."""

    def __init__(self, checks: list[FileCheck]) -> None:
        self.checks = checks
        problems = [check.message for check in checks if check.status != "ok"]
        super().__init__(
            "bundle verification failed:\n" + "\n".join(f"- {p}" for p in problems)
        )


@dataclass(frozen=True)
class Bundle:
    """A compile bundle read from disk."""

    compile_dir: Path
    manifest: dict[str, Any]
    snapshot_root: Path
    recorded_root: str
    workflow_path: Path
    chunk_size: int | None
    variation: str | None


def load_bundle(bundle_dir: str | Path) -> Bundle:
    """Read the bundle in ``bundle_dir``: a compile directory, or a build directory whose
    ``compile/`` holds the manifest."""
    directory = Path(bundle_dir)
    if (
        not (directory / MANIFEST_NAME).is_file()
        and (directory / "compile" / MANIFEST_NAME).is_file()
    ):
        directory = directory / "compile"
    manifest_path = directory / MANIFEST_NAME
    if not manifest_path.is_file():
        raise FileNotFoundError(f"no {MANIFEST_NAME} in {bundle_dir}")
    manifest = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(manifest, dict) or manifest.get("version") != 2:
        raise ValueError(f"{manifest_path} is not a version 2 compile manifest")
    snapshot_root = snapshot_dir(directory)
    if not (snapshot_root / SNAPSHOT_MARKER).is_file():
        raise ValueError(
            f"bundle {directory} has no snapshot; compile it with snapshot=True"
        )
    recorded_root = manifest.get("root")
    if not isinstance(recorded_root, str) or not recorded_root:
        raise ValueError(f"{manifest_path} records no project root")
    workflow_rows = [
        row for row in manifest.get("inputs") or [] if row.get("role") == "workflow"
    ]
    if not workflow_rows:
        raise ValueError(f"{manifest_path} records no workflow file")
    invocation = manifest.get("invocation") or {}
    chunk_size = invocation.get("chunk_size")
    return Bundle(
        compile_dir=directory,
        manifest=manifest,
        snapshot_root=snapshot_root,
        recorded_root=recorded_root,
        workflow_path=_snapshot_path(
            str(workflow_rows[0]["resolved"]), recorded_root, snapshot_root
        ),
        chunk_size=chunk_size if isinstance(chunk_size, int) else None,
        variation=_variation(directory),
    )


def check_outdir(bundle: Bundle, outdir: str | Path) -> None:
    """Refuse an output directory that overlaps the bundle (it would rewrite the snapshot)."""
    out = Path(outdir).resolve()
    bundle_path = bundle.compile_dir.resolve()
    if bundle_path.is_relative_to(out) or out.is_relative_to(bundle_path):
        raise ValueError(
            f"outdir {outdir} contains the bundle {bundle.compile_dir} or lies inside "
            "it; recompile into a separate directory"
        )


def verify_bundle(bundle: Bundle) -> list[FileCheck]:
    """Check the sha256 of every hashed file the snapshot does not hold a copy of."""
    checks: list[FileCheck] = []
    seen: set[str] = set()
    for row in bundle.manifest.get("inputs") or []:
        expected = row.get("sha256")
        resolved = row.get("resolved")
        if row.get("copied") or not isinstance(expected, str) or resolved in seen:
            continue
        if row.get("origin") == "uri" or not isinstance(resolved, str):
            continue
        seen.add(resolved)
        role = str(row.get("role"))
        try:
            actual = file_sha256(resolved)
        except OSError as exc:
            reason = exc.strerror or type(exc).__name__
            checks.append(
                FileCheck(role, resolved, "unavailable", expected, None, reason)
            )
            continue
        status: CheckStatus = "ok" if actual == expected else "changed"
        checks.append(FileCheck(role, resolved, status, expected, actual))
    return checks


def compare_bundle(
    bundle: Bundle, outdir: str | Path, checks: list[FileCheck]
) -> BundleReport:
    """Compare the manifest of the recompile in ``outdir`` with the recorded one."""
    new_compile_dir = BuildPaths(
        root=Path(outdir), variation=bundle.variation
    ).compile_dir()
    new = yaml.safe_load((new_compile_dir / MANIFEST_NAME).read_text(encoding="utf-8"))
    recorded = bundle.manifest
    snapshot_root = str(bundle.snapshot_root)
    return BundleReport(
        bundle_dir=str(bundle.compile_dir),
        outdir=str(outdir),
        compile_dir=str(new_compile_dir),
        recorded_root=bundle.recorded_root,
        snapshot_root=snapshot_root,
        recorded_plan_hash=_plan_sha(recorded),
        recompiled_plan_hash=_plan_sha(new),
        recorded_bundle_id=recorded.get("bundle_id"),
        recompiled_bundle_id=new.get("bundle_id"),
        rebased_bundle_id=_rebased_bundle_id(
            new, new_compile_dir, snapshot_root, bundle.recorded_root
        ),
        checks=checks,
        hook_artifacts=_hook_artifact_changes(
            recorded, new, new_compile_dir, snapshot_root, bundle.recorded_root
        ),
    )


def _snapshot_path(resolved: str, recorded_root: str, snapshot_root: Path) -> Path:
    """Where the snapshot holds ``resolved``; the recorded path for a file outside the root."""
    try:
        relative = Path(resolved).relative_to(recorded_root)
    except ValueError:
        return Path(resolved)
    candidate = snapshot_root / relative
    return candidate if candidate.exists() else Path(resolved)


def _variation(compile_dir: Path) -> str | None:
    plan_file = compile_dir / "plan.yaml"
    if not plan_file.is_file():
        return None
    plan = yaml.safe_load(plan_file.read_text(encoding="utf-8")) or {}
    context = plan.get("context") if isinstance(plan, dict) else None
    return output_variation_from_context(context if isinstance(context, dict) else None)


def _plan_sha(manifest: Mapping[str, Any]) -> str | None:
    value = manifest.get("plan_hash")
    sha = value.get("sha256") if isinstance(value, Mapping) else None
    return sha if isinstance(sha, str) else None


def _rebase(value: Any, old_root: str, new_root: str) -> Any:
    if isinstance(value, str):
        return replace_root(value, old_root, new_root)
    if isinstance(value, Mapping):
        return {key: _rebase(item, old_root, new_root) for key, item in value.items()}
    if isinstance(value, list):
        return [_rebase(item, old_root, new_root) for item in value]
    return value


def _rebased_bytes(path: Path, old_root: str, new_root: str) -> bytes | None:
    """The file's text with ``old_root`` replaced by ``new_root`` (``None`` if unreadable)."""
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return None
    return replace_root(text, old_root, new_root).encode("utf-8")


def _rebased_sha(path: Path, old_root: str, new_root: str) -> str | None:
    data = _rebased_bytes(path, old_root, new_root)
    return hashlib.sha256(data).hexdigest() if data is not None else None


def _rebased_bundle_id(
    new: Mapping[str, Any], compile_dir: Path, snapshot_root: str, recorded_root: str
) -> str:
    rebased = _rebase(dict(new), snapshot_root, recorded_root)
    software = rebased.get("software")
    if isinstance(software, dict) and isinstance(software.get("path"), str):
        data = _rebased_bytes(
            compile_dir / software["path"], snapshot_root, recorded_root
        )
        if data is not None:
            software["sha256"] = hashlib.sha256(data).hexdigest()
            software["size"] = len(data)
    return bundle_id(rebased)


def _artifact_hashes(
    manifest: Mapping[str, Any],
) -> dict[tuple[str, str], dict[str, Any]]:
    found: dict[tuple[str, str], dict[str, Any]] = {}
    for record in manifest.get("hooks") or []:
        artifacts = record.get("artifacts") if isinstance(record, Mapping) else None
        for name, entry in dict(artifacts or {}).items():
            if isinstance(entry, Mapping):
                found[(str(record.get("name")), str(name))] = dict(entry)
    return found


def _hook_artifact_changes(
    recorded: Mapping[str, Any],
    new: Mapping[str, Any],
    compile_dir: Path,
    snapshot_root: str,
    recorded_root: str,
) -> list[HookArtifactChange]:
    before = _artifact_hashes(recorded)
    after = _artifact_hashes(new)
    changes: list[HookArtifactChange] = []
    for key in sorted(before.keys() | after.keys()):
        old_sha = before.get(key, {}).get("sha256")
        entry = after.get(key)
        if entry is None:
            changes.append(HookArtifactChange(*key, "missing", old_sha, None))
            continue
        new_sha = entry.get("sha256")
        if key not in before:
            changes.append(HookArtifactChange(*key, "added", None, new_sha))
            continue
        path = entry.get("path")
        comparable = (
            _rebased_sha(compile_dir / path, snapshot_root, recorded_root)
            if isinstance(path, str)
            else new_sha
        )
        if comparable != old_sha:
            changes.append(HookArtifactChange(*key, "changed", old_sha, new_sha))
    return changes
