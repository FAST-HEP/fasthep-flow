from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
import sysconfig
import tarfile
import tempfile
from collections import Counter
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from importlib import metadata
from pathlib import Path
from time import monotonic
from typing import Any

from hepflow.build_layout import BuildPaths
from hepflow.model.worker_environment import worker_environment_plan_from_execution
from hepflow.utils import write_json

DISTRIBUTED_PREPARATION_PHASE = "Preparing distributed execution"


@dataclass(slots=True, frozen=True)
class PackedPixiEnvironmentSpec:
    environment: str
    archive_path: str
    worker_env_dir: str = "worker-env"
    type: str = "packed-pixi"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True, frozen=True)
class WorkerCredential:
    type: str
    source_path: Path
    target_name: str
    env_var: str


@dataclass(slots=True, frozen=True)
class WorkerEnvironment:
    python: str
    prologue: list[str]
    transfer_files: list[Path]
    credentials: list[WorkerCredential] = field(default_factory=list)


@dataclass(slots=True, frozen=True)
class PreparedWorkerEnvironment:
    python: str
    bootstrap_commands: list[str]
    transfer_files: list[Path]
    env: dict[str, str]
    environment_manifest: Path
    environment_archive: Path | None = None
    bootstrap_script: Path | None = None
    editable_snapshot_archive: Path | None = None


@dataclass(slots=True, frozen=True)
class EditableDistribution:
    name: str
    version: str
    source_path: Path


@dataclass(slots=True, frozen=True)
class EditableSnapshot:
    archive: Path
    manifest_entries: list[dict[str, Any]]


@dataclass(slots=True, frozen=True)
class StagedExecutionFiles:
    python: str
    bootstrap_commands: list[str]
    transfer_files: list[Path]
    env: dict[str, str]
    manifest: Path


class DistributedPreparationProgress:
    def __init__(self, reporter: Any | None = None) -> None:
        self._reporter = reporter
        self._started = monotonic()
        self.active_step: str | None = None

    def step(self, step: str, **detail: Any) -> None:
        self.active_step = step
        if self._reporter is None:
            return
        self._reporter.phase_started(
            DISTRIBUTED_PREPARATION_PHASE,
            detail={"step": step, **_safe_progress_detail(detail)},
        )

    def complete(
        self,
        *,
        prepared: PreparedWorkerEnvironment | None,
        staged: StagedExecutionFiles | None,
        staging_mode: str,
    ) -> None:
        if self._reporter is None:
            return
        detail = {
            "status": "completed",
            "elapsed_seconds": monotonic() - self._started,
            "staging_mode": staging_mode,
            **distributed_preparation_size_summary(prepared, staged),
        }
        self._reporter.phase_completed(
            DISTRIBUTED_PREPARATION_PHASE,
            detail=detail,
        )

    def fail(self, exc: BaseException) -> None:
        if self._reporter is None:
            return
        self._reporter.phase_completed(
            DISTRIBUTED_PREPARATION_PHASE,
            detail={
                "status": "failed",
                "active_step": self.active_step,
                "elapsed_seconds": monotonic() - self._started,
                "exception_type": type(exc).__name__,
            },
        )


def pack_pixi_environment(
    output_file: Path,
    *,
    environment: str,
) -> Path:
    output_file = output_file.resolve()
    output_file.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [
            "pixi",
            "pack",
            "--environment",
            environment,
            "--ignore-pypi-non-wheel",
            "--create-executable",
            "--output-file",
            str(output_file),
        ],
        check=True,
    )
    return output_file


def prepare_worker_environment(
    execution: Mapping[str, Any],
    *,
    build_paths: BuildPaths,
    progress: DistributedPreparationProgress | None = None,
) -> PreparedWorkerEnvironment | None:
    plan = worker_environment_plan_from_execution(execution)
    if plan is None:
        return None
    if plan.type != "packed-pixi":
        return None
    if plan.mode != "prefix":
        spec = packed_pixi_environment_spec_from_execution(execution)
        if spec is None:
            return None
        archive_path = Path(spec.archive_path)
        if not archive_path.is_absolute():
            archive_path = build_paths.root / archive_path
        legacy = build_packed_pixi_worker_environment(
            PackedPixiEnvironmentSpec(
                environment=spec.environment,
                archive_path=str(archive_path),
                worker_env_dir=spec.worker_env_dir,
            )
        )
        return PreparedWorkerEnvironment(
            python=legacy.python,
            bootstrap_commands=legacy.prologue,
            transfer_files=legacy.transfer_files,
            env={},
            environment_manifest=archive_path.with_suffix(".json"),
        )

    prefix = resolve_pixi_prefix(plan.source or "current")
    environment_id = _environment_id(plan.source or "current", prefix)
    environment_dir = build_paths.worker_environments_dir() / environment_id
    environment_dir.mkdir(parents=True, exist_ok=True)
    archive_path = environment_dir / "prefix.tar.gz"
    bootstrap_path = environment_dir / "bootstrap.sh"
    manifest_path = environment_dir / "manifest.json"

    editables = discover_editable_distributions(prefix)
    required_imports = required_fasthep_imports(prefix, editables)
    base_imports = ["distributed", "hepflow"]
    verify_source_prefix_imports(prefix, required_imports=base_imports)
    staging_mode = _staging_mode(execution)

    editable_snapshot: EditableSnapshot | None = None
    shared_pythonpath: list[Path] = []
    if editables and staging_mode == "transfer":
        if progress is not None:
            progress.step(
                "resolving_editable_snapshots",
                staging_mode=staging_mode,
                editable_count=len(editables),
            )
        editable_snapshot = build_editable_snapshot(
            editables,
            output_file=environment_dir / "editable-snapshot.tar.gz",
        )
        environment_id = _environment_id(
            plan.source or "current",
            prefix,
            editable_snapshot.archive,
        )
        final_environment_dir = build_paths.worker_environments_dir() / environment_id
        if final_environment_dir != environment_dir:
            if final_environment_dir.exists():
                shutil.rmtree(final_environment_dir)
            shutil.move(str(environment_dir), final_environment_dir)
            environment_dir = final_environment_dir
            archive_path = environment_dir / "prefix.tar.gz"
            bootstrap_path = environment_dir / "bootstrap.sh"
            manifest_path = environment_dir / "manifest.json"
            editable_snapshot = EditableSnapshot(
                archive=environment_dir / "editable-snapshot.tar.gz",
                manifest_entries=_editable_manifest_entries(
                    editables,
                    snapshot_record=_file_record(
                        environment_dir / "editable-snapshot.tar.gz"
                    ),
                ),
            )
    elif editables:
        shared_pythonpath = [item.source_path for item in editables]

    if progress is not None:
        progress.step(
            "packing_worker_environment",
            staging_mode=staging_mode,
            editable_count=len(editables),
            editable_snapshot=editable_snapshot is not None,
        )
    pack_relocatable_prefix(prefix, archive_path)
    validate_packed_prefix_archive(
        archive_path,
        editable_snapshot_archive=editable_snapshot.archive if editable_snapshot else None,
        shared_pythonpath=shared_pythonpath,
        required_imports=required_imports,
    )

    write_bootstrap_script(
        bootstrap_path,
        has_editable_snapshot=editable_snapshot is not None,
        shared_pythonpath=shared_pythonpath,
        required_imports=required_imports,
    )
    bootstrap_path.chmod(0o755)

    environment_manifest = {
        "type": "packed-pixi",
        "mode": "prefix",
        "source": plan.source or "current",
        "resolved_prefix": str(prefix),
        "packages": installed_package_records(prefix),
        "archive": _file_record(archive_path),
        "editable_snapshot": _file_record(editable_snapshot.archive)
        if editable_snapshot
        else None,
        "editables": editable_snapshot.manifest_entries if editable_snapshot else [
            {
                "name": item.name,
                "version": item.version,
                "source_path": str(item.source_path),
                "snapshot": None,
                "vcs": vcs_state(item.source_path),
            }
            for item in editables
        ],
        "environment_id": environment_id,
        "bootstrap": _file_record(bootstrap_path),
        "worker_python": "./worker-env/bin/python",
        "required_imports": required_imports,
    }
    write_json(environment_manifest, manifest_path)

    return PreparedWorkerEnvironment(
        python="./worker-env/bin/python",
        bootstrap_commands=_shared_bootstrap_commands(
            bootstrap_path=bootstrap_path,
            archive_path=archive_path,
            editable_snapshot_path=editable_snapshot.archive
            if editable_snapshot
            else None,
            shared_pythonpath=shared_pythonpath,
        ),
        transfer_files=[],
        env={},
        environment_manifest=manifest_path,
        environment_archive=archive_path,
        bootstrap_script=bootstrap_path,
        editable_snapshot_archive=editable_snapshot.archive if editable_snapshot else None,
    )


def resolve_pixi_prefix(source: str = "current") -> Path:
    if not source.strip():
        raise ValueError("Pixi environment source must be non-empty")
    if source.strip() != "current":
        raise ValueError(
            "Packed Pixi prefix environments currently support only source='current'"
        )
    prefix = Path(sys.prefix)
    if not prefix.exists():
        raise FileNotFoundError(f"Current Python prefix does not exist: {prefix}")
    python = prefix / "bin" / "python"
    if not python.exists():
        raise FileNotFoundError(f"Current Python prefix has no Python: {python}")
    return prefix.resolve()


def _staging_mode(execution: Mapping[str, Any]) -> str:
    staging = execution.get("staging")
    if not isinstance(staging, Mapping):
        return "shared"
    mode = str(staging.get("mode") or "shared")
    if mode not in {"shared", "transfer"}:
        raise ValueError("execution.staging.mode must be 'shared' or 'transfer'")
    return mode


def verify_source_prefix_imports(
    prefix: Path,
    *,
    required_imports: list[str] | None = None,
) -> None:
    python = prefix / "bin" / "python"
    imports = list(required_imports or ["distributed", "hepflow"])
    try:
        subprocess.run(
            [str(python), "-c", _import_smoke_code(imports)],
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as exc:
        details = (exc.stderr or exc.stdout or "").strip()
        suffix = f": {details}" if details else ""
        raise RuntimeError(
            f"Current Python prefix failed worker import preflight for {imports}{suffix}"
        ) from exc


def validate_packed_prefix_archive(
    archive_path: Path,
    *,
    editable_snapshot_archive: Path | None = None,
    shared_pythonpath: list[Path] | None = None,
    required_imports: list[str] | None = None,
) -> None:
    imports = list(required_imports or ["distributed", "hepflow"])
    with tempfile.TemporaryDirectory(prefix="fasthep-worker-env-") as tmp:
        target = Path(tmp) / "worker-env"
        target.mkdir()
        snapshot_dir = Path(tmp) / "editable-snapshot"
        try:
            with tarfile.open(archive_path, "r:gz") as archive:
                archive.extractall(target)
            subprocess.run(
                [str(target / "bin" / "conda-unpack")],
                check=True,
                capture_output=True,
                text=True,
            )
            pythonpath = [str(path) for path in shared_pythonpath or []]
            if editable_snapshot_archive is not None:
                snapshot_dir.mkdir()
                with tarfile.open(editable_snapshot_archive, "r:gz") as archive:
                    archive.extractall(snapshot_dir)
                pythonpath.insert(0, str(snapshot_dir))
            env = dict(os.environ)
            if pythonpath:
                env["PYTHONPATH"] = os.pathsep.join(
                    [*pythonpath, *([env["PYTHONPATH"]] if env.get("PYTHONPATH") else [])]
                )
            subprocess.run(
                [str(target / "bin" / "python"), "-c", _import_smoke_code(imports)],
                check=True,
                capture_output=True,
                text=True,
                env=env,
            )
        except (OSError, tarfile.TarError, subprocess.CalledProcessError) as exc:
            details = ""
            if isinstance(exc, subprocess.CalledProcessError):
                details = (exc.stderr or exc.stdout or "").strip()
            suffix = f": {details}" if details else ""
            raise RuntimeError(
                "Packed Pixi prefix failed local unpack/import preflight"
                f" for {imports}{suffix}"
            ) from exc


def prepare_staged_execution_files(
    prepared: PreparedWorkerEnvironment | None,
    *,
    build_paths: BuildPaths,
    staging: Mapping[str, Any] | None = None,
) -> StagedExecutionFiles | None:
    if prepared is None:
        return None
    mode = str(dict(staging or {}).get("mode") or "shared")
    if mode not in {"shared", "transfer"}:
        raise ValueError("execution.staging.mode must be 'shared' or 'transfer'")
    if mode == "shared":
        return StagedExecutionFiles(
            python=prepared.python,
            bootstrap_commands=list(prepared.bootstrap_commands),
            transfer_files=[],
            env=dict(prepared.env),
            manifest=prepared.environment_manifest,
        )
    return prepare_transfer_staging(prepared, build_paths=build_paths)


def prepare_transfer_staging(
    prepared: PreparedWorkerEnvironment,
    *,
    build_paths: BuildPaths,
) -> StagedExecutionFiles:
    staging_dir = build_paths.staging_dir()
    if staging_dir.exists():
        shutil.rmtree(staging_dir)
    staging_dir.mkdir(parents=True, exist_ok=True)

    compile_archive = staging_dir / "compile.tar.gz"
    environment_archive = staging_dir / "prefix.tar.gz"
    editable_snapshot = staging_dir / "editable-snapshot.tar.gz"
    bootstrap_script = staging_dir / "bootstrap.sh"
    manifest_path = staging_dir / "manifest.json"

    build_compile_bundle(build_paths.compile_dir(), compile_archive)
    if prepared.environment_archive is None or prepared.bootstrap_script is None:
        raise ValueError("Prepared worker environment is missing environment artifacts")
    shutil.copy2(prepared.environment_archive, environment_archive)
    shutil.copy2(prepared.bootstrap_script, bootstrap_script)
    transfer_files = [compile_archive, environment_archive, bootstrap_script]

    editable_snapshot_manifest: dict[str, Any] | None = None
    if prepared.editable_snapshot_archive is not None:
        shutil.copy2(prepared.editable_snapshot_archive, editable_snapshot)
        transfer_files.append(editable_snapshot)
        editable_snapshot_manifest = _file_record(editable_snapshot)

    verify_transfer_files(transfer_files)
    manifest = {
        "mode": "transfer",
        "compile": _file_record(compile_archive),
        "environment": _file_record(environment_archive),
        "bootstrap": _file_record(bootstrap_script),
        "editable_snapshot": editable_snapshot_manifest,
        "transfer_files": [
            _directory_record(path) if path.is_dir() else _file_record(path)
            for path in transfer_files
        ],
    }
    write_json(manifest, manifest_path)
    return StagedExecutionFiles(
        python="./worker-env/bin/python",
        bootstrap_commands=[
            "set -e",
            "mkdir -p compile",
            "tar -xzf compile.tar.gz -C compile",
            "FASTHEP_ENV_ARCHIVE=prefix.tar.gz",
            (
                "FASTHEP_EDITABLE_SNAPSHOT=editable-snapshot.tar.gz"
                if editable_snapshot_manifest is not None
                else "unset FASTHEP_EDITABLE_SNAPSHOT"
            ),
            ". ./bootstrap.sh",
        ],
        transfer_files=transfer_files,
        env={},
        manifest=manifest_path,
    )


def distributed_preparation_size_summary(
    prepared: PreparedWorkerEnvironment | None,
    staged: StagedExecutionFiles | None,
) -> dict[str, Any]:
    worker_environment: dict[str, int] = {}
    if prepared is not None and prepared.environment_archive is not None:
        worker_environment["prefix_archive_bytes"] = prepared.environment_archive.stat().st_size
    if prepared is not None and prepared.editable_snapshot_archive is not None:
        worker_environment["editable_snapshot_bytes"] = (
            prepared.editable_snapshot_archive.stat().st_size
        )

    staging: dict[str, Any] = {"transfer_file_count": 0, "transfer_bytes": 0}
    if staged is not None:
        staging["transfer_file_count"] = len(staged.transfer_files)
        staging["transfer_bytes"] = sum(_path_size(path) for path in staged.transfer_files)

    return {
        "worker_environment": worker_environment,
        "staging": staging,
    }


def build_compile_bundle(compile_dir: Path, output_file: Path) -> Path:
    required = [
        "plan.yaml",
        "normalized.yaml",
        "deps.yaml",
        "dataset_entries.json",
        "dataset_metadata.json",
        "worker_environment.json",
    ]
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(output_file, "w:gz") as archive:
        for name in required:
            path = compile_dir / name
            if path.exists():
                archive.add(path, arcname=name)
    return output_file


def verify_transfer_files(paths: list[Path]) -> None:
    for path in paths:
        if not path.exists():
            raise FileNotFoundError(f"Staged transfer input does not exist: {path}")
        if path.is_file():
            try:
                with path.open("rb") as handle:
                    handle.read(1)
            except OSError as exc:
                raise OSError(f"Staged transfer input is not readable: {path}") from exc
            continue
        if path.is_dir():
            for child in path.rglob("*"):
                if child.is_file():
                    try:
                        with child.open("rb") as handle:
                            handle.read(1)
                    except OSError as exc:
                        raise OSError(
                            f"Staged transfer input is not readable: {child}"
                        ) from exc
            continue
        raise ValueError(f"Staged transfer input must be a file or directory: {path}")


def pack_relocatable_prefix(prefix: Path, output_file: Path) -> Path:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    try:
        import conda_pack  # noqa: PLC0415
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Packed Pixi prefix environments require conda-pack in the submit environment"
        ) from exc
    try:
        conda_pack.pack(
            prefix=str(prefix),
            output=str(output_file),
            force=True,
            ignore_editable_packages=True,
        )
    except Exception as exc:
        raise RuntimeError(f"Failed to pack Pixi prefix {prefix}: {exc}") from exc
    return output_file


def discover_editable_distributions(prefix: Path) -> list[EditableDistribution]:
    site_packages = Path(
        sysconfig.get_path(
            "purelib",
            vars={
                "base": str(prefix),
                "platbase": str(prefix),
            },
        )
    )
    editables: list[EditableDistribution] = []
    for dist in metadata.distributions(path=[str(site_packages)]):
        direct_url_text = dist.read_text("direct_url.json")
        if not direct_url_text:
            continue
        try:
            direct_url = json.loads(direct_url_text)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"Invalid direct_url.json for distribution {dist.metadata['Name']}"
            ) from exc
        if not direct_url.get("dir_info", {}).get("editable"):
            continue
        url = str(direct_url.get("url") or "")
        if not url.startswith("file://"):
            raise ValueError(
                f"Editable distribution {dist.metadata['Name']} has unsupported URL {url!r}"
            )
        source_path = Path(url.removeprefix("file://")).resolve()
        pyproject = source_path / "pyproject.toml"
        if not pyproject.exists():
            raise ValueError(
                f"Editable distribution {dist.metadata['Name']} has no pyproject.toml"
            )
        editables.append(
            EditableDistribution(
                name=dist.metadata["Name"],
                version=dist.version,
                source_path=source_path,
            )
        )
    return sorted(editables, key=lambda item: item.name.lower())


def build_editable_snapshot(
    editables: list[EditableDistribution],
    *,
    output_file: Path,
) -> EditableSnapshot:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="fasthep-editable-snapshot-") as tmp:
        snapshot_dir = Path(tmp) / "snapshot"
        snapshot_dir.mkdir()
        cmd = [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--no-index",
            "--no-deps",
            "--no-build-isolation",
            "--target",
            str(snapshot_dir),
            *(str(item.source_path) for item in editables),
        ]
        try:
            subprocess.run(cmd, check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as exc:
            details = (exc.stderr or exc.stdout or "").strip()
            suffix = f": {details}" if details else ""
            raise RuntimeError(f"Failed to snapshot editable packages{suffix}") from exc
        if output_file.exists():
            output_file.unlink()
        with tarfile.open(output_file, "w:gz") as archive:
            for item in sorted(snapshot_dir.iterdir()):
                archive.add(item, arcname=item.name)

    snapshot_record = _file_record(output_file)
    return EditableSnapshot(
        archive=output_file,
        manifest_entries=_editable_manifest_entries(
            editables,
            snapshot_record=snapshot_record,
        ),
    )


def _editable_manifest_entries(
    editables: list[EditableDistribution],
    *,
    snapshot_record: dict[str, Any],
) -> list[dict[str, Any]]:
    return [
        {
            "name": item.name,
            "version": item.version,
            "source_path": str(item.source_path),
            "snapshot": dict(snapshot_record),
            "vcs": vcs_state(item.source_path),
        }
        for item in editables
    ]


def write_bootstrap_script(
    path: Path,
    *,
    has_editable_snapshot: bool,
    shared_pythonpath: list[Path],
    required_imports: list[str],
) -> None:
    import_list = ", ".join(repr(name) for name in required_imports)
    shared_path = os.pathsep.join(str(path.resolve()) for path in shared_pythonpath)
    editable_lines = []
    if has_editable_snapshot:
        editable_lines = [
            "SNAPSHOT_ARCHIVE=\"${FASTHEP_EDITABLE_SNAPSHOT:-editable-snapshot.tar.gz}\"",
            "echo '[fasthep] extracting editable snapshot'",
            "mkdir -p editable-snapshot",
            "tar -xzf \"$SNAPSHOT_ARCHIVE\" -C editable-snapshot",
            'export PYTHONPATH="$PWD/editable-snapshot${PYTHONPATH:+:$PYTHONPATH}"',
        ]
    elif shared_path:
        editable_lines = [
            f"SHARED_EDITABLE_PYTHONPATH={_shell_quote(shared_path)}",
            (
                'export PYTHONPATH="$SHARED_EDITABLE_PYTHONPATH'
                '${PYTHONPATH:+:$PYTHONPATH}"'
            ),
        ]
    lines = [
        "#!/bin/sh",
        "set -e",
        "ENV_ARCHIVE=\"${FASTHEP_ENV_ARCHIVE:-prefix.tar.gz}\"",
        "echo '[fasthep] extracting packed Pixi prefix'",
        "mkdir -p worker-env",
        "tar -xzf \"$ENV_ARCHIVE\" -C worker-env",
        "echo '[fasthep] running conda-unpack'",
        "./worker-env/bin/conda-unpack",
        *editable_lines,
        "echo '[fasthep] worker python: ./worker-env/bin/python'",
        "./worker-env/bin/python --version",
        "./worker-env/bin/python - <<'PY'",
        "import importlib",
        f"for name in [{import_list}]:",
        "    importlib.import_module(name)",
        "print('[fasthep] verified worker imports')",
        "PY",
        "./worker-env/bin/python -m distributed.cli.dask_worker --help >/dev/null",
        "echo '[fasthep] launching dask worker'",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def _shared_bootstrap_commands(
    *,
    bootstrap_path: Path,
    archive_path: Path,
    editable_snapshot_path: Path | None,
    shared_pythonpath: list[Path],
) -> list[str]:
    commands = [
        "set -e",
        f"FASTHEP_ENV_ARCHIVE={_shell_quote(str(archive_path.resolve()))}",
    ]
    if editable_snapshot_path is not None:
        commands.append(
            "FASTHEP_EDITABLE_SNAPSHOT="
            f"{_shell_quote(str(editable_snapshot_path.resolve()))}"
        )
    elif shared_pythonpath:
        commands.append("unset FASTHEP_EDITABLE_SNAPSHOT")
    commands.append(f". {_shell_quote(str(bootstrap_path.resolve()))}")
    return commands


def required_fasthep_imports(
    prefix: Path,
    editables: list[EditableDistribution],
) -> list[str]:
    site_packages = Path(
        sysconfig.get_path(
            "purelib",
            vars={
                "base": str(prefix),
                "platbase": str(prefix),
            },
        )
    )
    names = {item.name for item in editables}
    names.update(
        dist.metadata["Name"]
        for dist in metadata.distributions(path=[str(site_packages)])
        if dist.metadata["Name"].lower().startswith("fasthep")
    )
    imports = {"distributed", "hepflow"}
    imports.update(_import_name_for_distribution(name) for name in names)
    return sorted(imports)


def installed_package_records(prefix: Path) -> list[dict[str, Any]]:
    site_packages = Path(
        sysconfig.get_path(
            "purelib",
            vars={
                "base": str(prefix),
                "platbase": str(prefix),
            },
        )
    )
    records: list[dict[str, Any]] = []
    for dist in metadata.distributions(path=[str(site_packages)]):
        name = dist.metadata.get("Name")
        if not name:
            continue
        direct_url_text = dist.read_text("direct_url.json")
        editable = False
        if direct_url_text:
            try:
                direct_url = json.loads(direct_url_text)
            except json.JSONDecodeError:
                direct_url = {}
            editable = bool(direct_url.get("dir_info", {}).get("editable"))
        records.append(
            {
                "name": name,
                "version": dist.version,
                "editable": editable,
            }
        )
    return sorted(records, key=lambda item: str(item["name"]).lower())


def vcs_state(source_path: Path) -> dict[str, Any] | None:
    try:
        root = subprocess.run(
            ["git", "-C", str(source_path), "rev-parse", "--show-toplevel"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None
    try:
        revision = subprocess.run(
            ["git", "-C", str(source_path), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        status = subprocess.run(
            ["git", "-C", str(source_path), "status", "--porcelain"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    except (OSError, subprocess.CalledProcessError):
        return {"root": root, "revision": None, "dirty": None}
    return {
        "root": root,
        "revision": revision,
        "dirty": bool(status.strip()),
    }


def _import_smoke_code(imports: list[str]) -> str:
    return "\n".join(
        [
            "import importlib",
            f"imports = {imports!r}",
            "for name in imports:",
            "    importlib.import_module(name)",
        ]
    )


def _import_name_for_distribution(name: str) -> str:
    fasthep = "fasthep"
    mapping = {
        "fasthep-flow": "hepflow",
        "fasthep-carpenter": f"{fasthep}_carpenter",
        "fasthep-curator": f"{fasthep}_curator",
        "fasthep-render": f"{fasthep}_render",
        "fasthep-toolbench": f"{fasthep}_toolbench",
        "fasthep-cli": f"{fasthep}_cli",
        "fasthep-workshop": f"{fasthep}_workshop",
    }
    return mapping.get(name.lower(), name.replace("-", "_"))


def _environment_id(
    environment: str,
    prefix: Path,
    editable_snapshot_archive: Path | None = None,
) -> str:
    digest = hashlib.sha256(f"{environment}:{prefix}".encode())
    if editable_snapshot_archive is not None:
        digest.update(_sha256(editable_snapshot_archive).encode())
    return f"{environment}-{digest.hexdigest()[:12]}"


def _file_record(path: Path) -> dict[str, Any]:
    return {
        "path": str(path),
        "size": path.stat().st_size,
        "sha256": _sha256(path),
    }


def _directory_record(path: Path) -> dict[str, Any]:
    files = [
        _file_record(item)
        for item in sorted(path.rglob("*"))
        if item.is_file()
    ]
    digest = hashlib.sha256()
    for item in files:
        digest.update(str(Path(item["path"]).relative_to(path)).encode())
        digest.update(str(item["sha256"]).encode())
    return {
        "path": str(path),
        "size": sum(int(item["size"]) for item in files),
        "sha256": digest.hexdigest(),
        "files": files,
    }


def _path_size(path: Path) -> int:
    if path.is_file():
        return path.stat().st_size
    if path.is_dir():
        return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())
    return 0


def _safe_progress_detail(detail: dict[str, Any]) -> dict[str, Any]:
    safe: dict[str, Any] = {}
    for key, value in detail.items():
        if isinstance(value, Path):
            continue
        if isinstance(value, str | int | float | bool) or value is None:
            safe[key] = value
        elif isinstance(value, dict):
            safe[key] = _safe_progress_detail(value)
    return safe


def _shell_quote(value: str) -> str:
    return "'" + value.replace("'", "'\"'\"'") + "'"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_packed_pixi_worker_environment(
    spec: PackedPixiEnvironmentSpec,
    *,
    credentials: list[WorkerCredential] | None = None,
) -> WorkerEnvironment:
    archive_path = Path(spec.archive_path)
    archive_name = archive_path.name
    worker_python = f"./{spec.worker_env_dir}/bin/python"
    credential_list = list(credentials or [])
    return WorkerEnvironment(
        python=worker_python,
        transfer_files=[archive_path, *(item.source_path for item in credential_list)],
        credentials=credential_list,
        prologue=[
            "set -e",
            *_credential_prologue_lines(credential_list),
            "echo '[fasthep] extracting packed Pixi environment'",
            "pwd",
            "ls -la",
            f"chmod +x {archive_name}",
            f"./{archive_name} --output-directory . --env-name {spec.worker_env_dir}",
            f"echo '[fasthep] worker python: {worker_python}'",
            f"ls -l {worker_python}",
            f"{worker_python} --version",
            f"{worker_python} -m distributed.cli.dask_worker --help >/dev/null",
            "echo '[fasthep] launching dask worker'",
        ],
    )


def build_htcondor_worker_environment_job_kwargs(
    env: WorkerEnvironment,
    *,
    log_paths: Mapping[str, Path],
) -> dict[str, object]:
    validate_transfer_file_basenames(env.transfer_files)
    transfer_files = ",".join(str(path.resolve()) for path in env.transfer_files)
    return {
        "python": env.python,
        "job_extra_directives": {
            "should_transfer_files": "YES",
            "when_to_transfer_output": "ON_EXIT",
            "transfer_executable": "False",
            "transfer_output_files": '""',
            "transfer_input_files": transfer_files,
            "Output": str(
                (log_paths["out"] / "worker-$(ClusterId).$(ProcId).out").resolve()
            ),
            "Error": str(
                (log_paths["err"] / "worker-$(ClusterId).$(ProcId).err").resolve()
            ),
            "Log": str((log_paths["logs"] / "worker-$(ClusterId).log").resolve()),
            "Stream_Output": "True",
            "Stream_Error": "True",
        },
        "job_script_prologue": list(env.prologue),
    }


def x509_proxy_from_environment(
    *,
    env_var: str = "X509_USER_PROXY",
    target_name: str = "x509_proxy",
    required: bool = False,
) -> WorkerCredential | None:
    """
    Discover an X509 proxy path without reading or logging credential contents.

    Credential files must never be embedded in plan.yaml or logs. Only local
    paths are passed to HTCondor file transfer at job submission time.
    """

    raw = os.environ.get(env_var)
    if raw is None or not raw.strip():
        if required:
            raise ValueError(f"{env_var} is not set; cannot transfer X509 proxy")
        return None

    source_path = Path(raw).expanduser().resolve()
    if not source_path.exists():
        raise FileNotFoundError(
            f"{env_var} points to missing X509 proxy file: {source_path}"
        )
    if not source_path.is_file():
        raise ValueError(f"{env_var} must point to a file: {source_path}")
    if not target_name.strip() or "/" in target_name or target_name in {".", ".."}:
        raise ValueError("X509 proxy target_name must be a simple file name")

    return WorkerCredential(
        type="x509_proxy",
        source_path=source_path,
        target_name=target_name.strip(),
        env_var=env_var,
    )


def packed_pixi_environment_spec_from_execution(
    execution: Mapping[str, Any],
) -> PackedPixiEnvironmentSpec | None:
    plan = worker_environment_plan_from_execution(execution)
    if plan is None:
        return None
    if plan.type != "packed-pixi":
        return None
    if plan.mode == "prefix":
        raise NotImplementedError(
            "execution.environment mode 'prefix' is planned but runtime preparation "
            "is not implemented yet"
        )
    return PackedPixiEnvironmentSpec(
        environment=plan.environment or "default",
        archive_path=plan.archive_path or "execution/worker-environments/env.sh",
        worker_env_dir=plan.worker_env_dir or "worker-env",
    )


def _credential_prologue_lines(credentials: list[WorkerCredential]) -> list[str]:
    lines: list[str] = []
    for credential in credentials:
        if credential.type != "x509_proxy":
            raise ValueError(f"Unsupported worker credential type {credential.type!r}")
        source_name = credential.source_path.name
        target_name = credential.target_name
        env_var = credential.env_var
        lines.extend(
            [
                "echo '[fasthep] configuring x509_proxy credential'",
                f"{env_var}_SOURCE=\"$PWD/{source_name}\"",
                f"{env_var}_TARGET=\"$PWD/{target_name}\"",
                (
                    f"if [ \"${env_var}_SOURCE\" != \"${env_var}_TARGET\" ]; "
                    f"then cp \"${env_var}_SOURCE\" \"${env_var}_TARGET\"; fi"
                ),
                f"export {env_var}=\"${env_var}_TARGET\"",
                f"chmod 600 \"${env_var}\"",
            ]
        )
    return lines


def validate_transfer_file_basenames(paths: list[Path]) -> None:
    counts = Counter(path.name for path in paths)
    duplicates = sorted(name for name, count in counts.items() if count > 1)
    if duplicates:
        names = ", ".join(duplicates)
        raise ValueError(
            "HTCondor worker environment transfer files must have unique basenames; "
            f"duplicates: {names}"
        )
