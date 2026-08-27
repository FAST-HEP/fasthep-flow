from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
import sysconfig
import tarfile
from collections import Counter
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from importlib import metadata
from pathlib import Path
from typing import Any

from hepflow.build_layout import BuildPaths
from hepflow.model.worker_environment import worker_environment_plan_from_execution
from hepflow.utils import write_json


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
    application_wheelhouse: Path | None = None
    application_manifest: Path | None = None


@dataclass(slots=True, frozen=True)
class EditableDistribution:
    name: str
    version: str
    source_path: Path


@dataclass(slots=True, frozen=True)
class StagedExecutionFiles:
    python: str
    bootstrap_commands: list[str]
    transfer_files: list[Path]
    env: dict[str, str]
    manifest: Path


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

    prefix = resolve_pixi_prefix(plan.environment)
    environment_id = _environment_id(plan.environment, prefix)
    environment_dir = build_paths.worker_environments_dir() / environment_id
    environment_dir.mkdir(parents=True, exist_ok=True)
    archive_path = environment_dir / "environment.tar.gz"
    bootstrap_path = environment_dir / "bootstrap.sh"
    manifest_path = environment_dir / "manifest.json"

    pack_relocatable_prefix(prefix, archive_path)

    editables = discover_editable_distributions(prefix)
    application_manifest_path: Path | None = None
    application_transfer_files: list[Path] = []
    application_id: str | None = None
    if editables:
        application_id = _application_id(editables)
        application_dir = build_paths.applications_dir() / application_id
        application_transfer_files, application_manifest_path = stage_editable_wheels(
            editables,
            application_dir=application_dir,
        )

    required_imports = required_fasthep_imports(prefix, editables)
    write_bootstrap_script(
        bootstrap_path,
        wheelhouse_name="wheelhouse" if editables else None,
        required_imports=required_imports,
    )
    bootstrap_path.chmod(0o755)

    environment_manifest = {
        "type": "packed-pixi",
        "mode": "prefix",
        "environment": plan.environment,
        "environment_id": environment_id,
        "prefix": str(prefix),
        "archive": _file_record(archive_path),
        "bootstrap": _file_record(bootstrap_path),
        "worker_python": "./worker-env/bin/python",
        "application_id": application_id,
        "required_imports": required_imports,
    }
    write_json(environment_manifest, manifest_path)

    return PreparedWorkerEnvironment(
        python="./worker-env/bin/python",
        bootstrap_commands=_shared_bootstrap_commands(
            bootstrap_path=bootstrap_path,
            archive_path=archive_path,
            wheelhouse_path=application_transfer_files[0]
            if application_transfer_files
            else None,
        ),
        transfer_files=[],
        env={},
        environment_manifest=manifest_path,
        environment_archive=archive_path,
        bootstrap_script=bootstrap_path,
        application_wheelhouse=application_transfer_files[0]
        if application_transfer_files
        else None,
        application_manifest=application_manifest_path,
    )


def resolve_pixi_prefix(environment: str) -> Path:
    if not environment.strip():
        raise ValueError("Pixi environment name must be non-empty")
    current_name = os.environ.get("PIXI_ENVIRONMENT_NAME")
    current_prefix = os.environ.get("CONDA_PREFIX")
    if current_name == environment and current_prefix:
        prefix = Path(current_prefix)
    else:
        project_root = os.environ.get("PIXI_PROJECT_ROOT")
        if not project_root:
            raise ValueError(
                "PIXI_PROJECT_ROOT is not set; cannot resolve Pixi environment prefix"
            )
        prefix = Path(project_root) / ".pixi" / "envs" / environment
    if not prefix.exists():
        raise FileNotFoundError(f"Pixi environment prefix does not exist: {prefix}")
    python = prefix / "bin" / "python"
    if not python.exists():
        raise FileNotFoundError(f"Pixi environment prefix has no Python: {python}")
    return prefix.resolve()


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
    environment_archive = staging_dir / "environment.tar.gz"
    bootstrap_script = staging_dir / "bootstrap.sh"
    application_dir = staging_dir / "application"
    manifest_path = staging_dir / "manifest.json"

    build_compile_bundle(build_paths.compile_dir(), compile_archive)
    if prepared.environment_archive is None or prepared.bootstrap_script is None:
        raise ValueError("Prepared worker environment is missing environment artifacts")
    shutil.copy2(prepared.environment_archive, environment_archive)
    shutil.copy2(prepared.bootstrap_script, bootstrap_script)
    transfer_files = [compile_archive, environment_archive, bootstrap_script]

    application_manifest: dict[str, Any] | None = None
    if prepared.application_wheelhouse is not None:
        wheelhouse_target = application_dir / "wheelhouse"
        shutil.copytree(prepared.application_wheelhouse, wheelhouse_target)
        transfer_files.append(application_dir)
        application_manifest = {
            "wheelhouse": _directory_record(wheelhouse_target),
        }

    verify_transfer_files(transfer_files)
    manifest = {
        "mode": "transfer",
        "compile": _file_record(compile_archive),
        "environment": _file_record(environment_archive),
        "bootstrap": _file_record(bootstrap_script),
        "application": application_manifest,
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
            "FASTHEP_ENV_ARCHIVE=environment.tar.gz",
            "FASTHEP_WHEELHOUSE=application/wheelhouse",
            ". ./bootstrap.sh",
        ],
        transfer_files=transfer_files,
        env={},
        manifest=manifest_path,
    )


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


def stage_editable_wheels(
    editables: list[EditableDistribution],
    *,
    application_dir: Path,
) -> tuple[list[Path], Path]:
    wheelhouse = application_dir / "wheelhouse"
    if wheelhouse.exists():
        shutil.rmtree(wheelhouse)
    wheelhouse.mkdir(parents=True, exist_ok=True)
    for editable in editables:
        subprocess.run(
            [
                sys.executable,
                "-m",
                "build",
                "--wheel",
                "--outdir",
                str(wheelhouse),
                str(editable.source_path),
            ],
            check=True,
        )
    wheels = sorted(wheelhouse.glob("*.whl"))
    if len(wheels) < len(editables):
        raise RuntimeError("Editable package wheel staging did not produce all wheels")
    manifest_path = application_dir / "manifest.json"
    write_json(
        {
            "application_id": application_dir.name,
            "packages": [
                {
                    "name": item.name,
                    "version": item.version,
                    "source_path": str(item.source_path),
                }
                for item in editables
            ],
            "wheels": [_file_record(path) for path in wheels],
        },
        manifest_path,
    )
    return [wheelhouse], manifest_path


def write_bootstrap_script(
    path: Path,
    *,
    wheelhouse_name: str | None,
    required_imports: list[str],
) -> None:
    import_list = ", ".join(repr(name) for name in required_imports)
    wheel_lines = []
    if wheelhouse_name is not None:
        wheel_lines = [
            "mkdir -p applications",
            (
                "./worker-env/bin/python -m pip install --no-index --no-deps "
                "--find-links \"$WHEELHOUSE\" --target ./applications \"$WHEELHOUSE\"/*.whl"
            ),
            'export PYTHONPATH="$PWD/applications${PYTHONPATH:+:$PYTHONPATH}"',
        ]
    lines = [
        "#!/bin/sh",
        "set -e",
        "ENV_ARCHIVE=\"${FASTHEP_ENV_ARCHIVE:-environment.tar.gz}\"",
        f"WHEELHOUSE=\"${{FASTHEP_WHEELHOUSE:-{wheelhouse_name or 'wheelhouse'}}}\"",
        "echo '[fasthep] extracting packed Pixi prefix'",
        "mkdir -p worker-env",
        "tar -xzf \"$ENV_ARCHIVE\" -C worker-env",
        "echo '[fasthep] running conda-unpack'",
        "./worker-env/bin/conda-unpack",
        *wheel_lines,
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
    wheelhouse_path: Path | None,
) -> list[str]:
    commands = [
        "set -e",
        f"FASTHEP_ENV_ARCHIVE={_shell_quote(str(archive_path.resolve()))}",
    ]
    if wheelhouse_path is not None:
        commands.append(
            f"FASTHEP_WHEELHOUSE={_shell_quote(str(wheelhouse_path.resolve()))}"
        )
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


def _environment_id(environment: str, prefix: Path) -> str:
    digest = hashlib.sha256(f"{environment}:{prefix}".encode()).hexdigest()[:12]
    return f"{environment}-{digest}"


def _application_id(editables: list[EditableDistribution]) -> str:
    payload = "|".join(
        f"{item.name}:{item.version}:{item.source_path}" for item in editables
    )
    return f"editable-{hashlib.sha256(payload.encode()).hexdigest()[:12]}"


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
        environment=plan.environment,
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
