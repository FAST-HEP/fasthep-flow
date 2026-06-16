from __future__ import annotations

import os
import subprocess
from collections import Counter
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


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
    _validate_transfer_file_basenames(env.transfer_files)
    transfer_files = ",".join(str(path.resolve()) for path in env.transfer_files)
    return {
        "python": env.python,
        "job_extra_directives": {
            "should_transfer_files": "YES",
            "when_to_transfer_output": "ON_EXIT",
            "transfer_executable": "False",
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
    raw = execution.get("environment")
    if not isinstance(raw, Mapping):
        return None
    if raw.get("type") != "packed-pixi":
        return None
    environment = raw.get("environment", "default")
    if not isinstance(environment, str) or not environment.strip():
        raise ValueError("execution.environment.environment must be a string")
    archive_path = raw.get("archive_path", "debug/distributed/htcondor/env.sh")
    if not isinstance(archive_path, str) or not archive_path.strip():
        raise ValueError("execution.environment.archive_path must be a string")
    worker_env_dir = raw.get("worker_env_dir", "worker-env")
    if not isinstance(worker_env_dir, str) or not worker_env_dir.strip():
        raise ValueError("execution.environment.worker_env_dir must be a string")
    return PackedPixiEnvironmentSpec(
        environment=environment.strip(),
        archive_path=archive_path.strip(),
        worker_env_dir=worker_env_dir.strip(),
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


def _validate_transfer_file_basenames(paths: list[Path]) -> None:
    counts = Counter(path.name for path in paths)
    duplicates = sorted(name for name, count in counts.items() if count > 1)
    if duplicates:
        names = ", ".join(duplicates)
        raise ValueError(
            "HTCondor worker environment transfer files must have unique basenames; "
            f"duplicates: {names}"
        )
