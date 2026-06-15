from __future__ import annotations

import subprocess
from collections.abc import Mapping
from dataclasses import asdict, dataclass
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
class WorkerEnvironment:
    python: str
    prologue: list[str]
    transfer_files: list[Path]


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
) -> WorkerEnvironment:
    archive_path = Path(spec.archive_path)
    archive_name = archive_path.name
    worker_python = f"./{spec.worker_env_dir}/bin/python"
    return WorkerEnvironment(
        python=worker_python,
        transfer_files=[archive_path],
        prologue=[
            "set -e",
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
