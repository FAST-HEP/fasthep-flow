from __future__ import annotations

import argparse
import subprocess
from pathlib import Path

from distributed import Client

from hepflow.backends._dask._pooled import DaskPooledHTCondorCluster

DEBUG_ROOT = Path("build/debug/distributed/htcondor")
PACKED_ENV_NAME = "env.sh"
WORKER_ENV_DIR = "env"
PACKED_WORKER_PYTHON = f"./{WORKER_ENV_DIR}/bin/python"


def where_am_i(label: str) -> dict[str, object]:
    from distributed import get_worker

    worker = get_worker()
    return {
        "label": label,
        "worker": worker.address,
        "resources": dict(worker.total_resources),
    }


def main() -> None:
    args = parse_args()
    paths = prepare_debug_paths(DEBUG_ROOT)
    packed_env = None
    common_job_kwargs: dict[str, object] = {
        "log_directory": str(paths["logs"].resolve()),
    }

    if args.pack_env:
        packed_env = pack_pixi_environment(
            paths["root"] / PACKED_ENV_NAME,
            environment=args.environment,
        )
        common_job_kwargs.update(
            build_packed_env_job_kwargs(
                packed_env,
                paths=paths,
                worker_python=PACKED_WORKER_PYTHON,
            )
        )

    print(f"packed mode: {args.pack_env}")
    print(f"debug root: {paths['root'].resolve()}")
    print(f"HTCondor log directory: {paths['logs'].resolve()}")
    print(f"HTCondor stdout directory: {paths['out'].resolve()}")
    print(f"HTCondor stderr directory: {paths['err'].resolve()}")
    print(f"packed environment: {packed_env.resolve() if packed_env else '<disabled>'}")
    print(f"worker python: {common_job_kwargs.get('python', '<submit-host default>')}")

    cluster = DaskPooledHTCondorCluster(
        job_kwargs=common_job_kwargs,
        pools={
            "default": {
                "workers": 1,
                "job_kwargs": {
                    "cores": 1,
                    "memory": "100MB",
                    "disk": "10MB",
                    "resources": {"resource.default": 1},
                },
            },
            "high_memory": {
                "workers": 1,
                "job_kwargs": {
                    "cores": 1,
                    "memory": "800MB",
                    "disk": "10MB",
                    "resources": {"resource.high_memory": 1},
                },
            },
        },
        scheduler_options={"dashboard_address": ":0"},
    )

    with cluster:
        cluster.scale({"default": 1, "high_memory": 1})
        client = Client(cluster)
        try:
            print(f"scheduler address: {cluster.scheduler_address}")
            print(f"dashboard link: {client.dashboard_link}")
            default_future = client.submit(
                where_am_i,
                "default",
                resources={"resource.default": 1},
            )
            highmem_future = client.submit(
                where_am_i,
                "high_memory",
                resources={"resource.high_memory": 1},
            )

            print(default_future.result(timeout=120))
            print(highmem_future.result(timeout=120))
        finally:
            client.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Manual pooled Dask HTCondor prototype test."
    )
    parser.add_argument(
        "--pack-env",
        action="store_true",
        help="Pack the Pixi environment and transfer it to HTCondor workers.",
    )
    parser.add_argument(
        "-e",
        "--environment",
        default="default",
        help="Pixi environment to pack when --pack-env is enabled.",
    )
    return parser.parse_args()


def prepare_debug_paths(root: Path) -> dict[str, Path]:
    paths = {
        "root": root,
        "logs": root / "logs",
        "out": root / "out",
        "err": root / "err",
    }
    for path in paths.values():
        path.mkdir(parents=True, exist_ok=True)
    return paths


def pack_pixi_environment(output_file: Path, *, environment: str) -> Path:
    output_file = output_file.resolve()
    output_file.parent.mkdir(parents=True, exist_ok=True)
    print(f"packing Pixi environment {environment!r} to {output_file}")
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


def build_packed_env_job_kwargs(
    archive_path: Path,
    *,
    paths: dict[str, Path],
    worker_python: str,
) -> dict[str, object]:
    archive_path = archive_path.resolve()
    archive_name = archive_path.name
    return {
        "python": worker_python,
        "job_extra_directives": {
            "should_transfer_files": "YES",
            "when_to_transfer_output": "ON_EXIT",
            "transfer_executable": "False",
            "transfer_input_files": str(archive_path),
            "Output": str((paths["out"] / "worker-$(ClusterId).$(ProcId).out").resolve()),
            "Error": str((paths["err"] / "worker-$(ClusterId).$(ProcId).err").resolve()),
            "Log": str((paths["logs"] / "worker-$(ClusterId).log").resolve()),
            "Stream_Output": "True",
            "Stream_Error": "True",
        },
        "job_script_prologue": [
            "set -e",
            "echo '[fasthep] extracting packed Pixi environment'",
            "pwd",
            "ls -la",
            f"chmod +x {archive_name}",
            f"./{archive_name} --output-directory . --env-name {WORKER_ENV_DIR}",
            f"echo '[fasthep] worker python: {worker_python}'",
            f"ls -l {worker_python}",
            f"{worker_python} --version",
            f"{worker_python} -m distributed.cli.dask_worker --help >/dev/null",
            "echo '[fasthep] launching dask worker'",
        ],
    }


if __name__ == "__main__":
    main()
