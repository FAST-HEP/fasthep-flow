from __future__ import annotations

import argparse
from pathlib import Path

from distributed import Client

from hepflow.backends._dask._pooled import DaskPooledHTCondorCluster
from hepflow.backends._dask._worker_env import (
    PackedPixiEnvironmentSpec,
    build_htcondor_worker_environment_job_kwargs,
    build_packed_pixi_worker_environment,
    pack_pixi_environment,
)

DEBUG_ROOT = Path("build/debug/distributed/htcondor")
PACKED_ENV_NAME = "env.sh"
WORKER_ENV_DIR = "worker-env"


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
        worker_env = build_packed_pixi_worker_environment(
            PackedPixiEnvironmentSpec(
                environment=args.environment,
                archive_path=str(packed_env),
                worker_env_dir=WORKER_ENV_DIR,
            )
        )
        common_job_kwargs.update(
            build_htcondor_worker_environment_job_kwargs(
                worker_env,
                log_paths=paths,
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


if __name__ == "__main__":
    main()
