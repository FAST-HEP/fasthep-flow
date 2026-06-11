from __future__ import annotations

from distributed import Client

from hepflow.backends._dask._pooled import DaskPooledHTCondorCluster


def where_am_i(label: str) -> dict[str, object]:
    from distributed import get_worker

    worker = get_worker()
    return {
        "label": label,
        "worker": worker.address,
        "resources": dict(worker.total_resources),
    }


def main() -> None:
    cluster = DaskPooledHTCondorCluster(
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


if __name__ == "__main__":
    main()
