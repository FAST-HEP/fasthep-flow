from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass
from typing import Any


@dataclass(slots=True, frozen=True)
class WorkerEnvironmentPlan:
    type: str
    mode: str
    environment: str | None = None
    source: str | None = None
    archive_path: str | None = None
    worker_env_dir: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {key: value for key, value in asdict(self).items() if value is not None}


def worker_environment_plan_from_execution(
    execution: Mapping[str, Any],
) -> WorkerEnvironmentPlan | None:
    raw = execution.get("environment")
    if not isinstance(raw, Mapping) or not raw:
        return None
    env_type = raw.get("type")
    if env_type is None:
        raise ValueError("execution.environment.type must be defined")
    if env_type != "packed-pixi":
        raise ValueError(f"Unsupported execution.environment.type {env_type!r}")
    mode = raw.get("mode", "self-extracting")
    if mode not in {"prefix", "self-extracting"}:
        raise ValueError(
            "execution.environment.mode must be 'prefix' or 'self-extracting'"
        )
    if mode == "prefix":
        if "environment" in raw:
            raise ValueError(
                "execution.environment.environment is ambiguous for packed-pixi "
                "prefix mode; use execution.environment.source: current"
            )
        source = raw.get("source", "current")
        if not isinstance(source, str) or not source.strip():
            raise ValueError("execution.environment.source must be a string")
        if source.strip() != "current":
            raise ValueError(
                "execution.environment.source for packed-pixi prefix mode must be "
                "'current'"
            )
        return WorkerEnvironmentPlan(
            type="packed-pixi",
            mode="prefix",
            source="current",
        )
    environment = raw.get("environment", "default")
    if not isinstance(environment, str) or not environment.strip():
        raise ValueError("execution.environment.environment must be a string")
    archive_path = raw.get("archive_path", "execution/worker-environments/env.sh")
    if not isinstance(archive_path, str) or not archive_path.strip():
        raise ValueError("execution.environment.archive_path must be a string")
    worker_env_dir = raw.get("worker_env_dir", "worker-env")
    if not isinstance(worker_env_dir, str) or not worker_env_dir.strip():
        raise ValueError("execution.environment.worker_env_dir must be a string")
    return WorkerEnvironmentPlan(
        type="packed-pixi",
        mode="self-extracting",
        environment=environment.strip(),
        archive_path=archive_path.strip(),
        worker_env_dir=worker_env_dir.strip(),
    )
