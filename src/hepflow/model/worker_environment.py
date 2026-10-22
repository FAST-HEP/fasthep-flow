from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


@dataclass(slots=True, frozen=True)
class WorkerEnvironmentPlan:
    type: str
    mode: str
    environment: str | None = None
    source: str | None = None
    archive_path: str | None = None
    worker_env_dir: str | None = None
    prefix: str | None = None

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
    if env_type == "shared-prefix":
        return _shared_prefix_plan(raw)
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


def _shared_prefix_plan(raw: Mapping[str, Any]) -> WorkerEnvironmentPlan:
    """
    Workers run the Python of an existing prefix on a shared filesystem.

    Only the shape is checked here; whether the prefix exists and is usable is
    checked by the backend when it prepares the workers.
    """

    # ``mode`` is written back by the normalised plan; only "shared" is valid.
    unknown = sorted(str(key) for key in raw if key not in {"type", "prefix", "mode"})
    if raw.get("mode", "shared") != "shared":
        raise ValueError(
            "Unsupported execution.environment.mode for shared-prefix: "
            f"{raw['mode']!r} (only 'shared')"
        )
    if unknown:
        raise ValueError(
            "Unsupported execution.environment keys for shared-prefix: "
            + ", ".join(unknown)
        )
    prefix = raw.get("prefix", "current")
    if not isinstance(prefix, str) or not prefix.strip():
        raise ValueError("execution.environment.prefix must be a string")
    prefix = prefix.strip()
    if prefix != "current" and not Path(prefix).is_absolute():
        raise ValueError(
            "execution.environment.prefix for shared-prefix must be an absolute "
            f"path or 'current', got {prefix!r}"
        )
    return WorkerEnvironmentPlan(type="shared-prefix", mode="shared", prefix=prefix)
