from __future__ import annotations

import json
import os
import pickle
from dataclasses import asdict, is_dataclass
from datetime import UTC, datetime
from typing import Any

import yaml


def now_iso():
    return datetime.now(UTC).isoformat()


def write_json(obj: dict[str, Any], path: str) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(obj, f, indent=2, sort_keys=True)
    return path


def read_json(path: str) -> dict[str, Any]:
    with open(path) as f:
        return json.load(f)


def write_pickle(obj: Any, path: str) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        pickle.dump(obj, f, protocol=pickle.HIGHEST_PROTOCOL)
    return path


def read_pickle(path: str) -> Any:
    with open(path, "rb") as f:
        return pickle.load(f)


def read_yaml(path: str) -> dict[str, Any]:
    with open(path) as f:
        return yaml.safe_load(f)


def write_yaml(obj: dict[str, Any], path: str) -> None:
    with open(path, "w") as f:
        yaml.safe_dump(obj, f, sort_keys=False)


def write_text(text: str, path: str) -> None:
    with open(path, "w") as f:
        f.write(text)


def ensure_dir(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)


def rss_mb() -> float:
    try:
        import os

        import psutil

        return psutil.Process(os.getpid()).memory_info().rss / (1024**2)
    except Exception:
        return -1.0


def mem(tag: str) -> None:
    print(f"[mem] {tag}: rss={rss_mb():.1f} MB", flush=True)


def to_dict(obj: Any) -> Any:
    """
    Convert dataclasses to plain dictionaries recursively.
    Leaves other objects unchanged.
    """
    if obj is None:
        return None
    if is_dataclass(obj):
        return asdict(obj)
    return obj
