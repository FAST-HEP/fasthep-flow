from __future__ import annotations

import json
import pickle
from dataclasses import asdict, is_dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml


def now_iso():
    return datetime.now(UTC).isoformat()


def write_json(obj: dict[str, Any], path: str | Path) -> str:
    path = Path(path)
    ensure_dir(path)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, sort_keys=True)
    return str(path)


def read_json(path: str | Path) -> dict[str, Any]:
    with Path(path).open(encoding="utf-8") as f:
        return json.load(f)


def write_pickle(obj: Any, path: str | Path) -> str:
    path = Path(path)
    ensure_dir(path)
    with path.open("wb") as f:
        pickle.dump(obj, f, protocol=pickle.HIGHEST_PROTOCOL)
    return str(path)


def read_pickle(path: str | Path) -> Any:
    with Path(path).open("rb") as f:
        return pickle.load(f)


def read_yaml(path: str | Path) -> dict[str, Any]:
    with Path(path).open(encoding="utf-8") as f:
        return yaml.safe_load(f)


def write_yaml(obj: dict[str, Any], path: str | Path) -> None:
    with Path(path).open("w", encoding="utf-8") as f:
        yaml.safe_dump(obj, f, sort_keys=False)


def write_text(text: str, path: str | Path) -> None:
    with Path(path).open("w", encoding="utf-8") as f:
        f.write(text)


def ensure_dir(path: str | Path) -> Path:
    parent = Path(path).parent
    parent.mkdir(exist_ok=True, parents=True)
    return parent


def to_dict(obj: Any) -> Any:
    """
    Convert dataclasses to plain dictionaries recursively.
    Leaves other objects unchanged.
    """
    if obj is None:
        return None
    if is_dataclass(obj) and not isinstance(obj, type):
        return asdict(obj)
    return obj
