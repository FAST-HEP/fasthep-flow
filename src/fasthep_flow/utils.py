"""Utilities module for fasthep-flow.
Rule: Keep It Focused and Small (KISS)
- any function should have a single responsibility
- no classes
- explicit names
"""

from __future__ import annotations

import hashlib
import inspect
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

DEFAULT_DATE_FORMAT = "%Y.%m.%d"


def get_config_hash(config_file: Path) -> str:
    """Reads the config file and returns a shortened hash."""
    with config_file.open("rb") as f:
        return hashlib.file_digest(f, "sha256").hexdigest()[:8]


def formatted_today() -> str:
    """Return the current date in the format YYYY.MM.DD"""
    return datetime.now().strftime(DEFAULT_DATE_FORMAT)


def generate_save_path(base_path: Path, workflow_name: str, config_path: Path) -> Path:
    """
    Creates a save path for the workflow and returns the generated path.

    @param base_path: Base path for the save location.
    @param workflow_name: Name of the workflow.
    @param config_path: Path to the configuration file.

    returns: Path to the save location.
    """
    today = formatted_today()
    config_hash = get_config_hash(config_path)
    return Path(f"{base_path}/{workflow_name}/{today}/{config_hash}/").resolve()


def calculate_function_hash(func: Callable[..., Any], *args, **kwargs) -> str:  # type: ignore[no-untyped-def]
    """Calculate the hash of a function."""
    # encode parameter values to hash
    arg_hash = hashlib.sha256(str(args).encode() + str(kwargs).encode()).hexdigest()
    # encode function source code to hash
    func_hash = hashlib.sha256(inspect.getsource(func).encode()).hexdigest()
    # combine both hashes
    return hashlib.sha256(arg_hash.encode() + func_hash.encode()).hexdigest()[:8]
