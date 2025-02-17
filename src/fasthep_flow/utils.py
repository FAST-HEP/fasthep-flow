"""Utilities module for fasthep-flow.
Rule: Keep It Focused and Small (KISS)
- any function should have a single responsibility
- no classes
- explicit names
"""

from __future__ import annotations

import hashlib
from datetime import datetime
from pathlib import Path


def get_config_hash(config_file: Path) -> str:
    """Reads the config file and returns a shortened hash."""
    with config_file.open("rb") as f:
        return hashlib.file_digest(f, "sha256").hexdigest()[:8]


def generate_save_path(base_path: Path, workflow_name: str, config_path: Path) -> Path:
    """
    Creates a save path for the workflow and returns the generated path.

    @param base_path: Base path for the save location.
    @param workflow_name: Name of the workflow.
    @param config_path: Path to the configuration file.

    returns: Path to the save location.
    """
    date = datetime.now().strftime("%Y.%m.%d")
    config_hash = get_config_hash(config_path)
    return Path(f"{base_path}/{workflow_name}/{date}/{config_hash}/").resolve()
