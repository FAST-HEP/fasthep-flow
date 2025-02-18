"""Utilities module for fasthep-flow.
Rule: Keep It Focused and Small (KISS)
- any function should have a single responsibility
- no classes
- explicit names
"""

from __future__ import annotations

import hashlib
import importlib
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


def instance_from_type_string(type_string: str, *args, **kwargs) -> Any:  # type: ignore[no-untyped-def]
    """Create an instance from a type string, e.g. 'module.submodule.Class'."""
    module_path, class_name = type_string.rsplit(".", 1)
    mod = importlib.import_module(module_path)
    return getattr(mod, class_name)(*args, **kwargs)


def is_class(obj: Any) -> bool:
    """Check if an object is a class.
    Will unwrap the object to make it work for decorated classes."""
    return inspect.isclass(inspect.unwrap(obj))


def is_valid_import(module_path: str, aliases: dict[str, str]) -> bool:
    """Check if a module can be imported."""
    value = aliases.get(module_path, module_path)
    module_path, class_name = value.rsplit(".", 1)
    try:
        # Import the module
        mod = importlib.import_module(module_path)
        # this must be a class
        class_ = getattr(mod, class_name)
        return is_class(class_)
    except ImportError as _:
        return False
