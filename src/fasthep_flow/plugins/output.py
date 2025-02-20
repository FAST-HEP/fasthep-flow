from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import dill as pickle
from loguru import logger

from fasthep_flow.checksums import calculate_checksums

from ._base import PluginInterface

DEFAULT_FORMAT = "json"
DEFAULT_CHECKSUM_TYPES = ["adler32", "crc32c"]


def _save_as_json(result: Any, output_file: Path) -> None:
    with output_file.open("w") as f:
        json.dump(result, f)


def _save_as_pickle(result: Any, output_file: Path) -> None:
    with output_file.open("wb") as f:
        pickle.dump(result, f)


WRITERS = {
    "json": _save_as_json,
    "pickle": _save_as_pickle,
}
SUPPORTED_FORMATS = list(WRITERS.keys())


def _no_filter(x: Any) -> Any:
    return x


@dataclass
class LocalOutputPlugin(PluginInterface):
    """Plugin for writing output to a local file.
    Output will be stored with the function hash as the filename.

    @output_path: path to write the output to.
    @func_hash: hash of the function that generated the output
    """

    output_file: Path
    format: str
    filter_func: Callable[..., Any] = _no_filter
    checksum_types: list[str] = field(default_factory=list[str])
    __metadata__: dict[str, Any] = field(default_factory=dict[str, Any])

    def __post_init__(self) -> None:
        if self.format not in SUPPORTED_FORMATS:
            msg = f"Output format {self.format} is not supported."
            logger.error(msg)
            raise ValueError(msg)
        if not self.checksum_types:
            self.checksum_types = DEFAULT_CHECKSUM_TYPES
        self.output_file = Path(self.output_file)

    def before(self, func: Callable[..., Any], *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        logger.debug(f"Running before hook for {func.__name__}")
        if self.output_file.exists():
            logger.warning(
                f"File {self.output_file} already exists and will be overwritten."
            )
        output_path = self.output_file.parent
        logger.debug(f"Creating output path {output_path}")
        output_path.mkdir(parents=True, exist_ok=True)

    def after(self, func: Callable[..., Any], result: Any, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        logger.debug(f"Running after hook for {func.__name__}")
        logger.debug(f"Writing output to {self.output_file}")
        with self.output_file.open("w", encoding="UTF-8") as f:
            json.dump(result, f)
        logger.debug(f"Output written to {self.output_file}")
        logger.debug(f"Calculating checksums for {self.output_file}")
        checksums = calculate_checksums(self.output_file, self.checksum_types)
        self.__metadata__["checksums"] = checksums
        logger.debug(f"Checksums calculated: {checksums}")
