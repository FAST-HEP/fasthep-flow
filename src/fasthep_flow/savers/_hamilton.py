from __future__ import annotations

from collections.abc import Collection
from dataclasses import dataclass
from typing import Any

import awkward as ak
from hamilton.io.data_adapters import DataLoader, DataSaver
from hamilton.io.utils import get_file_metadata

# @datasaver()
# def save_ak_array(ak_array: ak.Array, path: str) -> dict:
#     ak.to_parquet(ak_array, path)
#     return {"destination": path, "format": "parquet"}


ARRAY_TYPES = (ak.Array, ak.highlevel.Array)


@dataclass
class AwkwardParquetSaver(DataSaver):
    path: str

    @classmethod
    def applicable_types(cls) -> Collection[type]:
        return ARRAY_TYPES

    @classmethod
    def name(cls) -> str:
        return "parquet"

    def save_data(self, data: Any) -> dict[str, Any]:
        ak.to_parquet(data, self.path)
        return get_file_metadata(self.path)


@dataclass
class AwkwardParquetLoader(DataLoader):
    path: str

    @classmethod
    def applicable_types(cls) -> Collection[type]:
        return ARRAY_TYPES

    @classmethod
    def name(cls) -> str:
        return "parquet"

    def load_data(self, type_: type) -> tuple[dict, dict[str, Any]]:
        return ak.from_parquet(self.path), get_file_metadata(self.path)


DATA_ADAPTERS = [AwkwardParquetSaver, AwkwardParquetLoader]
