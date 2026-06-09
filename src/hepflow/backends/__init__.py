from __future__ import annotations

from hepflow.backends._dask._common import DaskBackend as Dask
from hepflow.backends.local import LocalBackend as Local

__all__ = [
    "Dask",
    "Local",
]
