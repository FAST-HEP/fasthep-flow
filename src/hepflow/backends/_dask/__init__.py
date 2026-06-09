from __future__ import annotations

from hepflow.backends._dask._htcondor import compute_with_htcondor
from hepflow.backends._dask._local import compute_with_local_cluster

__all__ = [
    "compute_with_htcondor",
    "compute_with_local_cluster",
]
