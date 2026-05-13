from __future__ import annotations

import awkward as ak
import uproot


def read_events(file: str, tree: str, branches: list[str], entry_start: int, entry_stop: int) -> ak.Array:
    with uproot.open(file) as f:
        t = f[tree]
        return t.arrays(branches, entry_start=entry_start, entry_stop=entry_stop, library="ak")
