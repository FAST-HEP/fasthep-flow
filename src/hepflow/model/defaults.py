# hepflow/model/defaults.py
from __future__ import annotations

# Data / sources
DEFAULT_ROOT_TREE = "events"
DEFAULT_STREAM_TYPE = "event_stream"
DEFAULT_DATASET_EVENTTYPE = "mc"

# Joins / multi-tree
DEFAULT_JOIN_ON_MISMATCH = "error"  # error | warn | skip (future)
