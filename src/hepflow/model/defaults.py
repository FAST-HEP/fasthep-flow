# hepflow/model/defaults.py
from __future__ import annotations

# --------
# Planning / IO
# --------
DEFAULT_WORK_DIR = "work"
DEFAULT_RESULTS_DIR = "results"
DEFAULT_CHUNK_SIZE = 500_000

# --------
# Data / sources
# --------
DEFAULT_ROOT_TREE = "events"
DEFAULT_STREAM_TYPE = "event_stream"
DEFAULT_PRIMARY_STREAM_ID = "events"  # stream id, not the ROOT tree name
DEFAULT_DATASET_EVENTTYPE = "mc"

# Context symbols that runtime guarantees
# you also pass dataset_name; keep both if desired
DEFAULT_DATASET_CONTEXT_KEY = "dataset"
DEFAULT_DATASET_NAME_CONTEXT_KEY = "dataset_name"
DEFAULT_CONTEXT_SYMBOLS: set[str] = {
    DEFAULT_DATASET_CONTEXT_KEY,
    DEFAULT_DATASET_NAME_CONTEXT_KEY,
    "file",
    "entries",
    "start",
    "stop",
    "primary_stream",
}

# Joins / multi-tree
DEFAULT_JOIN_ON_MISMATCH = "error"  # error | warn | skip (future)

# --------
# Histograms
# --------
DEFAULT_HIST_STORAGE = "count"  # count | weighted
DEFAULT_HIST_YLABEL = "Events"
DEFAULT_HIST_ZLABEL = "Events"

# --------
# Rendering
# --------
DEFAULT_RENDER_WHEN = "final"          # final | always (future)
DEFAULT_RENDER_ON_MISMATCH = "skip"

DEFAULT_RENDER_KIND_1D = "plot1d_png"
DEFAULT_RENDER_KIND_2D = "plot2d_png"

DEFAULT_PLOT_STYLE = "CMS"
DEFAULT_PLOT_DPI = 150
DEFAULT_PLOT_LEGEND_LOC = "best"
DEFAULT_PLOT_YSCALE = "linear"
DEFAULT_PLOT_NORM = "linear"  # for 2D color norm (log/linear)
DEFAULT_PLOT_CMIN = 1

# --------
# inspection-friendly snapshot
# --------
DEFAULTS = {
    "paths": {
        "work": DEFAULT_WORK_DIR,
        "results": DEFAULT_RESULTS_DIR,
    },
    "planning": {
        "chunk_size": DEFAULT_CHUNK_SIZE,
    },
    "sources": {
        "root_tree": DEFAULT_ROOT_TREE,
        "stream_type": DEFAULT_STREAM_TYPE,
        "primary_stream_id": DEFAULT_PRIMARY_STREAM_ID,
        "join_on_mismatch": DEFAULT_JOIN_ON_MISMATCH,
    },
    "datasets": {
        "eventtype": DEFAULT_DATASET_EVENTTYPE,
    },
    "hist": {
        "storage": DEFAULT_HIST_STORAGE,
        "ylabel": DEFAULT_HIST_YLABEL,
        "zlabel": DEFAULT_HIST_ZLABEL,
    },
    "render": {
        "when": DEFAULT_RENDER_WHEN,
        "style": DEFAULT_PLOT_STYLE,
        "dpi": DEFAULT_PLOT_DPI,
        "legend_loc": DEFAULT_PLOT_LEGEND_LOC,
        "yscale": DEFAULT_PLOT_YSCALE,
        "norm": DEFAULT_PLOT_NORM,
        "cmin": DEFAULT_PLOT_CMIN,
        "kind_1d": DEFAULT_RENDER_KIND_1D,
        "kind_2d": DEFAULT_RENDER_KIND_2D,
    },
    "context": {
        "dataset": DEFAULT_DATASET_CONTEXT_KEY,
        "dataset_name": DEFAULT_DATASET_NAME_CONTEXT_KEY,
    },
}
