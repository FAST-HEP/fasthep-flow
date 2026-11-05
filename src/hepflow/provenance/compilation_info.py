"""Facts about the compilation itself, recorded under ``provenance.compilation``."""

from __future__ import annotations

import hepflow


def compilation_provenance() -> dict[str, str]:
    """Return the compile facts recorded in every normalised workflow and plan."""
    return {"flow_version": hepflow.__version__}
