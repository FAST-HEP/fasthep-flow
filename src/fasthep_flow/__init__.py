"""
Copyright (c) 2023 Luke Kreczko. All rights reserved.

fasthep-flow: Convert YAML into a workflow DAG
"""


from __future__ import annotations

from ._version import version as __version__
from .config import FlowConfig
from .workflow import Workflow

__all__ = ("__version__", "FlowConfig", "Workflow")
