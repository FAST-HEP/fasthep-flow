"""Module for defining basic operators."""
from __future__ import annotations

from .base import Operator
from .bash import BashOperator, LocalBashOperator

# only Operator is exposed to the user, everything else is imported directly by the workflow
__all__ = ["BashOperator", "LocalBashOperator", "Operator"]
