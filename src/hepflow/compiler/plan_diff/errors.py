"""Typed errors raised while comparing plans."""

from __future__ import annotations


class PlanDiffError(Exception):
    """Base class of plan comparison errors; ``code`` is stable for callers."""

    code = "comparison_failed"


class InvalidPlan(PlanDiffError):
    """A plan does not have the structure comparison needs."""

    code = "invalid_plan"

    def __init__(self, message: str, path: str | None = None) -> None:
        self.message = message
        self.path = path
        super().__init__(f"{path}: {message}" if path else message)


class EmptyScopeError(ValueError):
    """A scoped view selects no section at all."""
