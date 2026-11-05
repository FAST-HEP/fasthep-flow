"""Typed errors raised while comparing plans."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path


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


class InputNotFound(PlanDiffError):
    """A plan file or a declared plan does not exist."""

    code = "input_not_found"

    def __init__(self, path: str | Path) -> None:
        self.path = path
        super().__init__(f"plan input not found: {path}")


class NoPlanFound(PlanDiffError):
    """A directory holds no plan to compare."""

    code = "no_plan_found"

    def __init__(self, path: str | Path) -> None:
        self.path = path
        super().__init__(f"no plan found in {path}")


class AmbiguousPlans(PlanDiffError):
    """A directory declares several plans; the caller must name one."""

    code = "ambiguous_plans"

    def __init__(
        self, path: str | Path, candidates: Sequence[tuple[str, Path]]
    ) -> None:
        self.path = path
        self.candidates = tuple(candidates)
        listing = "; ".join(f"{name}: {plan}" for name, plan in self.candidates)
        super().__init__(
            f"{path} declares several plans ({listing}); pass an explicit plan file"
        )
