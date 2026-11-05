"""Compare compiled plans: semantic and provenance-aware differences."""

from __future__ import annotations

from hepflow.compiler.plan_diff.compare import diff_plan_files, diff_plans
from hepflow.compiler.plan_diff.errors import (
    AmbiguousPlans,
    EmptyScopeError,
    InputNotFound,
    InvalidPlan,
    NoPlanFound,
    PlanDiffError,
)
from hepflow.compiler.plan_diff.model import (
    DiffKind,
    DiffSection,
    PlanDiffEntry,
    PlanDiffReport,
    PlanDiffScope,
    PlanDiffWarning,
)

__all__ = [
    "AmbiguousPlans",
    "DiffKind",
    "DiffSection",
    "EmptyScopeError",
    "InputNotFound",
    "InvalidPlan",
    "NoPlanFound",
    "PlanDiffEntry",
    "PlanDiffError",
    "PlanDiffReport",
    "PlanDiffScope",
    "PlanDiffWarning",
    "diff_plan_files",
    "diff_plans",
]
