from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from hepflow.compiler.plan import build_plan_from_normalized
from hepflow.model.plan import ExecutionPlan
from hepflow.utils import write_yaml


@dataclass(slots=True)
class VariationContext:
    name: str
    group: str | None = None
    direction: str | None = None
    is_nominal: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        data = {
            "name": self.name,
            "is_nominal": self.is_nominal,
        }
        if self.group is not None:
            data["group"] = self.group
        if self.direction is not None:
            data["direction"] = self.direction
        data.update(deepcopy(self.metadata))
        return data


@dataclass(slots=True)
class ExpandedWorkflow:
    variation: VariationContext
    workflow: dict[str, Any]


def expand_systematics(normalized: dict[str, Any]) -> list[ExpandedWorkflow]:
    """Expand a normalized author workflow into variation-tagged copies."""
    systematics = normalized.get("systematics")
    if systematics is None:
        return [_expanded_nominal(normalized)]
    if not isinstance(systematics, dict):
        raise ValueError("systematics must be a mapping")

    expanded: list[ExpandedWorkflow] = []
    if systematics.get("include_nominal") is True:
        expanded.append(_expanded_nominal(normalized))

    variations = systematics.get("variations") or []
    if not isinstance(variations, list):
        raise ValueError("systematics.variations must be a list")

    for raw_variation in variations:
        if not isinstance(raw_variation, dict):
            raise ValueError("systematics.variations entries must be mappings")
        variation = _variation_context(raw_variation)
        workflow = deepcopy(normalized)
        workflow["variation"] = variation.to_dict()
        apply_weight_variation(workflow, variation)
        expanded.append(ExpandedWorkflow(variation=variation, workflow=workflow))

    return expanded


def apply_weight_variation(
    workflow: dict[str, Any], variation: VariationContext
) -> dict[str, Any]:
    if variation.is_nominal:
        return workflow

    multipliers = _weight_multipliers(variation)
    if not multipliers:
        return workflow

    rewrites: list[dict[str, Any]] = []
    analysis = workflow.get("analysis") or {}
    if not isinstance(analysis, dict):
        return workflow

    stages = analysis.get("stages") or []
    if not isinstance(stages, list):
        return workflow

    for stage in stages:
        if not isinstance(stage, dict):
            continue
        params = stage.get("params")
        if not isinstance(params, dict) or "weight_expr" not in params:
            continue

        original = params.get("weight_expr")
        if original is None:
            continue

        rewritten = _multiply_weight_expr(str(original), multipliers)
        params["weight_expr"] = rewritten
        rewrites.append(
            {
                "stage": str(stage.get("id") or ""),
                "original": original,
                "rewritten": rewritten,
                "multipliers": list(multipliers),
            }
        )

    if rewrites:
        variation_block = workflow.setdefault("variation", variation.to_dict())
        if isinstance(variation_block, dict):
            variation_block.setdefault("rewrites", {})["weight_expr"] = rewrites

    return workflow


def make_systematic_plan_files(
    normalized: dict[str, Any],
    *,
    outdir: Path,
    chunk_size: int | None = None,
) -> ExecutionPlan:
    expanded = expand_systematics(normalized)
    if not expanded:
        raise ValueError("systematics expansion produced no workflows")

    compile_path = outdir / "compile"
    first_plan: ExecutionPlan | None = None
    variation_summaries: list[dict[str, Any]] = []
    summary: dict[str, Any] = {
        "include_nominal": bool(
            (normalized.get("systematics") or {}).get("include_nominal", False)
        ),
        "variations": variation_summaries,
    }

    for item in expanded:
        variation_name = item.variation.name
        variation_dir = compile_path / variation_name
        variation_dir.mkdir(parents=True, exist_ok=True)

        _graph, plan = build_plan_from_normalized(
            item.workflow,
            chunk_size=chunk_size,
        )
        if first_plan is None:
            first_plan = plan

        write_yaml(item.workflow, str(variation_dir / "normalized.yaml"))
        write_yaml(plan.to_dict(), str(variation_dir / "plan.yaml"))

        variation_summary = item.variation.to_dict()
        variation_summary["plan"] = str(
            (variation_dir / "plan.yaml").relative_to(outdir)
        )
        variation_summaries.append(variation_summary)

    write_yaml(summary, str(compile_path / "systematics.yaml"))
    if first_plan is None:
        raise ValueError("systematics expansion produced no plans")
    return first_plan


def _expanded_nominal(normalized: dict[str, Any]) -> ExpandedWorkflow:
    variation = VariationContext(name="nominal", is_nominal=True)
    workflow = deepcopy(normalized)
    workflow["variation"] = variation.to_dict()
    return ExpandedWorkflow(variation=variation, workflow=workflow)


def _variation_context(raw_variation: dict[str, Any]) -> VariationContext:
    name = raw_variation.get("name")
    if not isinstance(name, str) or not name.strip():
        raise ValueError("systematics.variations[].name is required")

    group = raw_variation.get("group")
    direction = raw_variation.get("direction")
    metadata = {
        key: deepcopy(value)
        for key, value in raw_variation.items()
        if key not in {"name", "group", "direction"}
    }
    return VariationContext(
        name=name.strip(),
        group=group if isinstance(group, str) else None,
        direction=direction if isinstance(direction, str) else None,
        is_nominal=False,
        metadata=metadata,
    )


def _weight_multipliers(variation: VariationContext) -> list[str]:
    weight = variation.metadata.get("weight")
    if not isinstance(weight, dict):
        return []
    multiply = weight.get("multiply") or []
    if not isinstance(multiply, list):
        return []
    return [item.strip() for item in multiply if isinstance(item, str) and item.strip()]


def _multiply_weight_expr(weight_expr: str, multipliers: list[str]) -> str:
    pieces = [f"({weight_expr})"]
    pieces.extend(f"({multiplier})" for multiplier in multipliers)
    return " * ".join(pieces)
