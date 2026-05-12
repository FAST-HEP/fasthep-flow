from dataclasses import dataclass, field
from typing import Any, Callable

from hepflow.model.issues import FlowIssue
from hepflow.model.render import FigureSpec, AxesSpec, LegendSpec, StyleSpec, TransformSpec


@dataclass(frozen=True)
class RenderCommonSpec:
    figure: FigureSpec = field(default_factory=FigureSpec)
    axes: AxesSpec = field(default_factory=AxesSpec)
    select: dict[str, Any] = field(default_factory=dict)
    legend: LegendSpec = field(default_factory=LegendSpec)
    style: StyleSpec = field(default_factory=StyleSpec)
    transforms: list[TransformSpec] = field(default_factory=list)
    extensions: dict[str, Any] = field(default_factory=dict)

    @staticmethod
    def from_dict(d: dict[str, Any] | None) -> "RenderCommonSpec":
        d = dict(d or {})
        return RenderCommonSpec(
            figure=FigureSpec(**(d.get("figure") or {})),
            axes=AxesSpec.from_dict(d.get("axes") or {}),
            select=dict(d.get("select") or {}),
            legend=LegendSpec(**(d.get("legend") or {})),
            style=StyleSpec.from_dict(d.get("style") or {}),
            transforms=[TransformSpec.from_dict(t) for t in (d.get("transforms") or [])],
            extensions=dict(d.get("extensions") or {}),
        )


RenderParamsParser = Callable[[dict[str, Any]], Any]
RenderValidator = Callable[[RenderCommonSpec, Any, dict[str, Any]], list[FlowIssue]]
RenderHandler = Callable[[dict[str, Any], RenderCommonSpec, Any, dict[str, Any]], Any]


RenderInputResolver = Callable[
    [RenderCommonSpec, Any, dict[str, Any]],
    dict[str, Any],
]

@dataclass(frozen=True)
class RenderTypeSpec:
    parse_params: RenderParamsParser
    validate: RenderValidator
    resolve_input: RenderInputResolver


@dataclass(frozen=True)
class RenderEntry:
    spec: RenderTypeSpec
    handler: RenderHandler