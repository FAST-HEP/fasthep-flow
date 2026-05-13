from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import StrEnum
from typing import Any, Final, Literal

from hepflow.model.issues import FlowIssue, IssueLevel
from hepflow.utils import now_iso


class RenderStatus(StrEnum):
    PLANNED = "planned"
    RENDERED = "rendered"
    SKIPPED = "skipped"
    FAILED = "failed"


@dataclass(frozen=True)
class RenderAttempt:
    render_id: str
    when: str
    op: str

    product: str
    input: str
    output: str

    # provenance/debug
    spec: dict[str, Any] | None = None
    select: dict[str, Any] = field(default_factory=dict)

    # outcome
    status: RenderStatus = RenderStatus.PLANNED
    message: str | None = None

    timestamp: str = field(default_factory=now_iso)
    meta: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["status"] = self.status.value
        return d

    @staticmethod
    def from_dict(d: dict[str, Any]) -> RenderAttempt:
        d["status"] = RenderStatus(str(d.get("status", "planned")))
        return RenderAttempt(**d)


@dataclass(frozen=True)
class RenderArtifact:
    path: str
    kind: str | None = None  # e.g. "png", "directory", "json", "pdf"
    role: str | None = None  # e.g. "main", "panel", "status", "thumbnail"
    label: str | None = None  # human-readable label, e.g. dataset name


@dataclass(frozen=True)
class RenderOutcome:
    status: RenderStatus
    message: str | None = None
    artifacts: list[RenderArtifact] = field(default_factory=list)
    meta: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status.value,
            "message": self.message,
            "artifacts": [asdict(a) for a in self.artifacts],
            "meta": self.meta,
        }


# -----------------------------
# Render spec (input to renderers)
# -----------------------------


@dataclass(frozen=True)
class FigureSpec:
    # Matplotlib figsize in inches
    size: tuple[float, float] = (12.0, 8.0)
    dpi: int = 300


@dataclass(frozen=True)
class AxisSpec:
    name: str
    label: str | None = None
    scale: Literal["linear", "log"] = "linear"
    limits: tuple[float, float] | None = None

    def __post_init__(self):
        if self.limits is not None:
            # convert to float and validate
            object.__setattr__(
                self, "limits", (float(self.limits[0]), float(self.limits[1]))
            )
            if self.limits[0] >= self.limits[1]:
                raise ValueError(
                    f"AxisSpec limits should be (min, max) with min < max, got {self.limits}"
                )


@dataclass(frozen=True)
class AxesSpec:
    x: AxisSpec = field(default_factory=lambda: AxisSpec(name="x"))
    y: AxisSpec = field(default_factory=lambda: AxisSpec(name="y"))
    z: AxisSpec | None = None  # heatmap and other 2D plots
    ratio: AxisSpec | None = None  # for ratio panel if enabled

    @staticmethod
    def from_dict(d: dict[str, Any]) -> AxesSpec:
        if not d:
            return AxesSpec()
        return AxesSpec(
            x=AxisSpec(**d.get("x", {})),
            y=AxisSpec(**d.get("y", {})),
            z=AxisSpec(**d.get("z", {})) if d.get("z") else None,
            ratio=AxisSpec(**(d.get("ratio") or {})) if d.get("ratio") else None,
        )


@dataclass(frozen=True)
class LegendSpec:
    loc: str = "upper right"
    ncol: int | None = None  # if None, renderer can auto-pick
    frameon: bool = False
    fontsize: float | None = None


@dataclass(frozen=True)
class DatasetStyle:
    """
    Per-dataset style overrides. Any field left None means "renderer default".
    """

    label: str | None = None
    color: str | None = None
    hatch: str | None = None
    alpha: float | None = None
    # rendering intent hints (renderer may ignore if unsupported)
    kind: Literal["data", "background", "signal"] | None = None
    stack: bool | None = None


@dataclass(frozen=True)
class StyleSpec:
    """
    General style knobs. Keep it minimal and renderer-agnostic.
    """

    experiment: str | None = "CMS"  # used for mplhep label helper selection
    # fb^-1 (or whatever your convention is)
    lumi: float | None = None
    label: str | None = None  # e.g. "Preliminary"
    # Dataset mapping for labels/colors/etc:
    datasets: dict[str, DatasetStyle] = field(default_factory=dict)
    # Renderer can use this as a color cycle for MC if datasets[*].color unset
    color_cycle: list[str] = field(default_factory=list)

    @staticmethod
    def from_dict(d: dict[str, Any]) -> StyleSpec:
        datasets = {k: DatasetStyle(**v) for k, v in d.get("datasets", {}).items()}
        return StyleSpec(
            experiment=d.get("experiment", "CMS"),
            lumi=d.get("lumi"),
            label=d.get("label"),
            datasets=datasets,
            color_cycle=d.get("color_cycle", []),
        )


# -----------------------------
# data_mc plot config
# -----------------------------


@dataclass(frozen=True)
class DataMcInputs:
    """
    Defines how to interpret datasets in a data/MC comparison.
    ids refer to dataset axis category values (i.e. dataset names).
    """

    data: str = "data"
    backgrounds: list[str] = field(default_factory=list)
    signals: list[str] = field(default_factory=list)

    # if true, signals are included in the stack and ratio denom whenever stack=True
    # (renderer can implement "whatever is in MC stack goes into ratio")
    include_signals_in_stack: bool = True

    # if true, show combined MC uncertainty band on main panel (and ratio)
    show_mc_uncertainty: bool = True

    stack: bool = True
    stack_order: Literal["legend", "reverse_legend"] = "reverse_legend"
    ratio: bool = True

    # histtype defaults
    histtype_mc: Literal["fill", "step", "bar"] = "fill"
    histtype_signal: Literal["step", "fill", "bar"] = "step"

    # cosmetics / policy
    legend_auto_ncol_threshold: int = 5  # if >5 entries, use multi-column
    legend_max_ncol: int = 4  # cap

    # ratio panel details (optional)
    ratio_ylim: tuple[float, float] = (0.5, 1.5)
    ratio_ylabel: str = "Data/MC"



@dataclass(frozen=True)
class Heatmap2DSpec:
    # how to split outputs
    per_dataset: bool = True
    # mplhep.hist2dplot knobs (minimal now; can expand later)
    cbar: bool = True
    flow: bool = False
    density: bool = False


@dataclass(frozen=True)
class ProjectSpec:
    # axis name in the hist ("x", "y", "reco_et", etc) to keep
    axis: str
    # if present, project within each dataset category (usually yes)
    keep_dataset: bool = True
    # the next render spec after projection (must be resolved in plan.yaml)
    # store as dict in plan.yaml; convert to RenderSpec at runtime
    then: RenderSpec | dict[str, Any] | None = None

    @staticmethod
    def from_dict(d: dict[str, Any]) -> ProjectSpec:
        then_raw = d.get("then")
        then = RenderSpec.from_dict(then_raw) if isinstance(then_raw, dict) else None
        return ProjectSpec(
            axis=str(d.get("axis")),
            keep_dataset=bool(d.get("keep_dataset", True)),
            then=then,
        )


@dataclass(frozen=True)
class GroupTransformSpec:
    """
    Group sample-level histogram categories into larger groups.

    `by` can be:
      - "dataset_group" for automatic grouping from dataset metadata
      - dict[str, list[str]] mapping group name -> dataset names
    """
    by: str | dict[str, list[str]]
    kind: Final[str] = "group"

    @staticmethod
    def from_dict(d: dict[str, Any] | None) -> GroupTransformSpec | None:
        if not d:
            return None
        return GroupTransformSpec(by=d["by"])


@dataclass(frozen=True)
class ScaleTransformSpec:
    by: Literal["dataset_name", "dataset_group"]
    mode: Literal["overall", "bin_by_bin"] = "overall"
    factors: dict[str, Any] = field(default_factory=dict)
    factors_ref: str | None = None
    kind: Final[str] = "scale"

    @staticmethod
    def from_dict(d: dict[str, Any] | None) -> ScaleTransformSpec | None:
        if not d:
            return None
        return ScaleTransformSpec(**dict(d))


@dataclass(frozen=True)
class TransformSpec:
    kind: Literal["group", "scale"]
    group: GroupTransformSpec | None = None
    scale: ScaleTransformSpec | None = None

    @staticmethod
    def from_dict(d: dict[str, Any] | None) -> TransformSpec | None:
        if not d:
            return None
        d = dict(d)

        kind = d.pop("kind")
        if kind == "group":
            group_block = d.get("group")
            if group_block is None:
                group_block = {"by": d.get("by")}
            return TransformSpec(
                kind="group",
                group=GroupTransformSpec.from_dict(group_block),
            )
        if kind == "scale":
            return TransformSpec(
                kind="scale",
                scale=ScaleTransformSpec.from_dict(d),
            )
        raise ValueError(f"Unknown transform kind: {kind}")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# -----------------------------
# Top-level render spec
# -----------------------------


RenderPlotKind = Literal[
    "plot1d",
    "plot2d",
    "data_mc",
    "heatmap2d",
    "project",
    "comparison",
]


@dataclass(frozen=True)
class RenderSpec:
    """
    This is the stable interface between plan.yaml and any renderer.

    - 'plot' chooses the rendering algorithm.
    - 'axes' defines labels/scales/limits.
    - 'style' defines experiment styling + dataset color/label overrides.
    - 'extensions' is the escape hatch for renderer-specific knobs.
    """

    op: str

    figure: FigureSpec = field(default_factory=FigureSpec)
    axes: AxesSpec | None = None

    # Selection is already part of RenderPlan, but allowing here is useful for external renderers.
    select: dict[str, Any] = field(default_factory=dict)

    legend: LegendSpec = field(default_factory=LegendSpec)
    style: StyleSpec = field(default_factory=StyleSpec)
    # plot-specific configs:
    # plot="data_mc"
    data_mc: DataMcInputs | None = None
    # plot="heatmap2d"
    heatmap2d: Heatmap2DSpec | None = None
    # plot="project"
    project: ProjectSpec | None = None
    # plot="comparison"
    comparison: ComparisonSpec | None = None

    transforms: list[TransformSpec] = field(default_factory=list)

    # Renderer-specific options
    extensions: dict[str, Any] = field(default_factory=dict)

    @property
    def plot(self) -> str:
        return self.op

    def __post_init__(self):
        if self.plot == "data_mc":
            if self.data_mc is None:
                raise ValueError("RenderSpec(plot='data_mc') requires data_mc")
            if not self.data_mc.data:
                raise ValueError("RenderSpec.data_mc.data must be non-empty")
            # optional: ensure no duplicates
            if len(set(self.data_mc.backgrounds)) != len(self.data_mc.backgrounds):
                raise ValueError("data_mc.backgrounds contains duplicates")
        elif self.plot == "project":
            if self.project is None:
                raise ValueError("RenderSpec(plot='project') requires project")
            if not self.project.axis:
                raise ValueError("RenderSpec.project.axis must be set")
            if self.project.then is None:
                raise ValueError("RenderSpec(plot='project') requires project.then")
        elif self.plot == "comparison":
            if self.comparison is None:
                raise ValueError("RenderSpec(plot='comparison') requires comparison")
            if self.axes is None or self.axes.x is None:
                raise ValueError("RenderSpec(plot='comparison') requires axes.x")
        for t in self.transforms:
            if t is None:
                raise ValueError("RenderSpec.transforms must not contain null entries")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @staticmethod
    def from_dict(d: dict[str, Any]) -> RenderSpec:
        d = dict(d)
        plot = d.get("plot")

        d["figure"] = FigureSpec(**d.get("figure", {}))
        d["axes"] = AxesSpec.from_dict(d["axes"]) if "axes" in d else {}
        d["legend"] = LegendSpec(**d.get("legend", {}))
        d["style"] = StyleSpec.from_dict(d.get("style", {}))
        if plot == "data_mc":
            d["data_mc"] = DataMcInputs(**d["data_mc"])
        if plot == "heatmap2d":
            d["heatmap2d"] = Heatmap2DSpec(**d["heatmap2d"])
        if plot == "project":
            d["project"] = ProjectSpec.from_dict(d["project"])
        if plot == "comparison":
            d["comparison"] = ComparisonSpec.from_dict(d["comparison"])

        raw_transforms = d.get("transforms") or []
        d["transforms"] = [TransformSpec.from_dict(t) for t in raw_transforms]
        return RenderSpec(
            **d,
        )

    @staticmethod
    def validate_datasets_for_data_mc(
        spec: RenderSpec, available_datasets: list[str]
    ) -> FlowIssue | None:
        if spec.plot != "data_mc":
            return None
        dm = spec.data_mc
        if dm is None:
            return FlowIssue(
                level=IssueLevel.ERROR,
                code="RENDER_SPEC_MISSING_DATA_MC",
                message="RenderSpec with plot='data_mc' requires data_mc field to be set",
                meta={},
            )
        refs = [dm.data, *dm.signals, *dm.backgrounds]
        missing = [x for x in refs if x not in available_datasets]
        if missing:
            return FlowIssue(
                level=IssueLevel.ERROR,
                code="RENDER_SPEC_MISSING_DATASETS",
                message=f"RenderSpec data_mc references datasets not in data.datasets: {sorted(set(missing))}. "
                f"Known datasets: {sorted(available_datasets)}",
                meta={"missing": missing, "available": available_datasets},
            )
        return None

    @staticmethod
    def validate_products_for_comparison(
        spec: RenderSpec,
        available_products: list[str],
    ) -> FlowIssue | None:
        if spec.plot != "comparison":
            return None
        cmp = spec.comparison
        if cmp is None:
            return FlowIssue(
                level=IssueLevel.ERROR,
                code="RENDER_SPEC_MISSING_COMPARISON",
                message="RenderSpec with plot='comparison' requires comparison field to be set",
                meta={},
            )
        refs = [cmp.reference, cmp.target]
        missing = [x for x in refs if x not in available_products]
        if missing:
            return FlowIssue(
                level=IssueLevel.ERROR,
                code="RENDER_SPEC_MISSING_PRODUCTS",
                message=(
                    f"RenderSpec comparison references products not in plan.products: "
                    f"{sorted(set(missing))}. Known products: {sorted(available_products)}"
                ),
                meta={"missing": missing, "available": available_products},
            )
        return None

    @staticmethod
    def validate_group_transforms(
        spec: RenderSpec,
        available_datasets: list[str],
    ) -> FlowIssue | None:
        if not spec.transforms:
            return None

        known = set(available_datasets)

        for t in spec.transforms:
            if t.kind != "group" or t.group is None:
                continue

            by = t.group.by
            if isinstance(by, dict):
                missing: list[str] = []
                for _, members in by.items():
                    for ds in members:
                        if ds not in known:
                            missing.append(ds)
                if missing:
                    return FlowIssue(
                        level=IssueLevel.ERROR,
                        code="RENDER_TRANSFORM_GROUP_UNKNOWN_DATASET",
                        message=(
                            f"Group transform references datasets not present in plan.datasets: "
                            f"{sorted(set(missing))}"
                        ),
                        meta={
                            "missing": sorted(set(missing)),
                            "available": sorted(available_datasets),
                        },
                    )

            elif isinstance(by, str):
                if by != "dataset_group":
                    return FlowIssue(
                        level=IssueLevel.ERROR,
                        code="RENDER_TRANSFORM_GROUP_UNKNOWN_MODE",
                        message=f"Unsupported group transform mode: {by!r}",
                        meta={"supported": ["dataset_group"]},
                    )

        return None


@dataclass(frozen=True)
class ComparisonSpec:
    reference: str
    target: str

    reference_label: str = "reference"
    target_label: str = "target"

    comparison: Literal[
        "ratio",
        "split_ratio",
        "pull",
        "difference",
        "relative_difference",
        "efficiency",
        "asymmetry",
    ] = "ratio"

    comparison_ylabel: str | None = None
    comparison_ylim: tuple[float, float] | None = None

    w2method: Literal["sqrt", "poisson"] = "sqrt"
    flow: Literal["hint", "show", "none"] = "hint"

    @staticmethod
    def from_dict(d: dict[str, Any] | None) -> ComparisonSpec | None:
        if not d:
            return None
        return ComparisonSpec(**dict(d))

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
