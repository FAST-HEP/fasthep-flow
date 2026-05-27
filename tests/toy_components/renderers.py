from __future__ import annotations

from typing import Any

from hepflow.model.render import RenderOutcome, RenderStatus
from hepflow.model.render_types import RenderCommonSpec, RenderTypeSpec


def _parse_params(params: dict[str, Any]) -> dict[str, Any]:
    return dict(params)


def _validate(
    common: RenderCommonSpec,
    params: Any,
    ctx: dict[str, Any],
) -> list[Any]:
    return []


def _resolve_input(
    common: RenderCommonSpec,
    params: Any,
    ctx: dict[str, Any],
) -> dict[str, Any]:
    return {}


TOY_RENDER_TYPE = RenderTypeSpec(
    parse_params=_parse_params,
    validate=_validate,
    resolve_input=_resolve_input,
)


def render_toy(
    product: dict[str, Any],
    common: RenderCommonSpec,
    params: Any,
    ctx: dict[str, Any],
) -> RenderOutcome:
    return RenderOutcome(
        status=RenderStatus.RENDERED,
        message="rendered",
        meta={"product_keys": sorted(product)},
    )
