from dataclasses import dataclass
from typing import Any, Callable

from hepflow.model.render_types import RenderCommonSpec, RenderTypeSpec

RenderHandler = Callable[[dict[str, Any], RenderCommonSpec, Any, dict[str, Any]], Any]


@dataclass(frozen=True)
class RenderEntry:
    spec: RenderTypeSpec
    handler: RenderHandler