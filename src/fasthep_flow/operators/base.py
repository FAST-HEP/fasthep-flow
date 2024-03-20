"""Definition of the Operator protocol."""
from __future__ import annotations

from typing import Any, Protocol


class Operator(Protocol):
    """The base class for all operators.
    Only named parameters are allowed,
    since we need to have a way to pass the YAML configuration to the operator.
    """

    def __call__(self, **kwargs: Any) -> dict[str, Any]:
        ...

    def __repr__(self) -> str:
        ...

    def configure(self, **kwargs: Any) -> None:
        """General function to configure the operator."""
