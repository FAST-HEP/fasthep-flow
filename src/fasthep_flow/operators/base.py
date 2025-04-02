"""Definition of the Operator protocol."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

from fasthep_flow.utils import instance_from_type_string


class Operator(Protocol):
    """The base class for all operators.
    Only named parameters are allowed,
    since we need to have a way to pass the YAML configuration to the operator.
    """

    def __call__(self, **kwargs: Any) -> dict[str, Any]:
        ...

    def __repr__(self) -> str:
        ...

    def configure(self, *args: Any, **kwargs: Any) -> None:
        """General function to configure the operator."""


@dataclass
class ResultType:
    """The result type of an operator. Can add validation here if needed."""

    result: Any
    stdout: str
    stderr: str
    exit_code: int

    def to_dict(self) -> dict[str, Any]:
        """Convert the ResultType to a dictionary."""
        return {
            "result": self.result,
            "stdout": self.stdout,
            "stderr": self.stderr,
            "exit_code": self.exit_code,
        }


def create_operator(config: dict[str, Any]) -> Callable[..., Any]:
    """Create an operator from a configuration dictionary."""
    operator_type = config.pop("type")
    kwargs = config.pop("kwargs", {})
    instance = instance_from_type_string(operator_type, **kwargs)
    if not callable(instance):
        msg = f"Instance is not a valid operator: {instance} (needs to be callable)"
        raise ValueError(msg)
    return instance  # type: ignore[no-any-return]
