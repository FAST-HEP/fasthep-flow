"""Python related operators."""

from __future__ import annotations

from dataclasses import field
from typing import Any

from fasthep_flow.utils import is_valid_import

from .base import Operator, create_operator


class ProcessOperator(Operator):
    data_key: str = "data"
    process_with: dict[str, Any] = field(default_factory=dict)

    def __init__(self, **kwargs: Any):
        self.configure(**kwargs)

    def configure(self, **kwargs: Any) -> None:
        """Configure the operator."""
        self.data_key = kwargs.pop("data_key")
        self.process_with = kwargs.pop("process_with")
        process_type = str(self.process_with.get("type"))
        valid_type = is_valid_import(process_type, aliases={})
        if not valid_type:
            msg = f"Unable to import: {self.process_with.get('type')}"
            raise ImportError(msg)

    def __call__(self, **kwargs: Any) -> dict[str, Any]:
        process_with = create_operator(self.process_with)
        data = kwargs.pop("data")
        value = data.get(self.data_key)
        return process_with(value)  # type: ignore[no-any-return]

    def __repr__(self) -> str:
        return f'ProcessOperator(data_key="{self.data_key}", process_with={self.process_with})'
