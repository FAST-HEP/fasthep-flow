"""Python related operators."""

from __future__ import annotations

import io
from collections.abc import Callable
from contextlib import redirect_stderr, redirect_stdout
from typing import Any

from .base import Operator, ResultType


class PythonOperator(Operator):
    """A Python operator. This operator wraps a Python callable."""

    python_callable: Callable[..., Any]
    arguments: list[Any]

    def __init__(self, **kwargs: Any):
        self.configure(**kwargs)

    def configure(self, **kwargs: Any) -> None:
        """Configure the operator."""
        self.python_callable = kwargs.pop("python_callable")
        self.arguments = kwargs.pop("arguments")

    def __call__(self, **kwargs: Any) -> dict[str, Any]:
        stdout, stderr = io.StringIO(), io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            result = self.python_callable(*self.arguments)
        result = self.python_callable(*self.arguments)
        return ResultType(
            result=result,
            stdout=stdout.getvalue(),
            stderr=stderr.getvalue(),
            exit_code=0,
        ).to_dict()

    def __repr__(self) -> str:
        return f"PythonOperator(python_callable={self.python_callable}, arguments={self.arguments})"
