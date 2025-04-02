"""Python related operators."""

from __future__ import annotations

import io
from collections.abc import Callable
from contextlib import redirect_stderr, redirect_stdout
from typing import Any

from loguru import logger

from fasthep_flow.utils import get_callable

from .base import Operator, ResultType


class PythonOperator(Operator):
    """A Python operator. This operator wraps a Python callable."""

    python_callable: Callable[..., Any]
    arguments: tuple[Any, ...] | None

    def __init__(self, **kwargs: Any):
        self.configure(**kwargs)

    def configure(self, **kwargs: Any) -> None:
        """Configure the operator."""
        python_callable = kwargs.pop("python_callable")
        pycall = get_callable(python_callable)
        if pycall is None:
            msg = f"python_callable must be a callable: {python_callable} not found."
            raise ValueError(msg)
        self.python_callable = pycall

        self.arguments = kwargs.get("arguments", [])

    def __call__(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        if args and not self.arguments:
            self.arguments = args
        stdout, stderr = io.StringIO(), io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            result = self.python_callable(*self.arguments)  # type: ignore[misc]

        stdout_value = stdout.getvalue()
        stderr_value = stderr.getvalue()
        if stdout_value:
            logger.info(stdout_value)
        if stderr_value:
            logger.error(stderr_value)

        return ResultType(
            result=result,
            stdout=stdout_value,
            stderr=stderr_value,
            exit_code=0,
        ).to_dict()

    def __repr__(self) -> str:
        return f"PythonOperator(python_callable={self.python_callable}, arguments={self.arguments})"
