from __future__ import annotations

import ast
from collections.abc import Callable
from typing import Any

from .base import Operator


class LocalPythonOperator(Operator):
    """A local python operator. This operator runs python callables on the local machine."""

    callable: str | Callable[Any, Any]
    arguments: list[str]

    def __init__(self, **kwargs: Any):
        self.configure(**kwargs)

    def configure(self, **kwargs: Any) -> None:
        """
        Configure the operator. Allows for unqualified names as well as
        qualified names via imports and aliased imports
        """
        self.callable = kwargs["callable"]
        self.arguments = kwargs.pop("arguments", None)

        # # WIP: this could later be replaced as parsing is updated
        # for module, alias in kwargs.pop("aliases"):
        #     if self.callable.startswith(alias + '.'):
        #         self.callable = self.callable.replace(alias, module, 1)

        if isinstance(self.callable, str):
            obj = None
            # breakpoint()
            try:
                obj = eval(compile(self.callable, '<string>', 'eval'),
                           globals(),
                           locals())
            except (SyntaxError, AttributeError) as e:
                raise e

            if not callable(obj):
                msg = f"provided string `{self.callable}` did not compile to a callable"
                raise AttributeError(msg)

            self.callable = obj
        return

    def __call__(self, **kwargs: Any) -> dict[str, Any]:
        stdout, stderr, exit_code = "", "", 0
        return {"stdout": stdout, "stderr": stderr, "exit_code": exit_code}

    def __repr__(self) -> str:
        return f"LocalPythonOperator(callable={self.python_callable}, arguments={self.arguments})"

PythonOperator = LocalPythonOperator
