from __future__ import annotations

from typing import Any

from .base import Operator

from importlib import import_module

class LocalPythonOperator(Operator):
    """A local python operator. This operator runs python callables on the local machine."""

    python_callable: str
    arguments: list[str]

    def __init__(self, **kwargs: Any):
        self.configure(**kwargs)

    def configure(self, **kwargs: Any) -> None:
        """Configure the operator."""
        self.python_callable = kwargs.pop("callable")
        self.arguments = kwargs.pop("arguments")

        for module, alias in kwargs.pop("aliases"):
            if self.python_callable.startswith(alias + '.'):
                self.python_callable = self.python_callable.replace(alias, module, 1)

        # verify valid callable
        try:
            module_name, callable_name = self.python_callable.split('.', 1)
            module = import_module(module_name) # could throw ImportError

            if not callable(getattr(module, callable_name)): # could throw AttributeError
                return # how to fail

        except ImportError:
            return # how should this fail?

        except AttributeError:
            return

        return

    def __call__(self, **kwargs: Any) -> dict[str, Any]:
        stdout, stderr, exit_code = "", "", 0
        return {"stdout": stdout, "stderr": stderr, "exit_code": exit_code}

    def __repr__(self) -> str:
        return f"LocalPythonOperator(callable={self.python_callable}, arguments={self.arguments})"

