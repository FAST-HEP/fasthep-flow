from __future__ import annotations

from typing import Any

from .base import Operator

try:
    # try to import plumbum
    import plumbum
except ImportError as e:
    # if it fails, raise an error
    error_message = "The plumbum package is required for the (Local|Remote)BashOperator"
    raise ImportError(error_message) from e


class LocalBashOperator(Operator):
    bash_command: str
    arguments: list[str]

    def __init__(self, **kwargs: Any):
        self.configure(**kwargs)

    def configure(self, **kwargs: Any) -> None:
        self.command = kwargs.pop("bash_command")
        self.arguments = kwargs.pop("arguments")

    def __call__(self, **kwargs: Any) -> dict[str, Any]:
        command = plumbum.local[self.command]
        exit_code, stdout, stderr = command.run(*self.arguments)
        return {"stdout": stdout, "stderr": stderr, "exit_code": exit_code}

    def __repr__(self) -> str:
        return f"LocalBashOperator(bash_command={self.command}, arguments={self.arguments})"


BashOperator = LocalBashOperator
