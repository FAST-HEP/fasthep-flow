from __future__ import annotations

from typing import Any

from .base import Operator

try:
    # try to import plumbum
    import plumbum
except ImportError as e:
    # if it fails, raise an error
    msg = "The plumbum package is required for the (Local|Remote)BashOperator"  # pylint: disable=invalid-name
    raise ImportError(msg) from e


class LocalBashOperator(Operator):
    """A local bash operator. This operator runs a bash command on the local machine."""

    bash_command: str
    arguments: list[str]

    def __init__(self, **kwargs: Any):
        self.configure(**kwargs)

    def configure(self, **kwargs: Any) -> None:
        """Configure the operator."""
        self.bash_command = kwargs.pop("bash_command")
        self.arguments = kwargs.pop("arguments")

    def __call__(self, **kwargs: Any) -> dict[str, Any]:
        command = plumbum.local[self.bash_command]
        exit_code, stdout, stderr = command.run(*self.arguments)
        return {"stdout": stdout, "stderr": stderr, "exit_code": exit_code}

    def __repr__(self) -> str:
        return f"LocalBashOperator(bash_command={self.bash_command}, arguments={self.arguments})"


BashOperator = LocalBashOperator
