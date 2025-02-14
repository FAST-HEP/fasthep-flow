from __future__ import annotations

from fasthep_flow.operators.bash import BashOperator


def test_bash_operator():
    operator = BashOperator(bash_command="echo", arguments=["Hello World!"])
    result = operator()
    assert result["stdout"] == "Hello World!"
    assert result["stderr"] == ""
