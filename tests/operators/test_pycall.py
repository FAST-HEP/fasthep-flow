from __future__ import annotations

from fasthep_flow.operators.py_call import PythonOperator


def test_pycall_operator():
    operator = PythonOperator(python_callable="print", arguments=["Hello World!"])
    result = operator()
    assert result["stdout"] == "Hello World!\n"
    assert result["stderr"] == ""
