from __future__ import annotations

from fasthep_flow.operators.python import PythonOperator


def test_python_operator():
    operator = PythonOperator(callable="print", arguments=['"Hello World!"'])
    result = operator()
    assert result["stdout"] == "Hello World!\n"
    assert result["stderr"] == ""

def test_python_operator_aliased():
    operator = PythonOperator(callable="lambda x: print(np.count_nonzero(x))", arguments=["[0,1,1]"], aliases={'numpy': 'np'})
    result = operator()
    assert result["stdout"] == "2\n"
    assert result["stderr"] == ""

def test_python_operator_import_error():
    operator = PythonOperator(callable="lambda x: print(np.count_nonzero(x))", arguments=["[0,1,1]"])
    result = operator()
    assert result["stdout"] == ""
    assert result["stderr"] == "ImportError"

def test_python_operator_attrib_error():
    operator = PythonOperator(callable="lambda x: print(np.nonimplemented(x))", arguments=["[0,1,1]"])
    result = operator()
    assert result["stdout"] == ""
    assert result["stderr"] == "AttributeError"

def test_python_operator_callable_failure():
    operator = PythonOperator(callable="print(\"Hello World!\n\")")
    result = operator()
    assert result["stdout"] == ""
    assert result["stderr"] == "AttributeError, not callable"
