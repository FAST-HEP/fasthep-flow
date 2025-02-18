from __future__ import annotations

from typing import Any

from fasthep_flow.plugins import PrintPlugin, task_wrapper
from fasthep_flow.plugins._base import PluginInterface


def function_for_test(*args, **kwargs) -> dict[str, Any]:
    return {"args": args, "kwargs": kwargs}


def test_task_wrapper_result_unchanged():
    result_unwrapped = function_for_test(1, 2, 3, a=4, b=5)
    wrapepd_func = task_wrapper(function_for_test)
    result = wrapepd_func(1, 2, 3, a=4, b=5)
    assert result == result_unwrapped
    assert wrapepd_func.__wrapped__ is function_for_test


def test_task_wrapper(capsys):
    result_unwrapped = function_for_test(1, 2, 3, a=4, b=5)
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""
    plugins: dict[str, list[PluginInterface]] = {"function_for_test": [PrintPlugin()]}
    result = task_wrapper(function_for_test, plugins=plugins)(1, 2, 3, a=4, b=5)
    assert result == result_unwrapped
    captured = capsys.readouterr()
    assert (
        captured.out
        == "Running function_for_test with args (1, 2, 3) and kwargs {'a': 4, 'b': 5}\nFinished running function_for_test with result {'args': (1, 2, 3), 'kwargs': {'a': 4, 'b': 5}}\n"
    )
