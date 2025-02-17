from __future__ import annotations

import pytest

from fasthep_flow import utils


def function_for_hash_test(*args, **kwargs):
    for arg in args:
        print(arg)
    for key, value in kwargs.items():
        print(key, value)


@pytest.fixture()
def tmp_cfg(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text("test")
    return config_file


def test_get_config_hash(tmp_cfg):
    assert utils.get_config_hash(tmp_cfg) == "9f86d081"


def test_generate_save_path(tmp_path, tmp_cfg):
    base_path = tmp_path / "base"
    base_path.mkdir()
    workflow_name = "test"

    save_path = utils.generate_save_path(base_path, workflow_name, tmp_cfg)
    today = utils.formatted_today()
    assert save_path == base_path / f"{workflow_name}/{today}/9f86d081/"


def test_calculate_function_hash():
    args = ["test", "test2"]
    kwargs = {"key": "value", "key2": "value2"}
    hash = utils.calculate_function_hash(function_for_hash_test, *args, **kwargs)
    assert hash == "3152412c"

    args = ["test", "test3"]
    hash = utils.calculate_function_hash(function_for_hash_test, *args, **kwargs)
    assert hash == "c42147f9"
