from __future__ import annotations

import importlib.metadata

import fasthep_flow as m


def test_version():
    assert importlib.metadata.version("fasthep_flow") == m.__version__
