from __future__ import annotations

from hamilton.registry import register_adapter

from ._hamilton import DATA_ADAPTERS

registered = False

if not registered:
    for adapter in DATA_ADAPTERS:
        register_adapter(adapter)

registered = True
