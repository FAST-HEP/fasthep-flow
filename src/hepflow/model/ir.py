# hepflow/model/ir.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class InputRef:
    # exactly one of:
    stream: str | None = None # external stream id
    node: str | None = None   # upstream node id
    port: str | None = None   # upstream port name
    as_: str | None = None    # alias inside runtime inputs dict

    def __post_init__(self) -> None:
        if self.stream:
            if self.node or self.port:
                raise ValueError(
                    "InputRef: stream is mutually exclusive with node/port")
        else:
            if not (self.node and self.port):
                raise ValueError(
                    "InputRef: must provide either stream OR (node AND port)")

    def to_dict(self) -> dict[str, Any]:
        d = {}
        if self.stream is not None:
            d["stream"] = self.stream
        else:
            d["node"] = self.node
            d["port"] = self.port
        if self.as_:
            d["as"] = self.as_
        return d
    
    @staticmethod
    def from_dict(d: dict[str, Any]) -> "InputRef":
        d = dict(d)
        if "as" in d:
            d["as_"] = d.pop("as")
        return InputRef(**d)
