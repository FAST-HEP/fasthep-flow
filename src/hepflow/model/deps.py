# hepflow/compiler/deps_model.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class RequiredInput:
    kind: str                # "root_tree" for now
    tree: str                # TTree name inside ROOT file
    branches: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "tree": self.tree,
            "branches": list(self.branches),
        }


@dataclass(frozen=True)
class Deps:
    # Debug/reasoning
    node_order: tuple[str, ...]

    required_symbols_per_node: dict[str, tuple[str, ...]]
    provides_symbols_per_node: dict[str, tuple[str, ...]]

    external_symbols: tuple[str, ...]
    unresolved_external_symbols: tuple[str, ...]

    # IO plan: stream_id -> RequiredInput
    required_inputs: dict[str, RequiredInput]

    # Context
    context_symbols: tuple[str, ...]
    primary_stream: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "primary_stream": self.primary_stream,
            "context_symbols": list(self.context_symbols),
            "required_symbols_per_node": {k: list(v) for k, v in self.required_symbols_per_node.items()},
            "provides_symbols_per_node": {k: list(v) for k, v in self.provides_symbols_per_node.items()},
            "external_symbols": list(self.external_symbols),
            "unresolved_external_symbols": list(self.unresolved_external_symbols),
            "required_inputs": {sid: ri.to_dict() for sid, ri in self.required_inputs.items()},
        }

    @property
    def symbol_upstreams_per_node(self) -> dict[str, tuple[str, ...]]:
        """
        For each node, list upstream nodes that provide at least one symbol it requires.
        Uses node_order for directionality.
        """
        order_index = {nid: i for i, nid in enumerate(self.node_order)}

        # symbol -> latest provider node (in order)
        provider_of: dict[str, str] = {}

        out: dict[str, tuple[str, ...]] = {}
        for nid in self.node_order:
            req = self.required_symbols_per_node.get(nid, ())
            needed_up: dict[str, None] = {}  # ordered set via dict

            for s in req:
                p = provider_of.get(s)
                if p is not None and order_index[p] < order_index[nid]:
                    needed_up[p] = None

            out[nid] = tuple(needed_up.keys())

            # update providers after computing upstreams
            for s in self.provides_symbols_per_node.get(nid, ()):
                provider_of[s] = nid

        return out

    @property
    def stream_upstream_per_node(self) -> dict[str, tuple[str, ...]]:
        """
        v2.1 approximation: stream flows linearly in IR order.
        """
        out: dict[str, tuple[str, ...]] = {}
        prev: str | None = None
        for nid in self.node_order:
            out[nid] = (prev,) if prev else ()
            prev = nid
        return out
