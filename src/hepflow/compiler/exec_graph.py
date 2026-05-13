from __future__ import annotations

from collections.abc import Iterable
from dataclasses import replace

from hepflow.model.ir import InputRef
from hepflow.model.plan import ExecNode


def fill_input_aliases(
    nodes: Iterable[ExecNode],
    *,
    default_stream_alias: str = "events",
) -> tuple[ExecNode, ...]:
    """
    Ensure every InputRef has a stable `as` value.

    Rules:
      - stream ref:
          as = ref.as_ or ref.stream
          (special case: if stream is None / weird, fallback to default_stream_alias)
      - node ref:
          as = ref.as_ or ref.port

    Notes:
      - We keep aliases minimal and readable.
      - If you ever need collision-proofing later, change node rule to f"{node}.{port}".
    """
    out_nodes: list[ExecNode] = []

    for n in nodes:
        new_in: list[InputRef] = []
        for r in n.in_:
            if r.stream is not None:
                alias = r.as_ or r.stream or default_stream_alias
                new_in.append(replace(r, as_=alias))
            else:
                # node+port case (validated by InputRef.__post_init__)
                alias = r.as_ or (r.port or "data")
                new_in.append(replace(r, as_=alias))

        out_nodes.append(replace(n, in_=tuple(new_in)))

    return tuple(out_nodes)
