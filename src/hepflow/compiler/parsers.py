from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from .symbols import symbols_from_expression, symbols_from_expression_or_name


class ParserError(ValueError):
    pass


ParserFn = Callable[[Any], set[str]]


def _parse_expr(value: Any) -> set[str]:
    if not isinstance(value, str):
        value = str(value)
    syms, _ = symbols_from_expression(value)
    return syms


def _parse_expr_or_name(value: Any) -> set[str]:
    if not isinstance(value, str):
        value = str(value)
    return symbols_from_expression_or_name(value)


def _walk_and_parse_strings(value: Any, *, leaf_parser: ParserFn) -> set[str]:
    out: set[str] = set()

    if value is None:
        return out

    if isinstance(value, str):
        return set(leaf_parser(value))

    if isinstance(value, dict):
        # Special-case: reduce blocks are not expressions themselves.
        # Only "over" is an expression; "op" is metadata (e.g. any/all/count_nonzero).
        if "op" in value and "over" in value and len(value.keys()) <= 3:
            return _walk_and_parse_strings(value.get("over"), leaf_parser=leaf_parser)

        # Otherwise recurse into values
        for v in value.values():
            out |= _walk_and_parse_strings(v, leaf_parser=leaf_parser)
        return out

    if isinstance(value, (list, tuple)):
        for v in value:
            out |= _walk_and_parse_strings(v, leaf_parser=leaf_parser)
        return out

    return out


def _parse_walk_expr(value: Any) -> set[str]:
    return _walk_and_parse_strings(value, leaf_parser=_parse_expr)


@dataclass(frozen=True)
class ParserBundle:
    """
    Registry of parsers used by OpSpec RequireParse/ProvideParse rules.
    """
    parsers: dict[str, ParserFn]

    def extract_symbols(self, parser: str, value: Any) -> set[str]:
        fn = self.parsers.get(parser)
        if fn is None:
            raise ParserError(f"No parser registered: {parser!r}")
        return set(fn(value))


def default_parsers() -> ParserBundle:
    return ParserBundle(
        parsers={
            "expr": _parse_expr,
            "expr_or_name": _parse_expr_or_name,
            "walk_expr": _parse_walk_expr,   # for skip_pre_parsing=True on nested structures
        }
    )
