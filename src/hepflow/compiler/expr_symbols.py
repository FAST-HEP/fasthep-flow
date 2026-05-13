from __future__ import annotations

import ast

PYTHON_CONSTANT_SYMBOLS: set[str] = {
    "False",
    "None",
    "True",
}


def symbols_in_expr(expr: str) -> set[str]:
    normalised = str(expr).replace("&&", " and ").replace("||", " or ")
    tree = ast.parse(normalised, mode="eval")
    return {node.id for node in ast.walk(tree) if isinstance(node, ast.Name)}


def column_symbols_in_expr(
    expr: str,
    *,
    known_functions: set[str],
    known_constants: set[str],
    context_symbols: set[str],
    produced: set[str] | None = None,
) -> set[str]:
    symbols = symbols_in_expr(expr)
    symbols -= set(known_functions)
    symbols -= set(known_constants)
    symbols -= set(context_symbols)
    symbols -= set(produced or set())
    symbols -= PYTHON_CONSTANT_SYMBOLS
    return symbols


def data_symbols_in_expr(
    expr: str,
    *,
    known_functions: set[str],
    known_constants: set[str],
    context_symbols: set[str],
    produced: set[str] | None = None,
) -> set[str]:
    return column_symbols_in_expr(
        expr,
        known_functions=known_functions,
        known_constants=known_constants,
        context_symbols=context_symbols,
        produced=produced,
    )
