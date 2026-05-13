from __future__ import annotations

import ast
import re
from dataclasses import dataclass

# Match "branch-like" tokens that contain '.' or '/' (or both) and are not quoted.
# Examples:
#   ss./ss.nSingleScatters
#   ss./ss.xyCorrectedS2Area_phd
#   scatters.ss.x_cm            (if you ever allow it directly)
#
# This tries to match a leading identifier-ish chunk, then one or more segments
# separated by '.' or '/', allowing underscores and digits in segments.
# _BRANCHLIKE_TOKEN = re.compile(r"[A-Za-z_]\w*(?:[./][A-Za-z0-9_]+)+")
_BRANCHLIKE_TOKEN = re.compile(
    r"""
    (?P<tok>
        [A-Za-z_]\w*                 # leading identifier
        (?:                          # then one or more "path segments"
            (?:\.\./|\.\/|\.|\/)     # separators: ../ ./ . /
            [A-Za-z0-9_]+            # segment
        )+
    )
    """,
    re.VERBOSE,
)


_IDENT = re.compile(r"^[A-Za-z_]\w*$")
# IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class _SymbolCollector(ast.NodeVisitor):
    """
    Collect Name nodes and function calls in an AST expression.
    - names: identifiers referenced (e.g. Muon_Pt, __b0)
    - called_funcs: identifiers used as function calls (e.g. sqrt, log10)
      (so we can avoid treating them as required symbols).
    """

    def __init__(self) -> None:
        self.names: set[str] = set()
        self.called_funcs: set[str] = set()

    def visit_Name(self, node: ast.Name) -> None:
        self.names.add(node.id)
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        # Record the function name if it's a simple identifier
        if isinstance(node.func, ast.Name):
            self.called_funcs.add(node.func.id)
        self.generic_visit(node)


@dataclass(frozen=True)
class SanitizedExpr:
    """
    Result of sanitizing an expression for Python AST parsing.

    internal_to_original maps synthetic identifiers like "__b0" back to the original
    branch-like token like "ss./ss.nSingleScatters".
    """
    original: str
    sanitized: str
    internal_to_original: dict[str, str]


def _to_safe_ident(token: str) -> str:
    """
    Turn an arbitrary branch-like token into a readable python identifier by:
      - replacing all non [A-Za-z0-9_] chars with '_'
      - ensuring it doesn't start with a digit
    """
    s = re.sub(r"[^A-Za-z0-9_]", "_", token)
    if not s or s[0].isdigit():
        s = "_" + s
    return s


def sanitize_branchlike_tokens(expr: str) -> SanitizedExpr:
    """
    Replace branch-like tokens with readable safe identifiers (underscored),
    while keeping a reversible mapping.
    """
    if not isinstance(expr, str):
        expr = str(expr)

    internal_to_original: dict[str, str] = {}
    used: set[str] = set()

    def repl(m: re.Match) -> str:
        tok = m.group("tok")

        # Already safe? leave it.
        if _IDENT.match(tok):
            return tok

        base = _to_safe_ident(tok)
        key = base
        # Ensure uniqueness (collisions can happen after replacing chars)
        i = 0
        while key in used:
            i += 1
            key = f"{base}__{i}"
        used.add(key)
        internal_to_original[key] = tok
        return key

    sanitized = _BRANCHLIKE_TOKEN.sub(repl, expr)
    return SanitizedExpr(expr, sanitized, internal_to_original)


def symbols_from_expression(
    expr: str,
    *,
    on_syntax_error: str = "raise",
) -> tuple[set[str], SanitizedExpr]:
    """
    Parse an expression, returning a set of referenced symbols (variables),
    and the SanitizedExpr used for parsing.

    - Automatically sanitizes "branch-like" tokens containing '.' or '/'.
    - Restores sanitized identifiers back to their original token text.

    on_syntax_error:
      - "raise": raise ValueError with helpful context
      - "empty": return (set(), sanitized_expr)
    """
    sx = sanitize_branchlike_tokens(expr)

    try:
        tree = ast.parse(sx.sanitized, mode="eval")
    except SyntaxError as e:
        if on_syntax_error == "empty":
            return set(), sx
        # Provide both original and sanitized forms + mapping for debugging
        msg = (
            "Invalid expression syntax.\n"
            f"  original : {sx.original}\n"
            f"  sanitized: {sx.sanitized}\n"
            f"  mapping  : {sx.internal_to_original}\n"
            f"  error    : {e.msg} (line {e.lineno}, col {e.offset})"
        )
        raise ValueError(msg) from e

    v = _SymbolCollector()
    v.visit(tree)

    raw_names = v.names - v.called_funcs

    # Restore sanitized ids back to original branch-like tokens
    restored: set[str] = set()
    for name in raw_names:
        restored.add(sx.internal_to_original.get(name, name))

    return restored, sx


def symbols_from_expression_or_name(s: str) -> set[str]:
    """
    If `s` is a simple identifier -> {s}
    otherwise parse it as an expression and return its referenced symbols.
    (Sanitization applies automatically for branch-like tokens.)
    """
    if not isinstance(s, str):
        s = str(s)
    s2 = s.strip()
    if _IDENT.match(s2):
        return {s2}
    syms, _ = symbols_from_expression(s2)
    return syms
