from __future__ import annotations
from hepflow.compiler.parsers import ParserBundle
from typing import Any, Callable, Optional, Tuple, Iterable

from dataclasses import dataclass


@dataclass(frozen=True)
class OpDependencies:
    """
    Normalized dependency report in *symbol space*.

    - requires_symbols: symbols read from event stream or context
    - provides_symbols: symbols added to event stream
    - requires_inputs: explicit external inputs the op needs directly
      (used for templated branch sets etc.)

    requires_inputs uses stream ids as keys. Use "__primary__" for "whatever the primary stream is".
    """

    requires_symbols: tuple[str, ...] = ()
    provides_symbols: tuple[str, ...] = ()


@dataclass(frozen=True)
class RequireParse:
    """
    Pluck values from a node using `path`, then interpret them with a named parser.

    Typical uses:
      - expressions: parser="expr"
      - expr-or-name: parser="expr_or_name"
      - custom op structure: parser="cutflow"

    @param skip_pre_parsing: if True, don't pre-parse the value as string (e.g. for complex structures)
    """

    path: Tuple[str, ...]
    parser: str
    optional: bool = True
    skip_pre_parsing: bool = False


@dataclass(frozen=True)
class RequireLiteral:
    """
    Require these symbols literally (rare, but useful for fixed requirements).
    """

    symbols: Tuple[str, ...]


@dataclass(frozen=True)
class TemplateReq:
    param: str
    default: Any
    pattern: str
    vars: Tuple[str, ...]


@dataclass(frozen=True)
class RequireTemplates:
    """
    Require input items by expanding TemplateReq objects.
    (These are typically explicit ROOT branches, not symbols.)
    """

    templates: Tuple[TemplateReq, ...]


RequireRule = RequireParse | RequireLiteral | RequireTemplates


@dataclass(frozen=True)
class ValueProvide:
    """
    Provide these symbols literally
    """

    symbols: Tuple[str, ...]


@dataclass(frozen=True)
class ValueFromParamProvide:
    """
    Extract provided symbol names by walking the node/params structure.

    Example path:
      ("params", "variables", "*", "name")
    """

    path: Tuple[str, ...]  # supports "*"
    coerce_str: bool = True


@dataclass(frozen=True)
class TemplateProvide:
    param: str
    default: Any = None
    if_set: bool = False
    pattern: Optional[str] = None


def expand_template_requires(req: TemplateReq, chosen: Any) -> Tuple[str, ...]:
    """
    Expands TemplateReq into a list of required *symbols* or *branch names*.
    E.g. collection=Muon, pattern="{collection}_{var}", vars=("Px","Py") -> ("Muon_Px","Muon_Py")
    """
    env = {req.param: chosen}
    out = []
    for v in req.vars:
        env2 = dict(env)
        env2["var"] = v
        out.append(req.pattern.format(**env2))
    return tuple(out)


def _expand_template_provide(tp: TemplateProvide, chosen: Any) -> Optional[str]:
    if tp.if_set and chosen is None:
        return None
    if tp.pattern:
        return tp.pattern.format(**{tp.param: chosen})
    if isinstance(chosen, str) and chosen:
        return chosen
    return None


def pluck_values(obj: Any, path: Tuple[str, ...]) -> list[Any]:
    """
    Walk nested dict/list structures and return all values at `path`.
    Supports "*" to iterate over lists AND dict values.
    Preserves encounter order.
    """
    items: list[Any] = [obj]
    for key in path:
        next_items: list[Any] = []
        for it in items:
            if key == "*":
                if isinstance(it, list):
                    next_items.extend(it)
                elif isinstance(it, dict):
                    next_items.extend(it.values())
                continue

            if isinstance(it, dict) and key in it:
                next_items.append(it[key])
                continue

        items = next_items
    return items


def pluck_strings(obj: Any, path: Tuple[str, ...]) -> list[str]:
    """
    Convenience: pluck values then keep only non-empty strings (trimmed).
    Preserves order and duplicates (dedupe happens later if desired).
    """
    out: list[str] = []
    for v in pluck_values(obj, path):
        if isinstance(v, str):
            s = v.strip()
            if s:
                out.append(s)
    return out


def unique_preserve_order(items: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for x in items:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


@dataclass(frozen=True)
class OpSpec:
    requires: tuple[Any, ...] = ()
    provides: tuple[Any, ...] = ()
    consumes_event_stream: bool = True
    produces_event_stream: bool = True

    def dependencies(
        self, *, node: dict[str, Any], parsers: "ParserBundle"
    ) -> "OpDependencies":
        params = node.get("params", {}) or {}

        req_items: list[str] = []
        for rule in self.requires:
            if isinstance(rule, RequireLiteral):
                req_items.extend([str(s) for s in rule.symbols])
                continue

            if isinstance(rule, RequireTemplates):
                for template in rule.templates:
                    chosen = params.get(template.param, template.default)
                    expanded = expand_template_requires(template, chosen)
                    req_items.extend(expanded)
                continue

            if isinstance(rule, RequireParse):
                # 1) collect values from the node at path
                values = pluck_values(node, rule.path)

                if not values and rule.optional:
                    continue
                if not values:
                    raise ValueError(
                        f"Missing required path {rule.path} for op '{node.get('op')}' node '{node.get('id')}'"
                    )

                # 2) parse each value according to skip_pre_parsing
                if rule.skip_pre_parsing:
                    # pass raw objects (dict/list/etc.) to parser
                    for v in values:
                        req_items.extend(parsers.extract_symbols(rule.parser, v))
                else:
                    # only parse strings; ignore non-strings silently (or raise if you prefer strict)
                    for v in values:
                        if isinstance(v, str) and v.strip():
                            req_items.extend(
                                parsers.extract_symbols(rule.parser, v.strip())
                            )
                continue

            raise TypeError(f"Unknown require rule type: {type(rule).__name__}")

        prov_items: list[str] = []
        for p in self.provides:
            if isinstance(p, ValueProvide):
                prov_items.extend([str(s) for s in p.symbols])
                continue

            if isinstance(p, ValueFromParamProvide):
                prov_items.extend(pluck_strings(node, p.path))
                continue

            if isinstance(p, TemplateProvide):
                chosen = params.get(p.param, p.default)
                out = _expand_template_provide(p, chosen)
                if out:
                    prov_items.append(out)
                continue

            raise TypeError(f"Unknown provide spec type: {type(p).__name__}")

        # Deduplicate at the end, preserving order
        req_final = tuple(unique_preserve_order(req_items))
        prov_final = tuple(unique_preserve_order(prov_items))

        return OpDependencies(
            requires_symbols=req_final,
            provides_symbols=prov_final,
        )


OpHandler = Callable[[Any, dict[str, Any], dict[str, Any]], Any]


@dataclass(frozen=True)
class OpEntry:
    spec: OpSpec
    handler: OpHandler
