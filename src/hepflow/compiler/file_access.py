"""Map logical input file names to the URLs a site reads them from.

Dataset files name inputs as the experiment does (for CMS the logical file name
``/store/...``); how a site reaches them (an XRootD redirector, a local mount) is an
execution setting::

    execution:
      file_access:
      - match: /store/
        prefix: root://cms-xrd-global.cern.ch/

The plan keeps the logical names in its datasets and gives each partition the URL:
the first rule whose ``match`` starts the file name puts ``prefix`` in front of it.
File names that no rule matches, including URIs and relative paths, are kept.
"""

from __future__ import annotations

from typing import Any

_RULE_KEYS = {"match", "prefix"}


def normalize_file_access(raw: Any) -> list[dict[str, str]]:
    """Validate ``execution.file_access``: a list of ``{match, prefix}`` rules."""
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise ValueError("execution.file_access must be a list of {match, prefix} rules")
    rules: list[dict[str, str]] = []
    for index, rule in enumerate(raw):
        where = f"execution.file_access[{index}]"
        if not isinstance(rule, dict) or set(rule) != _RULE_KEYS:
            raise ValueError(f"{where} must have exactly the keys 'match' and 'prefix'")
        for key in ("match", "prefix"):
            value = rule[key]
            if not isinstance(value, str) or not value:
                raise ValueError(f"{where}.{key} must be a non-empty string")
        rules.append({"match": rule["match"], "prefix": rule["prefix"]})
    return rules


def resolve_file_access(path: str, rules: list[dict[str, str]]) -> str:
    """Return the URL of ``path`` under the first matching rule, else ``path``."""
    for rule in rules:
        if path.startswith(rule["match"]):
            return rule["prefix"] + path
    return path
