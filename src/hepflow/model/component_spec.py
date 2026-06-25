from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class RuntimeComponentSpec:
    """
    Declarative description of a Flow component.

    A component spec describes two independent things:

    - **Execution contract** — what flows into and out of the component during
      execution.
    - **Dependency contract** — what information the component needs and what
      information it makes available.

    Execution contract

    ``input``
        What the component consumes (for example, an ``event_stream``).

    ``result``
        What the component produces (for example, an event stream or an
        artifact).

    Dependency contract

    ``requires``
        The information needed before the component can run.

    ``provides``
        The information available after the component has run.

    These contracts are intentionally independent. For example, a component may
    consume an entire event stream while requiring only the fields
    ``Muon_Px`` and ``Muon_Py``. Likewise, it may produce an event stream while
    adding a single derived field such as ``Muon_Pt``.
    """

    name: str
    kind: str
    version: str | None = None
    input: dict[str, Any] = field(default_factory=dict)
    params: dict[str, Any] = field(default_factory=dict)
    result: dict[str, Any] = field(default_factory=dict)
    requires: dict[str, Any] = field(default_factory=dict)
    provides: dict[str, Any] = field(default_factory=dict)
    lifecycle: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_obj(cls, obj: Any) -> RuntimeComponentSpec:
        if isinstance(obj, cls):
            return obj
        if not isinstance(obj, dict):
            raise TypeError(
                "Runtime component spec must be a mapping or RuntimeComponentSpec, "
                f"got {type(obj).__name__}"
            )

        name = obj.get("name")
        if not isinstance(name, str) or not name:
            raise ValueError("Runtime component spec requires non-empty string 'name'")
        kind = obj.get("kind")
        if not isinstance(kind, str) or not kind:
            raise ValueError("Runtime component spec requires non-empty string 'kind'")

        version = obj.get("version")
        if version is not None:
            version = str(version)

        return cls(
            name=name,
            kind=kind,
            version=version,
            input=dict(obj.get("input") or {}),
            params=dict(obj.get("params") or {}),
            result=dict(obj.get("result") or {}),
            requires=dict(obj.get("requires") or {}),
            provides=dict(obj.get("provides") or {}),
            lifecycle=dict(obj.get("lifecycle") or {}),
        )
