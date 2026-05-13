from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class HookSpec:
    name: str
    kind: str = "hook"
    version: str | None = None
    events: list[str] = field(default_factory=list)
    context_outputs: list[str] = field(default_factory=list)

    @classmethod
    def from_obj(cls, obj: Any) -> HookSpec:
        if isinstance(obj, cls):
            return obj
        if not isinstance(obj, dict):
            raise TypeError("Hook spec must be a mapping or HookSpec")
        name = obj.get("name")
        if not isinstance(name, str) or not name:
            raise ValueError("Hook spec requires non-empty string 'name'")
        kind = str(obj.get("kind") or "hook")
        version = obj.get("version")
        if version is not None:
            version = str(version)
        events = obj.get("events") or []
        if not isinstance(events, list) or not all(
            isinstance(event, str) and event for event in events
        ):
            raise ValueError("Hook spec 'events' must be a list of strings")
        context_outputs = obj.get("context_outputs") or []
        if not isinstance(context_outputs, list) or not all(
            isinstance(item, str) and item for item in context_outputs
        ):
            raise ValueError(
                "Hook spec 'context_outputs' must be a list of strings"
            )
        return cls(
            name=name,
            kind=kind,
            version=version,
            events=list(events),
            context_outputs=list(context_outputs),
        )

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> HookSpec:
        return cls.from_obj(d)

    def to_dict(self) -> dict[str, Any]:
        out = {
            "name": self.name,
            "kind": self.kind,
            "events": list(self.events),
            "context_outputs": list(self.context_outputs),
        }
        if self.version is not None:
            out["version"] = self.version
        return out


class ExecutionHook:
    name: str | None = None

    def __init__(self, **params: Any) -> None:
        self.params = dict(params)

    def partition_start(self, *, partition, ctx: dict[str, Any]) -> None:
        pass

    def before_node(self, *, node, inputs: dict[str, Any], ctx: dict[str, Any]) -> None:
        pass

    def after_node(
        self,
        *,
        node,
        inputs: dict[str, Any],
        outputs: Any,
        ctx: dict[str, Any],
    ) -> None:
        pass

    @contextmanager
    def around_node(self, *, node, inputs: dict[str, Any], ctx: dict[str, Any]):
        pass

    def on_node_error(
        self,
        *,
        node,
        inputs: dict[str, Any],
        ctx: dict[str, Any],
        exc: BaseException,
    ) -> None:
        pass

    def partition_end(self, *, partition, ctx: dict[str, Any], value_store) -> None:
        pass

    def dataset_end(
        self,
        *,
        dataset_name: str,
        ctx: dict[str, Any],
        value_store,
    ) -> None:
        pass

    def run_end(self, *, plan, ctx: dict[str, Any], summary: dict[str, Any]) -> None:
        pass
