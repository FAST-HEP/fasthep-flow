"""Resolve the output ports of registered operations once per lowering.

``lower_graph`` needs the ``{port: kind}`` outputs of every stage and sink it lowers. They come
from the ``result`` of the op's registered spec. :class:`SpecResolver` loads each spec at most
once, validates it, and returns one canonical mapping, so lowering reads no registry entry
itself. An op without a registry entry keeps the defaults of its role (a transform produces a
``stream`` of kind ``event_stream``, a sink an ``artifact``); a registered op whose spec is
unusable is an error, never a silent default.

A transform also has an execution scope, ``partition`` or ``global``. A transform that produces
an event stream runs per partition, which a spec need not say. A transform without a stream
output must declare ``execution: {scope: partition | global}`` in its spec; a missing
declaration is an error naming the op, and so is ``global`` on a stream-producing transform.

Specs are resolved on first use, not when the registry is loaded: a registry lists every op of
every package, and importing all of them would load code a workflow never uses.
"""

from __future__ import annotations

from typing import Any, cast

from hepflow.model.graph import ExecutionScope
from hepflow.registry.loaders import load_object

STAGE_DEFAULT = {"stream": "event_stream"}
SINK_DEFAULT = {"artifact": "artifact"}


class InvalidRegisteredSpec(ValueError):
    """A registered op's spec cannot give its output ports."""


class SpecResolver:
    """Output ports of the registered transforms and sinks of one workflow."""

    def __init__(
        self,
        *,
        transforms: dict[str, Any] | None = None,
        sinks: dict[str, Any] | None = None,
    ) -> None:
        self._registries = {
            "transforms": dict(transforms or {}),
            "sinks": dict(sinks or {}),
        }
        self._cache: dict[tuple[str, str], dict[str, str]] = {}
        self._scopes: dict[str, ExecutionScope] = {}

    def stage_outputs(self, op: str) -> dict[str, str]:
        return dict(self._outputs("transforms", op, STAGE_DEFAULT))

    def stage_scope(self, op: str) -> ExecutionScope:
        """Execution scope of a transform; resolves the op's spec on first use."""
        self._outputs("transforms", op, STAGE_DEFAULT)
        return self._scopes.get(op, "partition")

    def sink_outputs(self, op: str) -> dict[str, str]:
        return dict(self._outputs("sinks", op, SINK_DEFAULT))

    def _outputs(self, category: str, op: str, default: dict[str, str]) -> dict[str, str]:
        key = (category, op)
        if key not in self._cache:
            self._cache[key] = self._resolve(category, op, default)
        return self._cache[key]

    def _resolve(self, category: str, op: str, default: dict[str, str]) -> dict[str, str]:
        if op not in self._registries[category]:
            return dict(default)
        where = f"registry.{category}[{op!r}]"
        entry = self._registries[category][op]
        if not isinstance(entry, dict) or not isinstance(entry.get("spec"), str):
            raise InvalidRegisteredSpec(f"{where} must be a mapping with a string 'spec'")
        spec_ref = entry["spec"]
        spec = load_object(spec_ref)
        if not isinstance(spec, dict):
            raise InvalidRegisteredSpec(f"{where}: spec {spec_ref!r} must be a mapping")
        try:
            outputs = (
                _outputs_from_result(spec["result"])
                if "result" in spec
                else dict(default)
            )
            if category == "transforms":
                self._scopes[op] = _scope_of(spec, outputs)
        except InvalidRegisteredSpec as error:
            raise InvalidRegisteredSpec(f"{where}: spec {spec_ref!r}: {error}") from None
        return outputs


def _produces_stream(outputs: dict[str, str]) -> bool:
    return "stream" in outputs or "event_stream" in outputs.values()


def _scope_of(spec: dict[str, Any], outputs: dict[str, str]) -> ExecutionScope:
    declared = spec.get("execution")
    scope: Any = None
    if declared is not None:
        scope = declared.get("scope") if isinstance(declared, dict) else None
        if scope not in ("partition", "global"):
            raise InvalidRegisteredSpec(
                "'execution' must be a mapping with 'scope' set to 'partition' or 'global'"
            )
    if _produces_stream(outputs):
        if scope == "global":
            raise InvalidRegisteredSpec(
                "a transform that produces an event stream runs per partition, "
                "not with scope 'global'"
            )
        return "partition"
    if scope is None:
        raise InvalidRegisteredSpec(
            "a transform without an event stream output must declare "
            "'execution: {scope: partition | global}'"
        )
    return cast(ExecutionScope, scope)


def _outputs_from_result(result: Any) -> dict[str, str]:
    if not isinstance(result, dict) or not result:
        raise InvalidRegisteredSpec("'result' must be a non-empty mapping")
    if isinstance(result.get("kind"), str):
        kind = str(result["kind"])
        port = result.get("port")
        if isinstance(port, str) and port:
            return {port: kind}
        if kind == "event_stream":
            return {"stream": kind}
        if kind == "artifact":
            return {"artifact": kind}
        raise InvalidRegisteredSpec(f"single result kind {kind!r} needs an output 'port'")
    outputs: dict[str, str] = {}
    for name, value in result.items():
        if isinstance(value, str) and value:
            outputs[str(name)] = value
        elif isinstance(value, dict) and isinstance(value.get("kind"), str) and value["kind"]:
            outputs[str(name)] = str(value["kind"])
        else:
            raise InvalidRegisteredSpec(
                f"result output {name!r} must be a kind string or a mapping with a 'kind'"
            )
    return outputs
