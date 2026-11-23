"""Resolve the output ports of registered operations once per lowering.

``lower_graph`` needs the ``{port: kind}`` outputs of every stage and sink it lowers. They come
from the ``result`` of the op's registered spec. :class:`SpecResolver` loads each spec at most
once, validates it, and returns one canonical mapping, so lowering reads no registry entry
itself. An op without a registry entry keeps the defaults of its role (a transform produces a
``stream`` of kind ``event_stream``, a sink an ``artifact``); a registered op whose spec is
unusable is an error, never a silent default.

Specs are resolved on first use, not when the registry is loaded: a registry lists every op of
every package, and importing all of them would load code a workflow never uses.
"""

from __future__ import annotations

from typing import Any

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

    def stage_outputs(self, op: str) -> dict[str, str]:
        return dict(self._outputs("transforms", op, STAGE_DEFAULT))

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
        if "result" not in spec:
            return dict(default)
        try:
            return _outputs_from_result(spec["result"])
        except InvalidRegisteredSpec as error:
            raise InvalidRegisteredSpec(f"{where}: spec {spec_ref!r}: {error}") from None


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
