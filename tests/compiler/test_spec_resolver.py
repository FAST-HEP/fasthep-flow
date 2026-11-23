"""Output ports of registered ops are resolved once and an unusable spec is an error."""

from __future__ import annotations

from typing import Any

import pytest

from hepflow.compiler import spec_resolver as module
from hepflow.compiler.lower_graph import lower_workflow_to_graph
from hepflow.compiler.normalize import normalize_workflow
from hepflow.compiler.spec_resolver import InvalidRegisteredSpec, SpecResolver

HERE = "tests.compiler.test_spec_resolver"

GOOD = {"name": "good", "result": {"stream": "event_stream", "hist": {"kind": "histogram"}}}
SINGLE = {"name": "single", "result": {"kind": "histogram", "port": "hist"}}
NO_RESULT = {"name": "no_result"}
RESULT_LIST = {"name": "bad", "result": ["stream"]}
RESULT_EMPTY = {"name": "empty", "result": {}}
MEMBER_BAD = {"name": "member", "result": {"stream": "event_stream", "n": 3}}
KIND_WITHOUT_PORT = {"name": "kind", "result": {"kind": "histogram"}}
NOT_A_MAPPING = ["not", "a", "mapping"]


def _resolver(**specs: str) -> SpecResolver:
    return SpecResolver(transforms={op: {"spec": f"{HERE}:{name}"} for op, name in specs.items()})


def test_registered_specs_give_their_ports() -> None:
    resolver = _resolver(a="GOOD", b="SINGLE")

    assert resolver.stage_outputs("a") == {"stream": "event_stream", "hist": "histogram"}
    assert resolver.stage_outputs("b") == {"hist": "histogram"}


def test_defaults_apply_to_an_unregistered_op_and_a_spec_without_result() -> None:
    resolver = _resolver(plain="NO_RESULT")

    assert resolver.stage_outputs("plain") == {"stream": "event_stream"}
    assert resolver.stage_outputs("unknown") == {"stream": "event_stream"}
    assert resolver.sink_outputs("unknown") == {"artifact": "artifact"}


@pytest.mark.parametrize(
    ("name", "reason"),
    [
        ("RESULT_LIST", "'result' must be a non-empty mapping"),
        ("RESULT_EMPTY", "'result' must be a non-empty mapping"),
        ("MEMBER_BAD", "result output 'n'"),
        ("KIND_WITHOUT_PORT", "needs an output 'port'"),
        ("NOT_A_MAPPING", "must be a mapping"),
    ],
)
def test_an_unusable_spec_names_the_op_the_spec_and_the_reason(name: str, reason: str) -> None:
    with pytest.raises(InvalidRegisteredSpec) as raised:
        _resolver(op=name).stage_outputs("op")

    message = str(raised.value)
    assert "registry.transforms['op']" in message
    assert f"{HERE}:{name}" in message
    assert reason in message


@pytest.mark.parametrize("entry", ["toy.spec", {"impl": "x:y"}, {"spec": 3}, None])
def test_a_registered_entry_needs_a_string_spec(entry: Any) -> None:
    with pytest.raises(InvalidRegisteredSpec, match="mapping with a string 'spec'"):
        SpecResolver(transforms={"op": entry}).stage_outputs("op")


def test_each_spec_is_loaded_once(monkeypatch: pytest.MonkeyPatch) -> None:
    loaded: list[str] = []
    real = module.load_object

    def counting(ref: str) -> Any:
        loaded.append(ref)
        return real(ref)

    monkeypatch.setattr(module, "load_object", counting)
    resolver = _resolver(a="GOOD")

    for _ in range(3):
        resolver.stage_outputs("a")

    assert loaded == [f"{HERE}:GOOD"]


def test_lowering_fails_on_an_invalid_registered_spec(toy_workflow: dict[str, Any]) -> None:
    toy_workflow["registry"]["transforms"]["toy.scale"]["spec"] = f"{HERE}:RESULT_LIST"

    with pytest.raises(InvalidRegisteredSpec, match=r"registry.transforms\['toy.scale'\]"):
        lower_workflow_to_graph(normalize_workflow(toy_workflow))


def test_the_graph_carries_no_datasets(toy_workflow: dict[str, Any]) -> None:
    graph = lower_workflow_to_graph(normalize_workflow(toy_workflow))

    assert dict(graph.graph) == {}
