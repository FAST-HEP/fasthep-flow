from __future__ import annotations

from typing import Any

from hepflow.backends.model import Backend
from hepflow.model.plan import ExecutionPlan
from hepflow.registry.loaders import load_object


def backend_key(execution: dict[str, Any]) -> str:
    backend = execution.get("backend", "local")
    strategy = execution.get("strategy", "default")
    return f"{backend}.{strategy}"


def normalize_backend_override(
    backend: str | None,
    strategy: str | None,
) -> dict[str, str]:
    if backend is None and strategy is None:
        return {}

    if backend is not None:
        backend = str(backend)
    if strategy is not None:
        strategy = str(strategy)

    if backend and "." in backend and strategy in (None, "default"):
        backend, strategy = backend.split(".", 1)

    override: dict[str, str] = {}
    if backend is not None:
        override["backend"] = backend
        override["strategy"] = strategy if strategy is not None else "default"
    elif strategy is not None:
        override["strategy"] = strategy
    return override


def load_backend(
    plan: ExecutionPlan,
    *,
    registry_cfg: dict[str, Any] | None = None,
) -> Backend:
    registry = registry_cfg or plan.registry
    key = backend_key(plan.execution)
    backends = dict((registry or {}).get("backends") or {})
    try:
        entry = backends[key]
    except KeyError as exc:
        raise KeyError(f"Unknown backend strategy '{key}'") from exc

    if not isinstance(entry, dict):
        raise TypeError(f"Backend registry entry '{key}' must be a mapping")

    impl_ref = entry.get("impl")
    if not isinstance(impl_ref, str):
        raise TypeError(f"Backend registry entry '{key}' must define string 'impl'")

    impl = load_object(impl_ref)
    backend = impl() if isinstance(impl, type) else impl
    if not hasattr(backend, "run"):
        raise TypeError(f"Backend implementation '{impl_ref}' does not provide run()")
    return backend
