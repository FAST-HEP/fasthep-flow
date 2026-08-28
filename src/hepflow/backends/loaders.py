from __future__ import annotations

from typing import Any

from hepflow.backends.model import Backend, BackendSpec
from hepflow.model.plan import ExecutionPlan
from hepflow.registry.loaders import load_object


def backend_key(execution: dict[str, Any]) -> str:
    backend = execution.get("backend", "local")
    strategy = execution.get("strategy", "default")
    return f"{backend}.{strategy}"


def backend_registry_keys(execution: dict[str, Any]) -> list[str]:
    backend = str(execution.get("backend", "local"))
    keys = [backend_key(execution)]
    keys.append(backend)
    return list(dict.fromkeys(keys))


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
    key, entry = resolve_backend_entry(
        registry,
        plan.execution,
        provenance=plan.provenance,
    )
    impl_ref = _backend_impl_ref(key, entry)
    impl = load_object(impl_ref)
    backend = impl() if isinstance(impl, type) else impl
    if not hasattr(backend, "run"):
        raise TypeError(f"Backend implementation '{impl_ref}' does not provide run()")
    return backend


def validate_backend_execution(
    registry_cfg: dict[str, Any] | None,
    execution: dict[str, Any],
    *,
    provenance: dict[str, Any] | None = None,
) -> None:
    key, entry = resolve_backend_entry(registry_cfg, execution, provenance=provenance)
    spec = load_backend_spec(key, entry)
    if spec.validate_execution is not None:
        spec.validate_execution(execution)


def backend_build_directories(
    registry_cfg: dict[str, Any] | None,
    execution: dict[str, Any],
    *,
    provenance: dict[str, Any] | None = None,
) -> tuple[str, ...]:
    key, entry = resolve_backend_entry(registry_cfg, execution, provenance=provenance)
    return load_backend_spec(key, entry).build_directories


def resolve_backend_entry(
    registry_cfg: dict[str, Any] | None,
    execution: dict[str, Any],
    *,
    provenance: dict[str, Any] | None = None,
) -> tuple[str, dict[str, Any]]:
    registry = registry_cfg or {}
    backends = dict((registry or {}).get("backends") or {})
    for key in backend_registry_keys(execution):
        if key not in backends:
            continue
        entry = backends[key]
        if not isinstance(entry, dict):
            raise TypeError(f"Backend registry entry '{key}' must be a mapping")
        return key, entry

    raise _unknown_backend_error(execution, provenance)


def load_backend_spec(key: str, entry: dict[str, Any]) -> BackendSpec:
    spec_ref = entry.get("spec")
    if spec_ref is None:
        return BackendSpec()
    if not isinstance(spec_ref, str):
        raise TypeError(f"Backend registry entry '{key}' spec must be a string")
    spec = load_object(spec_ref)
    if isinstance(spec, BackendSpec):
        return spec
    if not hasattr(spec, "validate_execution") and not hasattr(
        spec,
        "build_directories",
    ):
        raise TypeError(
            f"Backend specification '{spec_ref}' must provide BackendSpec fields"
        )
    return BackendSpec(
        validate_execution=getattr(spec, "validate_execution", None),
        build_directories=tuple(getattr(spec, "build_directories", ())),
    )


def _backend_impl_ref(key: str, entry: dict[str, Any]) -> str:
    impl_ref = entry.get("impl")
    if not isinstance(impl_ref, str):
        raise TypeError(f"Backend registry entry '{key}' must define string 'impl'")
    return impl_ref


def _unknown_backend_error(
    execution: dict[str, Any],
    provenance: dict[str, Any] | None,
) -> KeyError:
    keys = backend_registry_keys(execution)
    provider = _missing_provider_from_provenance(
        provenance,
        backend=str(execution.get("backend", "local")),
    )
    if provider is not None:
        return KeyError(
            f"Unknown backend strategy '{keys[0]}'. Install {provider['distribution']} "
            f"to provide optional profile {provider['profile']!r}."
        )
    return KeyError(f"Unknown backend strategy '{keys[0]}'")


def _missing_provider_from_provenance(
    provenance: dict[str, Any] | None,
    *,
    backend: str,
) -> dict[str, str] | None:
    for item in list((provenance or {}).get("optional_profiles_skipped") or []):
        if not isinstance(item, dict):
            continue
        provided_backends = {
            value.strip()
            for value in str(item.get("backends") or "").split(",")
            if value.strip()
        }
        if backend not in provided_backends:
            continue
        profile = item.get("profile")
        if not isinstance(profile, str) or ":" not in profile:
            continue
        package, _name = profile.split(":", 1)
        return {"profile": profile, "distribution": package.replace("_", "-")}
    return None
