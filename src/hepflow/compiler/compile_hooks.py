from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from hepflow.build_layout import BuildPaths
from hepflow.model.component_spec import RuntimeComponentSpec
from hepflow.model.plan import ExecutionPlan
from hepflow.registry.loaders import load_object


@dataclass(slots=True)
class CompileHookContext:
    normalized: dict[str, Any]
    plan_context: dict[str, Any]
    build_paths: BuildPaths
    artifacts: dict[str, Any] = field(default_factory=dict)


def run_compile_hooks(
    *,
    plan: ExecutionPlan,
    normalized: dict[str, Any] | None,
    build_paths: BuildPaths,
    when: str,
    artifacts: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Run registry-provided compile hooks for one compile phase."""
    artifact_map = dict(artifacts or {})
    ctx = CompileHookContext(
        normalized=dict(normalized or {}),
        plan_context=dict(plan.context or {}),
        build_paths=build_paths,
        artifacts=artifact_map,
    )

    out: dict[str, Any] = {}
    for name, entry, _spec in _compile_hook_entries(plan.registry, when=when):
        impl_ref = str(entry["impl"])
        try:
            impl = load_object(impl_ref)
            params = dict(entry.get("params") or {})
            result = impl(ctx, **params)
        except Exception as exc:
            raise RuntimeError(
                f"Compile hook {name!r} failed during {when!r} "
                f"using {impl_ref!r}: {exc}"
            ) from exc

        if result is None:
            continue
        if not isinstance(result, dict):
            raise TypeError(
                f"Compile hook {name!r} during {when!r} returned "
                f"{type(result).__name__}; expected a mapping of artifact names to data"
            )
        for artifact_name, artifact_data in result.items():
            if not isinstance(artifact_name, str) or not artifact_name.strip():
                raise ValueError(
                    f"Compile hook {name!r} returned invalid artifact name: "
                    f"{artifact_name!r}"
                )
            out[artifact_name.strip()] = artifact_data
            artifact_map[artifact_name.strip()] = artifact_data

    return out


def _compile_hook_entries(
    registry: dict[str, Any] | None,
    *,
    when: str,
) -> list[tuple[str, dict[str, Any], RuntimeComponentSpec]]:
    hooks = dict((registry or {}).get("compile_hooks") or {})
    selected: list[tuple[str, dict[str, Any], RuntimeComponentSpec]] = []
    for name, entry in hooks.items():
        if not isinstance(entry, dict):
            raise TypeError(f"Compile hook registry entry {name!r} must be a mapping")
        if not isinstance(entry.get("impl"), str) or not str(entry.get("impl")).strip():
            raise ValueError(f"Compile hook registry entry {name!r} requires 'impl'")
        spec_ref = entry.get("spec")
        if not isinstance(spec_ref, str) or ":" not in spec_ref:
            raise TypeError(
                f"Compile hook registry entry {name!r} must define string 'spec' "
                "as 'module:object'"
            )
        spec = RuntimeComponentSpec.from_obj(load_object(spec_ref))
        if when not in _compile_hook_phases(spec):
            continue
        selected.append((str(name), dict(entry), spec))
    return selected


def _compile_hook_phases(spec: RuntimeComponentSpec) -> set[str]:
    lifecycle = dict(spec.lifecycle or {})
    raw = lifecycle.get("when")
    if raw is None:
        return set()
    if isinstance(raw, str):
        return {raw}
    if isinstance(raw, list) and all(isinstance(item, str) and item for item in raw):
        return set(raw)
    raise ValueError(
        f"Compile hook spec {spec.name!r} lifecycle.when must be a string "
        "or list of strings"
    )
