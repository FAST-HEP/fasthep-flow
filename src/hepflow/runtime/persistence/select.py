"""Decide which boundary products are persisted, and with which capability."""

from __future__ import annotations

from collections.abc import Collection
from dataclasses import dataclass, field

from hepflow.model.persistence import PersistenceCapability, PersistenceSettings
from hepflow.model.plan import ExecutionPlan
from hepflow.model.products import BoundaryOutputSpec, ProductHandlerEntry
from hepflow.registry.runtime import RuntimeRegistry


@dataclass(frozen=True, slots=True)
class PersistenceSelection:
    """The resolved choice: persisted products and the ones left out, with a reason."""

    persisted: dict[tuple[str, str], PersistenceCapability] = field(
        default_factory=dict
    )
    not_persisted: list[dict[str, str]] = field(default_factory=list)

    def to_dict(self) -> dict[str, list[dict[str, str]]]:
        return {
            "persisted": [
                {
                    "node": node,
                    "output": output,
                    "kind": capability.kind,
                    "capability": capability.name,
                    "capability_version": capability.version,
                }
                for (node, output), capability in sorted(self.persisted.items())
            ],
            "not_persisted": list(self.not_persisted),
        }


def global_side_product_nodes(plan: ExecutionPlan) -> set[str]:
    """Nodes whose products are global side products: run once, seeded into every partition.

    They have no partition identity, so persisting them per partition would file one copy
    per partition under the wrong dataset; they are listed as not persisted instead.
    """
    return {
        node.id
        for node in plan.nodes
        if node.role == "transform"
        and node.input_scope == "global"
        and node.output_scope == "global"
        and any(kind != "event_stream" for kind in node.outputs.values())
    }


def resolve_persistence(
    boundary: list[BoundaryOutputSpec],
    registry: RuntimeRegistry,
    settings: PersistenceSettings,
    *,
    global_nodes: Collection[str] = (),
) -> PersistenceSelection:
    """Apply the explicit product choices, then the mode, then the kind defaults.

    ``off`` persists nothing. An explicit entry overrides the mode; asking for a kind
    that has no capability fails. A kind without a capability is otherwise listed in
    ``not_persisted``, never skipped silently.
    """
    if settings.mode == "off":
        return PersistenceSelection()
    persisted: dict[tuple[str, str], PersistenceCapability] = {}
    left_out: list[dict[str, str]] = []
    for spec in boundary:
        key = _key(spec)
        handler = registry.product_handlers.get(spec.kind)
        capabilities = dict(handler.persistence) if handler is not None else {}
        choice = settings.products.get(key)
        if choice is False:
            continue
        wanted = choice is not None or _wanted_by_mode(handler, settings)
        if not wanted:
            continue
        if spec.node_id in global_nodes:
            left_out.append(
                {
                    "node": spec.node_id,
                    "output": spec.output_name,
                    "kind": spec.kind,
                    "reason": "global side product",
                }
            )
            continue
        if not capabilities:
            if choice is not None:
                raise ValueError(
                    f"execution.persistence.products[{key!r}] asks to persist kind "
                    f"{spec.kind!r}, which has no persistence capability"
                )
            left_out.append(
                {
                    "node": spec.node_id,
                    "output": spec.output_name,
                    "kind": spec.kind,
                    "reason": "no capability",
                }
            )
            continue
        capability = _pick(key, spec.kind, capabilities, choice)
        if capability.save is None:
            if choice is not None:
                raise ValueError(
                    f"execution.persistence.products[{key!r}]: capability "
                    f"{capability.name!r} has no save implementation"
                )
            left_out.append(
                {
                    "node": spec.node_id,
                    "output": spec.output_name,
                    "kind": spec.kind,
                    "reason": "no save implementation",
                }
            )
            continue
        persisted[spec.key()] = capability
    unmatched = sorted(set(settings.products) - {_key(spec) for spec in boundary})
    if unmatched:
        raise ValueError(
            f"execution.persistence.products entries {unmatched} match no boundary product"
        )
    return PersistenceSelection(persisted=persisted, not_persisted=left_out)


def _key(spec: BoundaryOutputSpec) -> str:
    return f"{spec.node_id}:{spec.output_name}"


def _wanted_by_mode(
    handler: ProductHandlerEntry | None, settings: PersistenceSettings
) -> bool:
    if settings.mode == "all":
        return True
    return handler is not None and handler.boundary.representation == "persist"


def _pick(
    key: str,
    kind: str,
    capabilities: dict[str, PersistenceCapability],
    choice: bool | str | None,
) -> PersistenceCapability:
    if isinstance(choice, str):
        if choice not in capabilities:
            raise ValueError(
                f"execution.persistence.products[{key!r}]: kind {kind!r} has no "
                f"capability {choice!r}; registered: {sorted(capabilities)}"
            )
        return capabilities[choice]
    if len(capabilities) > 1:
        raise ValueError(
            f"{key!r}: kind {kind!r} has several capabilities {sorted(capabilities)}; "
            "name one in execution.persistence.products"
        )
    return next(iter(capabilities.values()))
