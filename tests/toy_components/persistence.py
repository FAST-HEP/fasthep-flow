"""A toy persistence capability: a JSON dict of counts, one payload file."""

from __future__ import annotations

import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from hepflow.model.persistence import (
    PersistenceCapability,
    PersistenceSettings,
    SavedPayload,
    SaveOutcome,
)
from hepflow.model.plan import ExecutionPlan
from hepflow.model.products import BoundaryProduct, ProductHandlerEntry
from hepflow.registry.runtime import RuntimeRegistry
from hepflow.runtime.persistence.save import persist_boundary_products
from hepflow.runtime.persistence.select import PersistenceSelection


def save_toy_counts(
    value: Any, *, root: Path, params: dict[str, Any], identity: dict[str, Any]
) -> SaveOutcome:
    root.mkdir(parents=True, exist_ok=True)
    (root / "counts.json").write_text(
        json.dumps(value, sort_keys=True, indent=params.get("indent")),
        encoding="utf-8",
    )
    return SaveOutcome(payloads=(SavedPayload("counts.json", role="data"),))


def load_toy_counts(
    root: Path,
    payloads: Sequence[Path],
    *,
    params: dict[str, Any],
    variant: str | None,
    locator: str | None,
) -> Any:
    return json.loads(Path(payloads[0]).read_text(encoding="utf-8"))


def load_echo_params(
    root: Path,
    payloads: Sequence[Path],
    *,
    params: dict[str, Any],
    variant: str | None,
    locator: str | None,
) -> Any:
    """Return the value together with the load params it was called with."""
    return {
        "value": json.loads(Path(payloads[0]).read_text(encoding="utf-8")),
        "params": dict(params),
        "variant": variant,
    }


def toy_capability(**overrides: Any) -> PersistenceCapability:
    fields: dict[str, Any] = {
        "name": "toy.counts",
        "version": "1",
        "kind": "histogram",
        "container": "json",
        "payload_roles": ("data",),
        "fidelity": "exact",
        "save": save_toy_counts,
        "load": load_echo_params,
        "variants": (),
        "params": {"save": {"indent": 2}, "load": {"scale": 1}},
    }
    fields.update(overrides)
    return PersistenceCapability(**fields)


def registry_with(*capabilities: PersistenceCapability) -> RuntimeRegistry:
    handlers: dict[str, ProductHandlerEntry] = {}
    for capability in capabilities:
        entry = handlers.get(capability.kind, ProductHandlerEntry())
        handlers[capability.kind] = ProductHandlerEntry(
            persistence={**entry.persistence, capability.name: capability}
        )
    return RuntimeRegistry(product_handlers=handlers)


def publish_toy_set(
    tmp_path: Path, capability: PersistenceCapability | None = None
) -> Path:
    capability = capability or toy_capability()
    product = BoundaryProduct(
        node_id="stage.Fill",
        output_name="hist",
        kind="histogram",
        dataset="Zmumu",
        partition_id="p0",
        representation="persist",
        value={"entries": 3},
    )
    selection = PersistenceSelection(persisted={product.key(): capability})
    [saved] = persist_boundary_products(
        [product],
        selection,
        plan=ExecutionPlan(),
        settings=PersistenceSettings(mode="all"),
        outdir=tmp_path,
    )
    return Path(saved.metadata["persisted"])


TOY = "tests.toy_components.persistence"
KIND_CFG = {
    "persistence": {
        "formats": {
            "toy.counts": {
                "version": "1",
                "container": "json",
                "payload_roles": ["data"],
                "fidelity": "exact",
                "save": f"{TOY}:save_toy_counts",
                "load": f"{TOY}:load_toy_counts",
            }
        }
    }
}


def persistence_workflow(
    toy_workflow: dict[str, Any], persistence: dict[str, Any] | None
) -> Any:
    execution: dict[str, Any] = {"persistence": persistence} if persistence else {}
    return {
        **toy_workflow,
        "data": {
            "datasets": [{"name": "toydata", "files": ["toy://events"], "nevents": 4}]
        },
        "execution": execution,
        "registry": {
            **toy_workflow["registry"],
            "product_handlers": {
                "histogram": {
                    "boundary": {"retain": True, "representation": "value"},
                    "combine": "tests.toy_components.transforms:combine_toy_histograms",
                    "merge": "tests.toy_components.transforms:merge_toy_histograms",
                    **KIND_CFG,
                }
            },
        },
        "analysis": {
            "stages": [
                {"id": "RecoilHist", "op": "hep.hist", "params": {"name": "recoil"}}
            ]
        },
    }
