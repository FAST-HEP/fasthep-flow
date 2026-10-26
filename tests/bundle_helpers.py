"""A small project with an include and resource files, for snapshot and recompile tests."""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from pathlib import Path
from typing import Any

import yaml

SOFTWARE_HOOK = {
    "impl": "hepflow.compiler.hooks.software:software",
    "spec": "hepflow.compiler.hooks.software:SOFTWARE_HOOK_SPEC",
    "params": {"git": False},
}
TOY_HOOKS = "tests.toy_components.compile_hooks"
OUTDIR_ARTIFACT_HOOK = {
    "spec": f"{TOY_HOOKS}:OUTDIR_ARTIFACT_HOOK_SPEC",
    "impl": f"{TOY_HOOKS}:outdir_artifact_hook",
}
SYSTEMATICS = {
    "include_nominal": True,
    "variations": [
        {
            "name": "trigger_eff_up",
            "group": "trigger_eff",
            "direction": "up",
            "applies_to": "mc",
            "requires": ["stage.TriggerEfficiencyWeights"],
            "weight": {"multiply": "TriggerEffWeight_up"},
        }
    ],
}


def make_project(
    tmp_path: Path,
    toy_workflow: Mapping[str, Any],
    *,
    files: Mapping[str, bytes] | None = None,
    resources: Mapping[str, str] | None = None,
    extra_hooks: Mapping[str, Any] | None = None,
    systematics: bool = False,
) -> Path:
    """Write ``proj/workflow.yaml`` with an include and one ``file`` resource per entry.

    ``files`` are written below the project (relative keys) or as given (absolute keys);
    ``resources`` maps a resource id to its declared path (default: ``data/x.json``).
    Returns the entry workflow path.
    """
    workflow = deepcopy(dict(toy_workflow))
    project = tmp_path / "proj"
    (project / "parts").mkdir(parents=True, exist_ok=True)
    for name, content in (files or {"data/x.json": b'{"x": 1}'}).items():
        path = Path(name) if Path(name).is_absolute() else project / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
    (project / "parts" / "sources.yaml").write_text(
        yaml.safe_dump({"sources": workflow.pop("sources")}), encoding="utf-8"
    )
    registry = dict(workflow["registry"])
    registry["compile_hooks"] = {
        **dict(registry.get("compile_hooks") or {}),
        "flow.software": SOFTWARE_HOOK,
        **dict(extra_hooks or {}),
    }
    declared = resources if resources is not None else {"kfactor": "data/x.json"}
    entry_doc: dict[str, Any] = {
        "include": "parts/sources.yaml",
        **workflow,
        "registry": registry,
        "execution": {
            "resources": {
                name: {"kind": "file", "path": path} for name, path in declared.items()
            }
        },
    }
    if systematics:
        entry_doc["systematics"] = deepcopy(SYSTEMATICS)
    entry = project / "workflow.yaml"
    entry.write_text(yaml.safe_dump(entry_doc, sort_keys=False), encoding="utf-8")
    return entry


def read_manifest(compile_dir: Path) -> dict[str, Any]:
    text = (compile_dir / "inputs.manifest.yaml").read_text(encoding="utf-8")
    return yaml.safe_load(text)


def rows_by_resolved(manifest: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    return {row["resolved"]: row for row in manifest["inputs"]}
