from __future__ import annotations

from importlib import resources
from pathlib import Path
from typing import Any

import yaml

from hepflow.registry.merge import RegistryLayer


def project_profile_dir(root: Path) -> Path:
    """Return the project-local directory reserved for explicit profile files."""
    return root / ".hepflow" / "profiles"


def default_profile_search_paths(root: Path) -> list[Path]:
    """Return profile search paths for future explicit profile loading.

    Future config provenance should merge in this order:
    builtin defaults < selected profile configs < author.yaml < CLI overrides.
    Packaged builtin profile templates can be appended here once real profiles
    exist.
    """
    return [
        project_profile_dir(root),
    ]


def normalize_profile_names(raw: Any) -> list[str]:
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise ValueError("use.profiles must be a list of strings")
    names: list[str] = []
    for index, item in enumerate(raw):
        if not isinstance(item, str) or not item.strip():
            raise ValueError(f"use.profiles[{index}] must be a non-empty string")
        names.append(item.strip())
    return names


def load_profile_config(
    name: str,
    *,
    project_root: Path,
) -> dict[str, Any]:
    """Load a selected profile from project-local profiles, then packaged profiles."""
    return _load_profile_with_provenance(name, project_root=project_root)[0]


def load_profile_registry_layer(
    name: str,
    *,
    project_root: Path,
) -> RegistryLayer:
    config, provenance = _load_profile_with_provenance(name, project_root=project_root)
    return RegistryLayer(
        name=name,
        kind="profile",
        registry=dict(config.get("registry") or {}),
        path=provenance["path"],
    )


def load_profile_config_with_provenance(
    name: str,
    *,
    project_root: Path,
) -> tuple[dict[str, Any], dict[str, str]]:
    return _load_profile_with_provenance(name, project_root=project_root)


def _load_profile_with_provenance(
    name: str,
    *,
    project_root: Path,
) -> tuple[dict[str, Any], dict[str, str]]:
    if not isinstance(name, str) or not name.strip():
        raise ValueError("Profile name must be a non-empty string")
    if ":" in name:
        return _load_qualified_package_profile(name)
    if "/" in name or "\\" in name or name in {".", ".."}:
        raise ValueError(f"Invalid profile name: {name!r}")

    filename = f"{name}.yaml"
    local_path = project_profile_dir(project_root) / filename
    if local_path.exists():
        loaded = yaml.safe_load(local_path.read_text(encoding="utf-8")) or {}
        if not isinstance(loaded, dict):
            raise ValueError(f"Profile {name!r} must contain a YAML mapping")
        return loaded, {"path": str(local_path.relative_to(project_root))}

    package_resource = resources.files("hepflow.profiles").joinpath(filename)
    if package_resource.is_file():
        loaded = yaml.safe_load(package_resource.read_text(encoding="utf-8")) or {}
        if not isinstance(loaded, dict):
            raise ValueError(f"Profile {name!r} must contain a YAML mapping")
        return loaded, {"path": f"package:hepflow.profiles/{filename}"}

    raise FileNotFoundError(
        f"Profile {name!r} not found in {project_profile_dir(project_root)} "
        f"or package hepflow.profiles"
    )


def _load_qualified_package_profile(
    name: str,
) -> tuple[dict[str, Any], dict[str, str]]:
    package_name, profile_name = name.split(":", 1)
    if not package_name or not profile_name:
        raise ValueError(
            f"Invalid qualified profile name {name!r}; expected package:profile"
        )
    if "/" in profile_name or "\\" in profile_name or profile_name in {".", ".."}:
        raise ValueError(f"Invalid profile name: {profile_name!r}")

    package = f"{package_name}.profiles"
    filename = f"{profile_name}.yaml"
    try:
        package_resource = resources.files(package).joinpath(filename)
    except ModuleNotFoundError as exc:
        raise FileNotFoundError(
            f"Profile package {package!r} not found while loading {name!r}"
        ) from exc

    if not package_resource.is_file():
        raise FileNotFoundError(f"Profile {name!r} not found at package:{package}/{filename}")

    loaded = yaml.safe_load(package_resource.read_text(encoding="utf-8")) or {}
    if not isinstance(loaded, dict):
        raise ValueError(f"Profile {name!r} must contain a YAML mapping")
    return loaded, {"path": f"package:{package}/{filename}"}
