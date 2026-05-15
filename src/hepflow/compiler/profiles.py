from __future__ import annotations

from dataclasses import dataclass
from importlib import resources
from importlib.resources.abc import Traversable
from pathlib import Path
from typing import Any

import yaml

from hepflow.registry.merge import RegistryLayer


@dataclass(frozen=True, slots=True)
class ProfileSource:
    ref: str
    owner: str
    filename: str
    path: str
    source: Path | Traversable


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


def resolve_profile_source(
    name: str,
    *,
    project_root: Path,
) -> ProfileSource:
    """Resolve a profile reference without loading its YAML content."""
    if not isinstance(name, str) or not name.strip():
        raise ValueError("Profile name must be a non-empty string")
    name = name.strip()
    if ":" in name:
        return _resolve_qualified_package_profile(name)
    if _looks_like_path(name):
        return _resolve_local_profile_path(name, project_root=project_root)

    if "/" in name or "\\" in name or name in {".", ".."}:
        raise ValueError(f"Invalid profile name: {name!r}")

    filename = f"{name}.yaml"
    local_path = project_profile_dir(project_root) / filename
    if local_path.exists():
        return ProfileSource(
            ref=name,
            owner="local",
            filename=local_path.name,
            path=str(local_path.relative_to(project_root)),
            source=local_path,
        )

    package_resource = resources.files("hepflow.profiles").joinpath(filename)
    if package_resource.is_file():
        return ProfileSource(
            ref=name,
            owner="hepflow",
            filename=filename,
            path=f"package:hepflow.profiles/{filename}",
            source=package_resource,
        )

    raise FileNotFoundError(
        f"Profile {name!r} not found in {project_profile_dir(project_root)} "
        f"or package hepflow.profiles"
    )


def _load_profile_with_provenance(
    name: str,
    *,
    project_root: Path,
) -> tuple[dict[str, Any], dict[str, str]]:
    profile = resolve_profile_source(name, project_root=project_root)
    loaded = yaml.safe_load(profile.source.read_text(encoding="utf-8")) or {}
    if not isinstance(loaded, dict):
        raise ValueError(f"Profile {name!r} must contain a YAML mapping")
    return loaded, {"path": profile.path}


def _resolve_qualified_package_profile(
    name: str,
) -> ProfileSource:
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
        raise FileNotFoundError(
            f"Profile {name!r} not found at package:{package}/{filename}"
        )

    return ProfileSource(
        ref=name,
        owner=package_name,
        filename=filename,
        path=f"package:{package}/{filename}",
        source=package_resource,
    )


def _resolve_local_profile_path(
    name: str,
    *,
    project_root: Path,
) -> ProfileSource:
    profile_path = Path(name)
    if not profile_path.is_absolute():
        profile_path = project_root / profile_path
    if not profile_path.is_file():
        raise FileNotFoundError(f"Profile path {name!r} not found")
    return ProfileSource(
        ref=name,
        owner="local",
        filename=profile_path.name,
        path=str(profile_path),
        source=profile_path,
    )


def _looks_like_path(name: str) -> bool:
    return (
        "/" in name
        or "\\" in name
        or name in {".", ".."}
        or name.startswith(".")
        or name.endswith((".yaml", ".yml"))
    )
