from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from importlib import import_module, resources
from importlib.resources.abc import Traversable
from pathlib import Path

from hepflow.compiler.profiles import resolve_profile_source

HEP_PROFILE_PACKAGES = [
    f"fasthep_{name}"
    for name in ("curator", "carpenter", "render")
]


@dataclass(slots=True)
class InitResult:
    profile_dir: Path
    created_profile_dir: bool
    copied: list[Path] = field(default_factory=list)
    skipped_existing: list[Path] = field(default_factory=list)
    overwritten: list[Path] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def written(self) -> list[Path]:
        return [*self.copied, *self.overwritten]

    def __iter__(self):
        return iter(self.written)

    def __len__(self) -> int:
        return len(self.written)

    def __contains__(self, path: object) -> bool:
        return path in self.written


def init_project(
    *,
    target_dir: str | Path,
    force: bool = False,
    include: Iterable[str] | None = None,
    profiles: Iterable[str] | None = None,
) -> InitResult:
    project_dir = Path(target_dir)
    profiles_root = project_dir / ".fasthep" / "profiles"
    profile_dir = profiles_root / "hepflow"
    created_profile_dir = not profile_dir.exists()
    profile_dir.mkdir(parents=True, exist_ok=True)

    result = InitResult(
        profile_dir=profile_dir,
        created_profile_dir=created_profile_dir,
    )
    _copy_bundled_flow_profiles(result=result, profile_dir=profile_dir, force=force)
    _copy_includes(
        result=result,
        project_dir=project_dir,
        profiles_root=profiles_root,
        include=include,
        force=force,
    )
    for profile_name in profiles or []:
        _copy_profile_bundle(
            result=result,
            profiles_root=profiles_root,
            profile_name=profile_name,
            force=force,
        )
    return result


def _copy_bundled_flow_profiles(
    *,
    result: InitResult,
    profile_dir: Path,
    force: bool,
) -> None:
    for relative_path, source in _packaged_profile_files():
        destination = profile_dir / relative_path
        _copy_resource(result=result, source=source, destination=destination, force=force)


def _copy_includes(
    *,
    result: InitResult,
    project_dir: Path,
    profiles_root: Path,
    include: Iterable[str] | None,
    force: bool,
) -> None:
    for profile_ref in include or []:
        profile = resolve_profile_source(profile_ref, project_root=project_dir)
        destination = profiles_root / profile.owner / profile.filename
        _copy_resource(
            result=result,
            source=profile.source,
            destination=destination,
            force=force,
        )


def _packaged_profile_files() -> Iterable[tuple[Path, Traversable]]:
    profiles_root = resources.files("hepflow.profiles")

    def walk(
        node: Traversable,
        relative_to_root: Path,
    ) -> Iterable[tuple[Path, Traversable]]:
        for child in node.iterdir():
            if child.name == "__pycache__":
                continue
            child_relative = relative_to_root / child.name
            if child.is_dir():
                yield from walk(child, child_relative)
                continue
            if child.name.endswith(".py"):
                continue
            yield child_relative, child

    yield from walk(profiles_root, Path())


def _copy_profile_bundle(
    *,
    result: InitResult,
    profiles_root: Path,
    profile_name: str,
    force: bool,
) -> None:
    normalized = str(profile_name).casefold()
    if normalized != "hep":
        result.warnings.append(f"unknown profile bundle: {profile_name}")
        return

    for package_name in HEP_PROFILE_PACKAGES:
        _copy_package_profiles(
            result=result,
            profiles_root=profiles_root,
            package_name=package_name,
            force=force,
        )


def _copy_package_profiles(
    *,
    result: InitResult,
    profiles_root: Path,
    package_name: str,
    force: bool,
) -> None:
    try:
        import_module(package_name)
    except ImportError:
        result.warnings.append(f"profile package not found: {package_name}")
        return

    profile_dir = resources.files(package_name).joinpath("profiles")
    if not profile_dir.is_dir():
        yaml_files = []
    else:
        yaml_files = sorted(
            (
                child
                for child in profile_dir.iterdir()
                if child.is_file() and child.name.endswith(".yaml")
            ),
            key=lambda child: child.name,
        )

    if not yaml_files:
        result.warnings.append(
            f"profile package has no profiles/*.yaml files: {package_name}"
        )
        return

    destination_dir = profiles_root / package_name
    for source in yaml_files:
        _copy_resource(
            result=result,
            source=source,
            destination=destination_dir / source.name,
            force=force,
        )


def _copy_resource(
    *,
    result: InitResult,
    source: Traversable,
    destination: Path,
    force: bool,
) -> None:
    exists = destination.exists()
    if exists and not force:
        result.skipped_existing.append(destination)
        return
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(source.read_bytes())
    if exists:
        result.overwritten.append(destination)
    else:
        result.copied.append(destination)
