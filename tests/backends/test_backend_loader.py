from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import Any

import pytest
import yaml

from hepflow.api import compile_workflow_file
from hepflow.backends.loaders import (
    backend_build_directories,
    load_backend,
    normalize_backend_override,
)
from hepflow.compiler import profiles as profile_module
from hepflow.compiler.lower_graph import lower_workflow_to_graph
from hepflow.compiler.normalize import normalize_workflow
from hepflow.compiler.plan import build_execution_plan


def test_local_default_backend_loads(toy_workflow: dict[str, Any]) -> None:
    normalized = normalize_workflow(toy_workflow)
    plan = build_execution_plan(
        lower_workflow_to_graph(normalized),
        registry=normalized["registry"],
    )

    backend = load_backend(plan)

    assert backend.name == "local.default"


def test_dask_backend_requires_fasthep_distributed_at_compile(
    toy_workflow: dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _force_fasthep_distributed_unavailable(monkeypatch)
    workflow_path = _write_workflow(
        tmp_path,
        {
            **toy_workflow,
            "use": {"profiles": ["basic"]},
            "execution": {"backend": "dask", "strategy": "local"},
        },
    )

    with pytest.raises(KeyError, match="Install fasthep-distributed"):
        compile_workflow_file(workflow_path, outdir=tmp_path / "build")


def test_unknown_backend_without_matching_provider_metadata_is_generic(
    toy_workflow: dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _force_fasthep_distributed_unavailable(monkeypatch)
    workflow_path = _write_workflow(
        tmp_path,
        {
            **toy_workflow,
            "use": {"profiles": ["basic"]},
            "execution": {"backend": "mystery", "strategy": "default"},
        },
    )

    with pytest.raises(KeyError, match=r"Unknown backend strategy 'mystery.default'"):
        compile_workflow_file(workflow_path, outdir=tmp_path / "build")


def test_dask_backend_resolves_from_fasthep_distributed_profile(
    toy_workflow: dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_fasthep_distributed(tmp_path, monkeypatch)
    workflow_path = _write_workflow(
        tmp_path,
        {
            **toy_workflow,
            "use": {"profiles": ["basic"]},
            "execution": {"backend": "dask", "strategy": "local"},
        },
    )

    plan = compile_workflow_file(workflow_path, outdir=tmp_path / "build")
    backend = load_backend(plan)

    assert backend.name == "dask"
    assert type(backend).__module__ == "fasthep_distributed._dask._common"
    assert plan.registry["backends"]["dask"]["impl"] == (
        "fasthep_distributed._dask._common:DaskBackend"
    )
    assert plan.registry["backends"]["dask"]["spec"] == (
        "fasthep_distributed._dask._spec:DASK_BACKEND_SPEC"
    )


def test_fake_external_backend_spec_validates_and_declares_build_dirs(
    toy_workflow: dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_backend_provider(tmp_path, monkeypatch)
    workflow_path = _write_workflow(
        tmp_path,
        {
            **toy_workflow,
            "use": {"profiles": ["fake_backend:registry"]},
            "execution": {"backend": "fake", "strategy": "cluster"},
        },
    )

    plan = compile_workflow_file(workflow_path, outdir=tmp_path / "build")

    assert backend_build_directories(plan.registry, plan.execution) == (
        "execution/fake/cluster",
    )
    assert plan.provenance["backend_build_directories"] == [
        "execution/fake/cluster"
    ]
    assert (tmp_path / "build" / "execution" / "fake" / "cluster").is_dir()


def test_importing_flow_backends_does_not_import_dask_modules() -> None:
    for module_name in ["dask", "distributed", "dask_jobqueue"]:
        sys.modules.pop(module_name, None)

    import hepflow.backends  # noqa: F401, PLC0415

    assert "dask" not in sys.modules
    assert "distributed" not in sys.modules
    assert "dask_jobqueue" not in sys.modules


def test_dask_unsupported_strategy_errors_during_compile(
    toy_workflow: dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_fasthep_distributed(tmp_path, monkeypatch)
    workflow_path = _write_workflow(
        tmp_path,
        {
            **toy_workflow,
            "use": {"profiles": ["basic"]},
            "execution": {"backend": "dask", "strategy": "pbs"},
        },
    )

    with pytest.raises(ValueError, match=r"Dask strategy 'pbs' is not implemented yet"):
        compile_workflow_file(workflow_path, outdir=tmp_path / "build")


def test_unsafe_backend_build_directories_are_rejected(
    toy_workflow: dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_backend_provider(
        tmp_path,
        monkeypatch,
        build_directories=("../outside",),
    )
    workflow_path = _write_workflow(
        tmp_path,
        {
            **toy_workflow,
            "use": {"profiles": ["fake_backend:registry"]},
            "execution": {"backend": "fake", "strategy": "cluster"},
        },
    )

    with pytest.raises(ValueError, match="must not contain traversal"):
        compile_workflow_file(workflow_path, outdir=tmp_path / "build")


def test_shorthand_backend_override_splits_backend_and_strategy() -> None:
    assert normalize_backend_override("dask.local", None) == {
        "backend": "dask",
        "strategy": "local",
    }


def _write_workflow(tmp_path: Path, workflow: dict[str, Any]) -> Path:
    workflow_path = tmp_path / "workflow.yaml"
    workflow_path.write_text(yaml.safe_dump(workflow, sort_keys=False), encoding="utf-8")
    return workflow_path


def _install_fake_fasthep_distributed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    package = tmp_path / "fasthep_distributed"
    profiles = package / "profiles"
    dask_package = package / "_dask"
    profiles.mkdir(parents=True)
    dask_package.mkdir()
    (package / "__init__.py").write_text("", encoding="utf-8")
    (profiles / "__init__.py").write_text("", encoding="utf-8")
    (dask_package / "__init__.py").write_text("", encoding="utf-8")
    (profiles / "registry.yaml").write_text(
        "\n".join(
            [
                "registry:",
                "  backends:",
                "    dask:",
                "      spec: fasthep_distributed._dask._spec:DASK_BACKEND_SPEC",
                "      impl: fasthep_distributed._dask._common:DaskBackend",
            ]
        ),
        encoding="utf-8",
    )
    (dask_package / "_spec.py").write_text(
        "\n".join(
            [
                "from hepflow.backends.model import BackendSpec",
                "def validate_dask_execution(execution):",
                "    strategy = execution.get('strategy', 'default')",
                "    if strategy not in {None, '', 'default', 'local', 'htcondor', 'slurm'}:",
                "        raise ValueError(f\"Dask strategy {strategy!r} is not implemented yet.\")",
                "DASK_BACKEND_SPEC = BackendSpec(",
                "    validate_execution=validate_dask_execution,",
                "    build_directories=(",
                "        'execution/dask/htcondor/submit',",
                "        'execution/dask/htcondor/logs',",
                "        'execution/dask/htcondor/out',",
                "        'execution/dask/htcondor/err',",
                "        'debug/dask',",
                "    ),",
                ")",
            ]
        ),
        encoding="utf-8",
    )
    (dask_package / "_common.py").write_text(
        "\n".join(
            [
                "class DaskBackend:",
                "    name = 'dask'",
                "    def run(self, *args, **kwargs):",
                "        raise NotImplementedError",
            ]
        ),
        encoding="utf-8",
    )
    for module_name in list(sys.modules):
        if module_name == "fasthep_distributed" or module_name.startswith(
            "fasthep_distributed."
        ):
            monkeypatch.delitem(sys.modules, module_name, raising=False)
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()


def _install_fake_backend_provider(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    build_directories: tuple[str, ...] = ("execution/fake/cluster",),
) -> None:
    package = tmp_path / "fake_backend"
    profiles = package / "profiles"
    profiles.mkdir(parents=True)
    (package / "__init__.py").write_text("", encoding="utf-8")
    (profiles / "__init__.py").write_text("", encoding="utf-8")
    (profiles / "registry.yaml").write_text(
        "\n".join(
            [
                "registry:",
                "  backends:",
                "    fake:",
                "      spec: fake_backend:FAKE_BACKEND_SPEC",
                "      impl: fake_backend:FakeBackend",
            ]
        ),
        encoding="utf-8",
    )
    directories_repr = repr(build_directories)
    (package / "__init__.py").write_text(
        "\n".join(
            [
                "from hepflow.backends.model import BackendSpec",
                "class FakeBackend:",
                "    name = 'fake'",
                "    def run(self, *args, **kwargs):",
                "        raise NotImplementedError",
                "def validate_execution(execution):",
                "    if execution.get('strategy') != 'cluster':",
                "        raise ValueError('fake strategy is invalid')",
                "FAKE_BACKEND_SPEC = BackendSpec(",
                "    validate_execution=validate_execution,",
                f"    build_directories={directories_repr},",
                ")",
            ]
        ),
        encoding="utf-8",
    )
    for module_name in list(sys.modules):
        if module_name == "fake_backend" or module_name.startswith("fake_backend."):
            monkeypatch.delitem(sys.modules, module_name, raising=False)
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()


def _force_fasthep_distributed_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_files = profile_module.resources.files

    def fake_files(package: str) -> Any:
        if package == "fasthep_distributed.profiles":
            raise ModuleNotFoundError(package)
        return real_files(package)

    monkeypatch.setattr(profile_module.resources, "files", fake_files)
