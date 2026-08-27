from __future__ import annotations

import importlib.util
import json
import sys
import types
from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast

import pytest

from hepflow.backends._dask._pooled import (
    DaskPooledCluster,
    normalize_pooled_worker_pools,
)
from hepflow.backends._dask._worker_env import (
    EditableDistribution,
    PackedPixiEnvironmentSpec,
    PreparedWorkerEnvironment,
    WorkerCredential,
    build_htcondor_worker_environment_job_kwargs,
    build_packed_pixi_worker_environment,
    discover_editable_distributions,
    pack_relocatable_prefix,
    prepare_staged_execution_files,
    prepare_worker_environment,
    resolve_pixi_prefix,
    stage_editable_wheels,
    verify_transfer_files,
    x509_proxy_from_environment,
)
from hepflow.build_layout import BuildPaths


class FakeJob:
    pass


class FakePooledCluster(DaskPooledCluster):
    job_cls = FakeJob


def test_pooled_cluster_creates_scheduler_spec() -> None:
    cluster = FakePooledCluster(
        pools=_pool_config(),
        scheduler_options={"dashboard_address": ":0"},
        start=False,
    )

    assert cluster.scheduler_spec["options"] == {"dashboard_address": ":0"}


def test_pooled_cluster_creates_initial_worker_specs() -> None:
    cluster = FakePooledCluster(pools=_pool_config(), start=False)

    assert sorted(cluster.worker_spec) == [
        "default-0",
        "default-1",
        "high_memory-0",
    ]
    assert cluster.worker_spec["default-0"]["cls"] is FakeJob
    assert "pool" not in cluster.worker_spec["default-0"]["options"]
    assert "pool" not in cluster.worker_spec["high_memory-0"]["options"]
    assert cluster._worker_pool == {
        "default-0": "default",
        "default-1": "default",
        "high_memory-0": "high_memory",
    }


def test_pooled_cluster_worker_specs_keep_distinct_job_kwargs() -> None:
    cluster = FakePooledCluster(pools=_pool_config(), start=False)

    assert cluster.worker_spec["default-0"]["options"]["memory"] == "4GB"
    assert cluster.worker_spec["high_memory-0"]["options"]["memory"] == "32GB"
    assert cluster.worker_spec["default-0"]["options"]["worker_extra_args"] == [
        "--resources",
        "resource.default=1",
    ]
    assert cluster.worker_spec["high_memory-0"]["options"]["worker_extra_args"] == [
        "--resources",
        "resource.high_memory=1",
    ]


def test_pooled_cluster_common_job_kwargs_merge_with_pool_overrides() -> None:
    cluster = FakePooledCluster(
        pools={
            "default": {
                "workers": 1,
                "job_kwargs": {
                    "memory": "4GB",
                    "resources": {"resource.default": 1},
                },
            },
            "high_memory": {
                "workers": 1,
                "job_kwargs": {
                    "memory": "32GB",
                    "job_extra_directives": {"RequestMemory": "32768"},
                    "job_script_prologue": ["echo pool"],
                    "resources": {"resource.high_memory": 1},
                },
            },
        },
        job_kwargs={
            "cores": 1,
            "memory": "2GB",
            "python": "./env/bin/python",
            "job_extra_directives": {"should_transfer_files": "YES"},
            "job_script_prologue": ["echo common"],
        },
        start=False,
    )

    assert cluster.worker_spec["default-0"]["options"]["memory"] == "4GB"
    assert cluster.worker_spec["default-0"]["options"]["python"] == "./env/bin/python"
    assert cluster.worker_spec["high_memory-0"]["options"]["memory"] == "32GB"
    assert cluster.worker_spec["high_memory-0"]["options"]["job_extra_directives"] == {
        "should_transfer_files": "YES",
        "RequestMemory": "32768",
    }
    assert cluster.worker_spec["high_memory-0"]["options"]["job_script_prologue"] == [
        "echo common",
        "echo pool",
    ]


def test_pooled_cluster_scale_mapping_replaces_worker_targets() -> None:
    cluster = FakePooledCluster(pools=_pool_config(), start=False)

    cluster.scale({"default": 1, "high_memory": 2})

    assert sorted(cluster.worker_spec) == [
        "default-0",
        "high_memory-0",
        "high_memory-1",
    ]
    assert cluster._worker_pool == {
        "default-0": "default",
        "high_memory-0": "high_memory",
        "high_memory-1": "high_memory",
    }


def test_pooled_cluster_integer_scale_zero_clears_multiple_pools() -> None:
    cluster = FakePooledCluster(pools=_pool_config(), start=False)

    cluster.scale(0)

    assert cluster.worker_spec == {}
    assert cluster._worker_pool == {}


def test_pooled_cluster_normalize_scale_zero_targets_all_pools() -> None:
    cluster = FakePooledCluster(pools=_pool_config(), start=False)

    assert cluster._normalize_scale(0) == {"default": 0, "high_memory": 0}


def test_pooled_cluster_integer_scale_supports_single_default_pool() -> None:
    cluster = FakePooledCluster(
        pools={
            "default": {
                "workers": 1,
                "job_kwargs": {
                    "cores": 1,
                    "memory": "4GB",
                    "resources": {"resource.default": 1},
                },
            }
        },
        start=False,
    )

    cluster.scale(3)

    assert sorted(cluster.worker_spec) == ["default-0", "default-1", "default-2"]


def test_pooled_cluster_integer_scale_errors_for_multiple_pools() -> None:
    cluster = FakePooledCluster(pools=_pool_config(), start=False)

    with pytest.raises(
        ValueError,
        match="Integer scale is only supported for a single default pool",
    ):
        cluster.scale(3)


def test_pooled_cluster_unknown_pool_errors_clearly() -> None:
    cluster = FakePooledCluster(pools=_pool_config(), start=False)

    with pytest.raises(ValueError, match="Unknown Dask worker pool 'missing'"):
        cluster.scale({"missing": 1})


@pytest.mark.parametrize("workers", [-1, "two"])
def test_pooled_cluster_invalid_worker_counts_error(workers: Any) -> None:
    with pytest.raises(ValueError, match=r"pools\['default'\]\.workers must be"):
        normalize_pooled_worker_pools(
            {"default": {"workers": workers, "job_kwargs": {}}}
        )


def test_pooled_cluster_invalid_scale_count_errors() -> None:
    cluster = FakePooledCluster(pools=_pool_config(), start=False)

    with pytest.raises(ValueError, match=r"scale\['default'\] must be"):
        cluster.scale({"default": -1})


def test_packed_pixi_environment_spec_serializes() -> None:
    spec = PackedPixiEnvironmentSpec(
        environment="default",
        archive_path="debug/distributed/htcondor/env.sh",
        worker_env_dir="worker-env",
    )

    assert spec.to_dict() == {
        "type": "packed-pixi",
        "environment": "default",
        "archive_path": "debug/distributed/htcondor/env.sh",
        "worker_env_dir": "worker-env",
    }


def test_packed_pixi_worker_environment_describes_unpack() -> None:
    env = build_packed_pixi_worker_environment(
        PackedPixiEnvironmentSpec(
            environment="default",
            archive_path="debug/distributed/htcondor/env.sh",
            worker_env_dir="worker-env",
        )
    )

    assert env.python == "./worker-env/bin/python"
    assert env.transfer_files == [Path("debug/distributed/htcondor/env.sh")]
    assert any(
        "./env.sh --output-directory . --env-name worker-env" in item
        for item in env.prologue
    )
    assert any("./worker-env/bin/python --version" in item for item in env.prologue)


def test_htcondor_worker_environment_job_kwargs_include_transfer_directives(
    tmp_path: Path,
) -> None:
    paths = {
        "logs": tmp_path / "logs",
        "out": tmp_path / "out",
        "err": tmp_path / "err",
    }
    env = build_packed_pixi_worker_environment(
        PackedPixiEnvironmentSpec(
            environment="default",
            archive_path=str(tmp_path / "env.sh"),
            worker_env_dir="worker-env",
        )
    )
    kwargs = build_htcondor_worker_environment_job_kwargs(env, log_paths=paths)

    directives = cast("Mapping[str, str]", kwargs["job_extra_directives"])
    assert directives["should_transfer_files"] == "YES"
    assert directives["when_to_transfer_output"] == "ON_EXIT"
    assert directives["transfer_executable"] == "False"
    assert directives["transfer_output_files"] == '""'
    assert directives["transfer_input_files"].endswith("/env.sh")
    assert "out/worker-$(ClusterId).$(ProcId).out" in directives["Output"]
    assert "err/worker-$(ClusterId).$(ProcId).err" in directives["Error"]
    assert kwargs["python"] == "./worker-env/bin/python"
    assert not Path(str(kwargs["python"])).is_absolute()
    prologue = cast("list[str]", kwargs["job_script_prologue"])
    assert any(
        "./env.sh --output-directory . --env-name worker-env" in item
        for item in prologue
    )


def test_x509_proxy_from_environment_returns_none_when_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("X509_USER_PROXY", raising=False)

    assert x509_proxy_from_environment() is None


def test_x509_proxy_from_environment_can_require_env_var(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("X509_USER_PROXY", raising=False)

    with pytest.raises(ValueError, match="X509_USER_PROXY is not set"):
        x509_proxy_from_environment(required=True)


def test_x509_proxy_from_environment_resolves_existing_proxy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    proxy = tmp_path / "x509up_u12345"
    proxy.write_text("secret proxy bytes", encoding="utf-8")
    monkeypatch.setenv("X509_USER_PROXY", str(proxy))

    credential = x509_proxy_from_environment()

    assert credential == WorkerCredential(
        type="x509_proxy",
        source_path=proxy.resolve(),
        target_name="x509_proxy",
        env_var="X509_USER_PROXY",
    )


def test_x509_proxy_from_environment_errors_for_missing_proxy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    proxy = tmp_path / "missing-proxy"
    monkeypatch.setenv("X509_USER_PROXY", str(proxy))

    with pytest.raises(FileNotFoundError, match="missing X509 proxy file"):
        x509_proxy_from_environment()


def test_htcondor_worker_environment_job_kwargs_include_x509_credential(
    tmp_path: Path,
) -> None:
    paths = {
        "logs": tmp_path / "logs",
        "out": tmp_path / "out",
        "err": tmp_path / "err",
    }
    env_archive = tmp_path / "env.sh"
    proxy = tmp_path / "x509up_u12345"
    env_archive.write_text("env", encoding="utf-8")
    proxy.write_text("secret proxy bytes", encoding="utf-8")
    env = build_packed_pixi_worker_environment(
        PackedPixiEnvironmentSpec(
            environment="default",
            archive_path=str(env_archive),
            worker_env_dir="worker-env",
        ),
        credentials=[
            WorkerCredential(
                type="x509_proxy",
                source_path=proxy,
                target_name="x509_proxy",
                env_var="X509_USER_PROXY",
            )
        ],
    )

    kwargs = build_htcondor_worker_environment_job_kwargs(env, log_paths=paths)

    directives = cast("Mapping[str, str]", kwargs["job_extra_directives"])
    transfer_files = directives["transfer_input_files"]
    assert str(env_archive.resolve()) in transfer_files
    assert str(proxy.resolve()) in transfer_files
    prologue = cast("list[str]", kwargs["job_script_prologue"])
    assert any("configuring x509_proxy credential" in item for item in prologue)
    assert 'export X509_USER_PROXY="$X509_USER_PROXY_TARGET"' in prologue
    assert 'chmod 600 "$X509_USER_PROXY"' in prologue
    assert not any("secret proxy bytes" in item for item in prologue)
    assert any(
        "./env.sh --output-directory . --env-name worker-env" in item
        for item in prologue
    )


def test_htcondor_worker_environment_transfer_basenames_must_be_unique(
    tmp_path: Path,
) -> None:
    paths = {
        "logs": tmp_path / "logs",
        "out": tmp_path / "out",
        "err": tmp_path / "err",
    }
    first = tmp_path / "a" / "x509_proxy"
    second = tmp_path / "b" / "x509_proxy"
    env = build_packed_pixi_worker_environment(
        PackedPixiEnvironmentSpec(
            environment="default",
            archive_path=str(first),
            worker_env_dir="worker-env",
        ),
        credentials=[
            WorkerCredential(
                type="x509_proxy",
                source_path=second,
                target_name="x509_proxy",
                env_var="X509_USER_PROXY",
            )
        ],
    )

    with pytest.raises(ValueError, match="unique basenames"):
        build_htcondor_worker_environment_job_kwargs(env, log_paths=paths)


def test_resolve_pixi_prefix_uses_current_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prefix = tmp_path / "prefix"
    (prefix / "bin").mkdir(parents=True)
    (prefix / "bin" / "python").write_text("", encoding="utf-8")
    monkeypatch.setenv("PIXI_ENVIRONMENT_NAME", "default")
    monkeypatch.setenv("CONDA_PREFIX", str(prefix))

    assert resolve_pixi_prefix("default") == prefix.resolve()


def test_resolve_pixi_prefix_uses_project_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prefix = tmp_path / ".pixi" / "envs" / "analysis"
    (prefix / "bin").mkdir(parents=True)
    (prefix / "bin" / "python").write_text("", encoding="utf-8")
    monkeypatch.setenv("PIXI_ENVIRONMENT_NAME", "default")
    monkeypatch.setenv("CONDA_PREFIX", str(tmp_path / "other"))
    monkeypatch.setenv("PIXI_PROJECT_ROOT", str(tmp_path))

    assert resolve_pixi_prefix("analysis") == prefix.resolve()


def test_resolve_pixi_prefix_errors_for_missing_prefix(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PIXI_PROJECT_ROOT", str(tmp_path))

    with pytest.raises(FileNotFoundError, match="Pixi environment prefix does not exist"):
        resolve_pixi_prefix("missing")


def test_pack_relocatable_prefix_uses_conda_pack(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, Any]] = []

    def fake_pack(**kwargs: Any) -> None:
        calls.append(kwargs)
        Path(str(kwargs["output"])).write_text("archive", encoding="utf-8")

    monkeypatch.setitem(sys.modules, "conda_pack", types.SimpleNamespace(pack=fake_pack))
    archive = pack_relocatable_prefix(tmp_path / "prefix", tmp_path / "env.tar.gz")

    assert archive == tmp_path / "env.tar.gz"
    assert calls == [
        {
            "prefix": str(tmp_path / "prefix"),
            "output": str(tmp_path / "env.tar.gz"),
            "force": True,
            "ignore_editable_packages": True,
        }
    ]


def test_discover_editable_distributions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    site = tmp_path / "site-packages"
    dist_info = site / "fasthep_workshop-1.2.3.dist-info"
    dist_info.mkdir(parents=True)
    (dist_info / "METADATA").write_text(
        "Name: fasthep-workshop\nVersion: 1.2.3\n",
        encoding="utf-8",
    )
    source = tmp_path / "src" / "workshop"
    source.mkdir(parents=True)
    (source / "pyproject.toml").write_text("[project]\nname='x'\n", encoding="utf-8")
    (dist_info / "direct_url.json").write_text(
        json.dumps({"url": f"file://{source}", "dir_info": {"editable": True}}),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "hepflow.backends._dask._worker_env.sysconfig.get_path",
        lambda *args, **kwargs: str(site),
    )

    assert discover_editable_distributions(tmp_path / "prefix") == [
        EditableDistribution(
            name="fasthep-workshop",
            version="1.2.3",
            source_path=source.resolve(),
        )
    ]


def test_stage_editable_wheels_records_hashes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "src"
    source.mkdir()

    def fake_run(cmd: list[str], *, check: bool) -> None:
        assert check is True
        outdir = Path(cmd[cmd.index("--outdir") + 1])
        outdir.mkdir(parents=True, exist_ok=True)
        (outdir / "fasthep_workshop-1.0.0-py3-none-any.whl").write_text(
            "wheel",
            encoding="utf-8",
        )

    monkeypatch.setattr(
        "hepflow.backends._dask._worker_env.subprocess.run",
        fake_run,
    )
    files, manifest = stage_editable_wheels(
        [
            EditableDistribution(
                name="fasthep-workshop",
                version="1.0.0",
                source_path=source,
            )
        ],
        application_dir=tmp_path / "applications" / "app",
    )

    payload = json.loads(manifest.read_text(encoding="utf-8"))
    assert files == [tmp_path / "applications" / "app" / "wheelhouse"]
    assert payload["packages"][0]["name"] == "fasthep-workshop"
    assert payload["wheels"][0]["sha256"]


def test_prepare_prefix_environment_creates_archive_bootstrap_and_manifest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prefix = tmp_path / "prefix"
    (prefix / "bin").mkdir(parents=True)
    (prefix / "bin" / "python").write_text("", encoding="utf-8")
    monkeypatch.setenv("PIXI_ENVIRONMENT_NAME", "default")
    monkeypatch.setenv("CONDA_PREFIX", str(prefix))
    monkeypatch.setattr(
        "hepflow.backends._dask._worker_env.pack_relocatable_prefix",
        lambda prefix, output: output.write_text("archive", encoding="utf-8") or output,
    )
    monkeypatch.setattr(
        "hepflow.backends._dask._worker_env.discover_editable_distributions",
        lambda prefix: [],
    )
    monkeypatch.setattr(
        "hepflow.backends._dask._worker_env.required_fasthep_imports",
        lambda prefix, editables: ["distributed", "hepflow"],
    )

    prepared = prepare_worker_environment(
        {
            "environment": {
                "type": "packed-pixi",
                "mode": "prefix",
                "environment": "default",
            }
        },
        build_paths=BuildPaths(root=tmp_path / "build"),
    )

    assert prepared is not None
    assert prepared.python == "./worker-env/bin/python"
    assert prepared.transfer_files == []
    assert prepared.environment_archive is not None
    assert prepared.bootstrap_script is not None
    bootstrap = prepared.bootstrap_script
    script = bootstrap.read_text(encoding="utf-8")
    assert 'tar -xzf "$ENV_ARCHIVE" -C worker-env' in script
    assert "./worker-env/bin/conda-unpack" in script
    assert script.index("conda-unpack") < script.index("distributed.cli.dask_worker")
    manifest = json.loads(prepared.environment_manifest.read_text(encoding="utf-8"))
    assert manifest["archive"]["sha256"]
    assert manifest["worker_python"] == "./worker-env/bin/python"


def test_shared_staging_transfers_no_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prefix = tmp_path / "prefix"
    (prefix / "bin").mkdir(parents=True)
    (prefix / "bin" / "python").write_text("", encoding="utf-8")
    monkeypatch.setenv("PIXI_ENVIRONMENT_NAME", "default")
    monkeypatch.setenv("CONDA_PREFIX", str(prefix))
    monkeypatch.setattr(
        "hepflow.backends._dask._worker_env.pack_relocatable_prefix",
        lambda prefix, output: output.write_text("archive", encoding="utf-8") or output,
    )
    monkeypatch.setattr(
        "hepflow.backends._dask._worker_env.discover_editable_distributions",
        lambda prefix: [],
    )
    monkeypatch.setattr(
        "hepflow.backends._dask._worker_env.required_fasthep_imports",
        lambda prefix, editables: ["distributed", "hepflow"],
    )
    prepared = prepare_worker_environment(
        {
            "environment": {
                "type": "packed-pixi",
                "mode": "prefix",
                "environment": "default",
            }
        },
        build_paths=BuildPaths(root=tmp_path / "build"),
    )

    staged = prepare_staged_execution_files(
        prepared,
        build_paths=BuildPaths(root=tmp_path / "build"),
        staging={"mode": "shared"},
    )

    assert staged is not None
    assert staged.transfer_files == []
    assert "FASTHEP_ENV_ARCHIVE=" in staged.bootstrap_commands[1]


def test_transfer_staging_creates_compile_environment_application_manifest(
    tmp_path: Path,
) -> None:
    build_paths = BuildPaths(root=tmp_path / "build")
    build_paths.compile_dir().mkdir(parents=True)
    for name in ["plan.yaml", "normalized.yaml", "deps.yaml"]:
        build_paths.compile_file(name).write_text(name, encoding="utf-8")
    env_dir = build_paths.worker_environments_dir() / "env"
    env_dir.mkdir(parents=True)
    archive = env_dir / "environment.tar.gz"
    bootstrap = env_dir / "bootstrap.sh"
    archive.write_text("archive", encoding="utf-8")
    bootstrap.write_text("bootstrap", encoding="utf-8")
    wheelhouse = build_paths.applications_dir() / "app" / "wheelhouse"
    wheelhouse.mkdir(parents=True)
    (wheelhouse / "pkg.whl").write_text("wheel", encoding="utf-8")
    manifest = env_dir / "manifest.json"
    manifest.write_text("{}", encoding="utf-8")
    prepared = PreparedWorkerEnvironment(
        python="./worker-env/bin/python",
        bootstrap_commands=[],
        transfer_files=[],
        env={},
        environment_manifest=manifest,
        environment_archive=archive,
        bootstrap_script=bootstrap,
        application_wheelhouse=wheelhouse,
    )

    staged = prepare_staged_execution_files(
        prepared,
        build_paths=build_paths,
        staging={"mode": "transfer"},
    )

    assert staged is not None
    names = {path.name for path in staged.transfer_files}
    assert names == {"compile.tar.gz", "environment.tar.gz", "bootstrap.sh", "application"}
    assert staged.bootstrap_commands[:3] == [
        "set -e",
        "mkdir -p compile",
        "tar -xzf compile.tar.gz -C compile",
    ]
    assert all(not Path(item).is_absolute() for item in staged.bootstrap_commands)
    payload = json.loads(staged.manifest.read_text(encoding="utf-8"))
    assert payload["compile"]["sha256"]
    assert payload["environment"]["sha256"]
    assert payload["application"]["wheelhouse"]["sha256"]


def test_verify_transfer_files_fails_before_submit(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="Staged transfer input does not exist"):
        verify_transfer_files([tmp_path / "missing.tar.gz"])


def test_manual_script_uses_shared_worker_env_helpers() -> None:
    manual = _load_manual_pooled_htcondor()

    assert not hasattr(manual, "build_packed_env_job_kwargs")
    assert manual.pack_pixi_environment.__module__ == "hepflow.backends._dask._worker_env"
    assert manual.x509_proxy_from_environment.__module__ == (
        "hepflow.backends._dask._worker_env"
    )


def _pool_config() -> dict[str, dict[str, Any]]:
    return {
        "default": {
            "workers": 2,
            "job_kwargs": {
                "cores": 1,
                "memory": "4GB",
                "disk": "10GB",
                "resources": {"resource.default": 1},
            },
        },
        "high_memory": {
            "workers": 1,
            "job_kwargs": {
                "cores": 2,
                "memory": "32GB",
                "disk": "20GB",
                "resources": {"resource.high_memory": 1},
            },
        },
    }


def _load_manual_pooled_htcondor() -> Any:
    path = (
        Path(__file__).resolve().parents[3]
        / "scripts"
        / "manual"
        / "test_pooled_htcondor.py"
    )
    spec = importlib.util.spec_from_file_location("manual_pooled_htcondor", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load manual script from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
