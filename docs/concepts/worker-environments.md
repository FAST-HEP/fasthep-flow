# Worker Environments

Worker environments describe the software available to distributed workers.

When running locally, FAST-HEP executes directly within the current Python environment.

When running on HTCondor or Slurm, workers may execute on different machines and need access to:

* Python,
* FAST-HEP packages,
* analysis code,
* required dependencies.

## Why Worker Environments Exist

Consider a workflow running on HTCondor:

```yaml
execution:
  backend: dask
  strategy: htcondor
```

The scheduler may run on one machine while workers run elsewhere.

Those workers cannot assume they have access to the same filesystem or Python environment as the submit host.

A worker environment ensures that workers can execute the workflow consistently.

## Packed Pixi Environments

FAST-HEP currently supports packed Pixi environments.

The execution system can:

1. package a Pixi environment,
2. transfer it to worker nodes,
3. extract it on the worker,
4. start Dask workers using the packaged Python environment.

Conceptually:

```text
Submit Host
    ↓
Pack Pixi Environment
    ↓
Transfer Archive
    ↓
Extract on Worker
    ↓
Start Worker
```

This allows distributed execution without requiring FAST-HEP to be installed on every worker node.

## Worker Startup

When a worker starts, FAST-HEP:

1. transfers the environment archive,
2. extracts it,
3. configures any required credentials,
4. launches the Dask worker process.

The worker then behaves like any other Dask worker and can execute tasks assigned by the scheduler.

## Shared Filesystems

Some clusters provide a shared filesystem.

In these environments:

```text
Submit Host
    ↕
Shared Filesystem
    ↕
Worker Nodes
```

workers may access the same files directly.

FAST-HEP does not require a shared filesystem, but can make use of one when available.

## Worker Logs

Distributed workers produce logs and diagnostics.

Typical outputs include:

```text
debug/
└── distributed/
    ├── logs/
    ├── out/
    └── err/
```

These files are useful when debugging:

* worker startup failures,
* environment issues,
* credential problems,
* scheduler connectivity issues.

## Worker Resources

Worker environments are independent from worker resources.

A worker environment describes:

```text
What software is available.
```

Resources describe:

```text
What hardware is available.
```

For example:

```yaml
execution:
  pools:

    gpu:
      workers: 1

      resources:
        gpus: 1
```

defines GPU-capable workers.

The worker environment determines which software those workers run.

## Execution Modifiers

Execution modifiers run inside the worker environment.

Examples include:

```yaml
execution:
  modifiers:
    - gpu.preload
```

```yaml
execution:
  modifiers:
    - cuda.jit
```

These modifiers execute on the worker after the environment has been prepared.
