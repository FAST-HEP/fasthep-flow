# Execution Infrastructure

Execution infrastructure describes **where** and **how** a workflow runs.

FAST-HEP separates analysis logic from execution details. The same workflow can run:

* locally on a laptop
* on a university HTCondor cluster
* on a Slurm supercomputer
* on heterogeneous CPU/GPU resources
* or on future distributed platforms

The analysis itself does not need to change.

## Overview

Execution infrastructure consists of four layers:

```text
Workflow
    ↓
Resources
    ↓
Pools
    ↓
Backend + Strategy
```

### Workflow

The workflow describes the scientific analysis:

```yaml
analysis:
  stages:
    - id: SelectMuons
      op: hep.select
      ...
```

This layer should contain as little infrastructure-specific logic as possible.

### Resources

Resources describe the capabilities required by a stage.

Examples:

```yaml
execution:
  require: gpu
```

```yaml
execution:
  require: high_memory
```

Resources are labels rather than concrete batch-system requests.

This allows workflows to remain portable across sites.

### Pools

Pools describe groups of workers with specific capabilities.

Example:

```yaml
execution:
  pools:
    default:
      workers: 20
      resources:
        cpus: 1
        memory: 4GB

    high_memory:
      workers: 2
      resources:
        cpus: 4
        memory: 64GB

    gpu:
      workers: 1
      resources:
        gpus: 1
        memory: 16GB
```

A stage requesting:

```yaml
execution:
  require: high_memory
```

will be routed to the `high_memory` pool.

A stage requesting:

```yaml
execution:
  require: gpu
```

will be routed to the `gpu` pool.

## Backends and Strategies

FAST-HEP separates execution into two concepts:

### Backend

The backend is responsible for executing tasks.

Examples:

```yaml
execution:
  backend: local
```

```yaml
execution:
  backend: dask
```

### Strategy

The strategy determines how workers are created.

Examples:

```yaml
execution:
  strategy: htcondor
```

```yaml
execution:
  strategy: slurm
```

A backend may support multiple strategies.

For example:

```yaml
execution:
  backend: dask
  strategy: htcondor
```

and

```yaml
execution:
  backend: dask
  strategy: slurm
```

both use Dask, but deploy workers differently.

## Heterogeneous Worker Pools

Traditional analysis workflows often split processing into separate jobs:

1. preprocess data on high-memory nodes
2. run analysis on standard nodes
3. run inference on GPU nodes

FAST-HEP allows these stages to exist within a single workflow.

Example:

```yaml
analysis:
  stages:

    - id: BuildIndex
      op: custom.build_index
      execution:
        require: high_memory

    - id: TrainModel
      op: custom.train
      execution:
        require: gpu

    - id: ProducePlots
      op: hep.hist
```

The execution system routes each stage to appropriate workers automatically.

## Worker Environments

Workers need access to:

* Python packages,
* FAST-HEP components,
* experiment software,
* analysis code.

FAST-HEP supports packaging worker environments independently from workflow definitions.

Examples include:

* shared filesystems
* packed Pixi environments

The workflow remains unchanged.

## Credentials

Some workflows require access to protected data and authenticate via X509 proxies or Scitokens.
Credentials are treated as execution infrastructure rather than analysis logic.
This allows workflows to remain portable while execution systems handle credential transfer and setup.

## Execution Modifiers

Execution modifiers adapt how a stage executes.

Examples:

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

Modifiers are applied at runtime and may:

* preload data onto GPUs
* wrap operations
* collect diagnostics
* perform profiling

Modifiers are attached to stages and travel through the execution plan as metadata.
