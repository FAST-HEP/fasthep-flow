# Resource Pools

Resource pools are the mechanism FAST-HEP uses to route work to appropriate workers.

They allow a single workflow to use multiple types of compute resources without splitting the analysis into separate jobs.

## Motivation

Scientific workflows often contain stages with very different requirements.

Examples include:

* lightweight filtering,
* memory-intensive preprocessing,
* machine learning inference,
* GPU-accelerated calculations.

Traditionally these stages are executed as separate workflows or submitted to different queues manually.

FAST-HEP allows them to coexist within a single workflow.

## Resources vs Pools

A useful way to think about the execution system is:

```text
Resources describe what a stage needs.

Pools describe what workers exist.
```

### Resources

Stages request resources:

```yaml
analysis:
  stages:
    - id: BuildIndex
      op: custom.build_index
      execution:
        require: high_memory

    - id: Inference
      op: custom.inference
      execution:
        require: gpu
```

The stage does not know:

* which batch system is used,
* how many workers exist,
* which machine will execute it.

It only describes what it needs.

### Pools

Pools describe available worker groups:

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
        cpus: 8
        memory: 128GB

    gpu:
      workers: 1
      resources:
        gpus: 1
        memory: 16GB
```

Each pool creates workers with specific capabilities.

## Routing

When a stage requests:

```yaml
execution:
  require: high_memory
```

FAST-HEP schedules that stage onto workers from the corresponding pool.

Likewise:

```yaml
execution:
  require: gpu
```

will only execute on workers belonging to the GPU pool.

Conceptually:

```text
Stage
    ↓
Required resource
    ↓
Matching pool
    ↓
Worker
```

## A Typical Example

Consider a workflow that:

1. builds an event index,
2. performs GPU inference,
3. creates histograms.

```yaml
analysis:
  stages:

    - id: BuildIndex
      op: custom.index
      execution:
        require: high_memory

    - id: RunInference
      op: custom.inference
      execution:
        require: gpu

    - id: MuonPt
      op: hep.hist
```

With pools:

```yaml
execution:
  pools:

    default:
      workers: 10

    high_memory:
      workers: 2
      resources:
        memory: 128GB

    gpu:
      workers: 1
      resources:
        gpus: 1
```

Execution proceeds automatically:

```text
BuildIndex
    → high_memory workers

RunInference
    → gpu workers

MuonPt
    → default workers
```

No manual job splitting is required.

## Pool Profiles

Many sites use the same resource configurations repeatedly.

Profiles can be used to avoid repetition.

For example:

```yaml
execution:
  pools:

    default:
      use: standard_worker

    gpu:
      use: gpu_worker
```

where profiles define the actual worker configuration.

This allows sites to standardise resource definitions while analyses remain portable.

## Pool-Specific Configuration

Pools may also carry backend-specific configuration.

For example:

```yaml
execution:
  pools:

    high_memory:
      workers: 2

      resources:
        memory: 128GB

      config:
        walltime: 04:00:00
```

The exact configuration options depend on the selected execution strategy.

For example:

* HTCondor

may expose different scheduling options.

## Heterogeneous Execution

The most important feature of resource pools is heterogeneous execution.

A single workflow can use:

```text
CPU workers
High-memory workers
GPU workers
```

simultaneously.

This allows workflows to express the natural structure of an analysis rather than forcing users to divide it into separate batch submissions.

## Resource Pools and Dask

When using the Dask backend, pools are translated into worker groups.

Each pool advertises resources to Dask:

```text
resource.default
resource.high_memory
resource.gpu
```

Stages are automatically annotated with the required resources.

Dask then routes tasks to matching workers.

The analysis author does not need to manage this routing manually.

