

```text
execution infrastructure
```

The two headline capabilities naturally fit together:

1. **Distributed execution**

   * Dask local
   * Dask + HTCondor
   * Dask + Slurm
   * backend/strategy/resource config

2. **Accelerated execution**

   * GPU resource requests
   * GPU preload hooks
   * optional GPU wrappers for stages
   * user-defined execution wrappers

The key design question is probably:

```text
How does author intent become backend/resource/runtime configuration?
```

Implementation note: task 1 is adding Flow-language normalization and plan metadata
for `execution`. Scheduler behavior, resource-aware placement, GPU execution, and
modifier execution are intentionally still future work.

Task 2 makes `backend: dask` with `strategy: local` consume normalized local
Dask config such as `workers`, `threads_per_worker`, and `processes`.

Task 3 adds an experimental Dask HTCondor provisioning prototype:

```yaml
execution:
  backend: dask
  strategy: htcondor
```

This requires `dask-jobqueue`; the prototype maps global/default resources and
worker counts into `HTCondorCluster`, but does not yet route stage-level
resources or modifiers.

Task 4 adds an experimental Dask Slurm provisioning prototype:

```yaml
execution:
  backend: dask
  strategy: slurm
```

This also requires `dask-jobqueue`; the prototype maps global/default resources,
worker counts, queue, account, walltime, and job extra directives into
`SLURMCluster`, without GPU or stage-level resource routing yet.

Worker pools separate what a worker needs from how many workers to provision:

```yaml
execution:
  pools:
    default:
      resources: default
      workers: 100
    gpu:
      resources: gpu
      workers: 2
```

Resources describe worker requirements, pools describe the number of workers of
each type, and stage execution metadata decides which resource class a task
requires or prefers.

Task 8 generalizes Dask pool labels beyond GPUs:

```yaml
execution:
  resources:
    high_memory:
      cpus: 8
      memory: 128GB
    default:
      cpus: 1
      memory: 4GB
  pools:
    preprocess:
      resources: high_memory
      workers: 2
    default:
      resources: default
      workers: 50
```

Resource classes describe worker capabilities, pools describe how many workers
of each type to create, and stage execution routes tasks to the matching
resource class. Dask workers advertise canonical labels such as
`resource.high_memory=1`; a stage with `execution.require: high_memory` requests
the same label. GPU is just one resource class in the author language. GPU
classes additionally advertise the Dask `GPU` alias when `gpus` is present, and
HTCondor/Slurm GPU pools map that requirement to scheduler-specific GPU
requests. The current prototype supports single-pool provisioning per strategy;
heterogeneous multi-pool provisioning is kept behind explicit strategy errors
until shared-scheduler cluster provisioning is implemented.

I’d avoid putting raw scheduler details everywhere in `author.yaml`. Instead, use:

```yaml
execution:
  backend: dask
  strategy: htcondor
  resources:
    default:
      cpus: 1
      memory: 4GB
      disk: 10GB
    gpu:
      gpus: 1
      memory: 16GB
```

then stage-level overrides:

```yaml
analysis:
  stages:
    - id: HeavyInference
      op: hep.inference
      execution:
        prefer: gpu
        fallback: default
        modifiers:
          - gpu.preload
          - cuda.jit
```

Modifiers describe execution-time adaptations applied to a stage. They are
normalised, propagated into the plan, and resolved at runtime through the
`execution_modifiers` registry section. Concrete modifier implementations are
still experimental and package-owned. The shorthand above is normalised to the
future-compatible form:

```yaml
execution:
  modifiers:
    - name: gpu.preload
      params: {}
    - name: cuda.jit
      params: {}
```

Expanded modifier entries may carry parameters:

```yaml
execution:
  modifiers:
    - name: gpu.preload
      params:
        fields:
          - Jet_Pt
          - Jet_Eta
```

Execution modifiers reuse the same runtime lifecycle machinery as execution
hooks. Hooks are usually observational; modifiers may intentionally mutate
runtime state or adapt node execution. Modifier implementations may provide any
subset of the node lifecycle methods:

```text
before_node   -> inspect or adapt node inputs/context before execution
around_node   -> context-manage node execution
after_node    -> inspect or adapt outputs/context after execution
on_node_error -> react to node failures
```

Global execution hooks run outside node modifiers, so timing and diagnostic hooks
include modifier overhead. For modifiers listed as `A, B`, `before_node` and
`around_node` enter in author order, while `after_node` runs in reverse order.

So the conceptual stack becomes:

```text
backend  = execution engine
strategy = how to map plan → backend
resources = what tasks request
hooks    = lifecycle/data movement
wrappers = how stages are adapted
```

I’d probably do the next release in this order:

1. Design doc for `execution:` language.
2. Implement backend/strategy/resource normalisation.
3. Dask local cleanup.
4. Dask HTCondor strategy.
5. Dask Slurm strategy.
6. GPU resource metadata.
7. GPU preload hook.
8. Stage wrapper mechanism.
9. Tutorials:

   * run locally
   * run on Dask
   * run on HTCondor/Slurm
   * GPU-enabled stage

Docs/tutorials can grow alongside this, but I’d make the next release’s technical anchor:

```text
same analysis, different execution infrastructure
```

That is very compelling after the alpha release because the language and products are now in place.
