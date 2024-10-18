# Global Settings

In addition to the tasks, the YAML file can also contain global settings. These
settings apply to all tasks, unless overridden by the task.

```yaml
global:
  resources:
    memory: 4Gi
    process_on: cpu | gpu | gpu_if_available
  histogram:
    prefix: h_
    folder_rule: from_name | fixed | None
    folder: None
  flow: dask::local
  output:
    directory: /path/to/output/dir
  variables: <path to .env> | { <key>: <value>, ... }
```

## Resources

The `resources` key defines the resources to use for the workflow. How these are
interpreted depends on the flow and tasks used.

- `memory`: the amount of memory to use for the workflow. This is passed to the
  executor.
- `process_on`: the type of resource to use for the workflow. This is passed to
  the tasks.

For `process_on`, the following values are supported:

- `cpu`: run on a CPU. This is the default.
- `gpu`: run on a GPU. This will fail if no GPU is available or the task cannot
  run on GPU.
- `gpu_if_available`: run on a GPU if available, otherwise run on a CPU.

tasks that can support GPUs will need to register their CPU and GPU versions
with `fasthep-flow` (see [here](register.md)). If a task is not registered with
a GPU version, then `gpu_if_available` will run the CPU version.
