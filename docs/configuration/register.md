# Register stages

`fasthep-flow` provides a set of useful stages for common tasks and, if
`fasthep-stages` is installed, a set of stages for common HEP tasks. However,
you may want to write your own stages. This is easy to do, and we encourage you
to share your stages with the community via `fasthep-stages-contrib`, which are
also automatically loaded into the scope. Sometimes you may want to use a stage
that cannot be publicly shared, or you may want to use a stage that is not yet
available in `fasthep-stages-contrib`. In this case, you can register your own
stage with `fasthep-flow`.

## Registering a stage

To register a stage, you need to point `fasthep-flow` to your callable. This can
be a function or a class implementing the `__call__` method.

```yaml
register:
  my_namespace::my_stage: my_module.my_stage
```

Here, `my_namespace` is the namespace for the stage, `my_stage` is the name of
the stage, and `my_module.my_stage` is the path to the callable. Ideally, the
namespace will match your module name, but this is not required. Now, you can
use `my_namespace::my_stage` as a type for a stage, e.g.

```yaml
stages:
  - name: my_stage
    type: my_namespace::my_stage
```

```{note}
There are protected namespaces, which you cannot use like this. These are `airflow` and anything starting with `fasthep`.
```

## Registering a stage that can run on GPU

If your stage can run on GPU, you can register it with `fasthep-flow` so that it
can be used with the `process_on: gpu` option. To do this, you need to register
the GPU version of the stage with `fasthep-flow`. This is done by adding a `gpu`
key to the stage definition:

```yaml
register:
  my_namespace::my_stage:
    cpu: my_module.my_stage
    gpu: my_module.my_stage_gpu
```

If you do not have a `cpu` version of the stage, you can omit the `cpu` key. The
stage will fail if `process_on: cpu` is used and there is no `cpu` version of
the stage or during fall-back from `gpu_if_available`.
