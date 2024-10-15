# Register tasks

`fasthep-flow` provides a set of useful tasks for common tasks and, if
`fasthep-tasks` is installed, a set of tasks for common HEP tasks. However, you
may want to write your own tasks. This is easy to do, and we encourage you to
share your tasks with the community via `fasthep-tasks-contrib`, which are also
automatically loaded into the scope. Sometimes you may want to use a task that
cannot be publicly shared, or you may want to use a task that is not yet
available in `fasthep-task-contrib`. In this case, you can register your own
task with `fasthep-flow`.

## Registering a task

To register a task, you need to point `fasthep-flow` to your callable. This can
be a function or a class implementing the `__call__` method.

```yaml
register:
  my_namespace::my_task: my_module.my_task
```

Here, `my_namespace` is the namespace for the task, `my_task` is the name of the
task, and `my_module.my_task` is the path to the callable. Ideally, the
namespace will match your module name, but this is not required. Now, you can
use `my_namespace::my_task` as a type for a task, e.g.

```yaml
tasks:
  - name: my_task
    type: my_namespace::my_task
```

```{note}
There are protected namespaces, which you cannot use like this. These are anything starting with `fasthep`.
```

## Registering a task that can run on GPU

If your task can run on GPU, you can register it with `fasthep-flow` so that it
can be used with the `process_on: gpu` option. To do this, you need to register
the GPU version of the task with `fasthep-flow`. This is done by adding a `gpu`
key to the task definition:

```yaml
register:
  my_namespace::my_task:
    cpu: my_module.my_task
    gpu: my_module.my_task_gpu
```

If you do not have a `cpu` version of the task, you can omit the `cpu` key. The
task will fail if `process_on: cpu` is used and there is no `cpu` version of the
task or during fall-back from `gpu_if_available`.
