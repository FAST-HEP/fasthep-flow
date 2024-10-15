# Environments

By default, all of the tasks in a workflow are executed in the same environment.
However, in some scenarios, it may be necessary to execute different tasks in
different environments. For example, you may want to run a task that uses a
framework that needs its own specific environment, such as TensorFlow, PyTorch,
or ROOT. Or, you may want to run a task on a different machine that has more
memory or more CPUs.

To support this, `fasthep-flow` allows you to specify the environment for each
task in a workflow. The environment is specified using the `environment` key in
the task definition. The `environment` has many settings, which we will discuss
here.

Let's start with a simple example:

```yaml
tasks:
  - name: runROOTMacro
    type: "fasthep_flow.operators.BashOperator"
    kwargs:
      bash_command: root -bxq <path to ROOT macro>
    environment:
      image: docker.io/rootproject/root:6.28.04-ubuntu22.04
      variables: <path to .env>
      flow: prefect::SequentialTaskRunner
  - name: runStatsCode
    type: "fasthep_flow.operators.BashOperator"
    kwargs:
      bash_command: ./localStatsCode.sh
    environment:
      image: gitlab.cern/<namespace>/<project>/<my image>
      variables:
        STATS_METHOD: CLS
      resources:
        memory: 8Gi
        process_on: gpu_if_available
```

There is a lot to unpack here, so let's start bit by bit. The first task uses
`environment::image`, `environment::variables`, and `environment::flow`. The
`image` is a container image, here Docker, while `variables` defines the
environmental variables. The values for `variables` can either be a path to an
`.env` file or a dictionary of key-value pairs (see 2nd example).

```{note}
A `.env` file is a file specifying variables in the format `VARIABLE=VALUE` - one per line. For example, `STATS_METHOD=CLs` is a valid `.env` file.
```

The `flow`defines the orchestration of the workflow to use for this task. The
default orchestration is defined in the global settings, usually set to
`prefect::DaskTaskRunner`. In this case, we are using the
`prefect::SequentialTaskRunner` to run the task locally.

````{note}
The `flow` setting has to use the same prefix as the global setting and has to match a defined orchestration.```


In the second task, we use `environment::image`, `environment::variables` and
`environment::resources`. We've already discussed the firs two, but we use the
dictionary variable definition here, instead of the `.env` file. The new
additin, `resources`, is the same as for the global setting. Here you can define
memory, CPU, and GPU resources for the task. These will be passed to the
orchestration layer.

The full set of options for `environment` is:

```yaml
environment:
  variables: <path to .env> | { <key>: <value>, ... }
  image: <image name>
  workflow:
    transform: prefect
    kwargs:
      runner: SequentialTaskRunner | DaskTaskRunner | any other supported value
  resources: # see details in global settings
  extra_data: TBC
````
