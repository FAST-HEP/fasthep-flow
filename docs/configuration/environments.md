# Environments

By default, all of the stages in a workflow are executed in the same
environment. However, in some scenarios, it may be necessary to execute
different stages in different environments. For example, you may want to run a
stage that uses a framework that needs its own specific environment, such as
TensorFlow, PyTorch, or ROOT. Or, you may want to run a stage on a different
machine that has more memory or more CPUs.

To support this, `fasthep-flow` allows you to specify the environment for each
stage in a workflow. The environment is specified using the `environment` key in
the stage definition. The `environment` has many settings, which we will discuss
here.

Let's start with a simple example:

```yaml
stages:
  - name: runROOTMacro
    type: "fasthep_flow.operators.BashOperator"
    kwargs:
      bash_command: root -bxq <path to ROOT macro>
    environment:
      image: docker.io/rootproject/root:6.28.04-ubuntu22.04
      variables: <path to .env>
      executor: LocalExecutor
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

There is a lot to unpack here, so let's start bit by bit. The first stage uses
`environment::image`, `environment::variables`, and `environment::executor`. The
`image` is a container image, here Docker, while `variables` defines the
environmental variables. The values for `variables` can either be a path to an
`.env` file or a dictionary of key-value pairs (see 2nd example). The `executor`
defines the executor to use for this stage. The default is `DaskExecutor`, but
here we are using `LocalExecutor` to run the stage locally.

```{note}
A `.env` file is a file specifying variables in the format `VARIABLE=VALUE` - one per line. For example, `STATS_METHOD=CLs` is a valid `.env` file.
```

In the second stage, we use `environment::image`, `environment::variables` and
`environment::resources`. We've already discussed the firs two, but we use the
dictionary variable definition here, instead of the `.env` file. The new
additin, `resources`, is the same as for the global setting. Here you can define
memory, CPU, and GPU resources for the stage. These will be passed to the
executor.

The full set of options for `environment` is:

```yaml
environment:
  variables: <path to .env> | { <key>: <value>, ... }
  image: <image name>
  executor: LocalExecutor | DaskExecutor | any other supported executor
  resources: # see details in global settings
  extra_data: TBC
```
