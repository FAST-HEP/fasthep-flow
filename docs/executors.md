# Executors - run your workflows

`fasthep-flow` is currently built on top of
[prefect](https://docs.prefect.io/latest/), which means it supports all of the
executors, or in this case "Task Runners", that prefect supports. The default
executor is the [SequentialTaskRunner](#sequentialtaskrunner), which runs all
tasks in sequence on the local machine. A full list of executors can be found in
the
[prefect documentation](https://docs.prefect.io/latest/concepts/task-runners/).

Since prefect is not widely used in High Energy Particle Physics, let's go over
the executors that are most relevant to us.

## SequentialTaskRunner

The `SequentialTaskRunner` (see
[prefect docs](https://docs.prefect.io/latest/api-ref/prefect/task-runners/#prefect.task_runners.SequentialTaskRunner))
runs each task in a separate process on the local machine. This is the default
executor for `fasthep-flow`.

## DaskTaskRunner

The `DaskTaskRunner` (see
[prefect docs](https://prefecthq.github.io/prefect-dask/)) runs each task in a
separate process on a Dask cluster. A Dask cluster can be run on a local machine
or as a distributed cluster using a batch system (e.g. HTCondor, LSF, PBS, SGE,
SLURM) or other distributed systems such as LHCb's DIRAC. This is the
recommended executor for running `fasthep-flow` workflows on distributed
resources.

## Custom executors

Documentation on how to create custom executors can be found in the
[developer's corner](devcon/executors.md).
