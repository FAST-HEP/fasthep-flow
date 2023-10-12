# Executors - run your workflows

Since `fasthep-flow` is built on top of Apache Airflow, it supports all of the
executors that Airflow supports. The default executor is the
[LocalExecutor](#localexecutor), which runs all tasks in parallel on the local
machine. A full list of executors can be found in the
[Airflow documentation](https://airflow.apache.org/docs/apache-airflow/stable/executor/index.html).

Since Apache Airflow is not widely used in High Energy Particle Physics, let's
go over the executors that are most relevant to us.

## LocalExecutor

The `LocalExecutor` (see
[Airflow docs](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/local.html))
runs each task in a separate process on the local machine. This is the default
executor for `fasthep-flow`.

## DaskExecutor

The `DaskExecutor` (see
[Airflow docs](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/dask.html))
runs each task in a separate process on a Dask cluster. A Dask cluster can be
run on a local machine or as a distributed cluster using a batch system (e.g.
HTCondor, LSF, PBS, SGE, SLURM) or other distributed systems such as LHCb's
DIRAC. This is the recommended executor for running `fasthep-flow` workflows on
distributed resources.
