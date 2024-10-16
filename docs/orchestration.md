# Workflow orchestration

`fasthep-flow` is built to convert a workflow definition into an executable set
of tasks. Internally, a workflow is represented as a directed acyclic graph
(DAG) of tasks and can be executed locally. However, to scale to larger
workflows and to run on distributed resources, `fasthep-flow` allows for
conversions to other tools/frameworks to optimize the execution.

## Hamilton

After the internal workflow creation, the workflow is converted into a
[Hamilton DAG](https://hamilton.dagworks.io/en/latest/). Hamilton is a
general-purpose framework to write dataflows using regular Python functions.

### Work in progress

Hamilton allows for scaling execution via Dask, PySpark and Ray.
