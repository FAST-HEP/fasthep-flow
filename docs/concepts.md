# Concepts

`fasthep-flow` does not implement any processing itself, but rather delegates
between user workflow description (the
[YAML configuration file](./configuration/index.md)), the workflow tasks (e.g.
**Python Callables**), the **workflow DAG** and the **Orchestration** layer.
Unless excplicitely stated, every workflow has to start with a **Data task**,
has one or more **Processing tasks**, and end with an **Output task**.

## Tasks

**Data task**: The data task is any callable that returns data. This can be a
function that reads data from a file, or a function that generates data. The
data task is the first task in a workflow, and is executed only once. The output
of the data task is passed to the first processing task.

**Processing task**: A processing task is any callable that takes data as input
and returns data as output. The output of a processing task is passed to the
next processing task. The processing tasks are executed in order, and can be
parallelised. In `fasthep-flow` these tasks are represented by **Operators**.
Details on how this works and what is required to write one, can be found in
[Operators](./operators.md).

**Output task**: The output task is any callable that takes data as input and
returns data as output. The output of the last processing task is passed to the
output task. The output task is executed only once, and is the last task in a
workflow. The output of the output task is saved to disk.

### Exceptional tasks

Of course, not all workflows are as simple as the above. In some cases, you may
want to checkpoint the process, write out intermediate results, or do some other
special processing. For this, `fasthep-flow` has the following special tasks:

**Provenance task**: The provenance task is a special task that typically runs
outside the workflow. It is used to collect information about the workflow, such
as the software versions used, the input data, and the output data. The
provenance task is executed only once, and is the last task in a workflow. The
output of the provenance task is saved to disk.

**Caching task**: The caching task is a special task that can be used to cache
the output of a processing task. The caching task can be added to any processing
task, and will save the output of the processing task to disk or remote storage.

**Monitoring task**: The monitoring task is a special task that can be used to
monitor the progress of the workflow. The monitoring task can be added to any
processing task, and can either store the information locally or send it in
intervals to a specified endpoint.

## Anatomy of an analysis workflow

In the most general terms, an analysis workflow consists of the following parts:

- **Data task**: the data to be analysed
- **Processing tasks**: the analysis steps
- **Output task**: the output of the analysis

The following diagram shows the different parts of an analysis workflow,
including the data flow between them:

```{figure} /images/analysis_workflow.png
:align: center
:class: with-border

Analysis workflow: starting from input data, the workflow is split into tasks, which are then executed in order. Tasks in each task can be parallelised, and the output of each task is passed to the next. In the end, the output of the last task is saved to disk.
```

In `fasthep-flow` we attempt to map each part of an analysis workflow onto a
**task** in the YAML file. By default each consecutive task will be executed in
order, but this can be changed by specifying dependencies between tasks.
Currently only one parallelisation strategy is supported, `split-by-file`, but
more will be added in the future. `fasthep-flow` will create **Tasks** for each
task based on the parallelisation strategy. E.g. if the parallelisation strategy
is `split-by-file`, then each file will be processed in a separate task.

The following diagram shows the different tasks:

```{figure} /images/workflow_stages.png
---
class: with-border
---
Task of a workflow: a workflow starts with a data task, has one or more processing tasks, and ends with an output task.
```

Of course, this is a very simplified picture, a more realistic example is shown
in [CMS Public tutorial example](./examples/cms_pub_example.md).
