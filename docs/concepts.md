# Concepts

`fasthep-flow` does not implement any processing itself, but rather delegates
between user workflow description (the
[YAML configuration file](./configuration.md)), the workflow stages (e.g.
**Python Callables**), the **workflow DAG** and the **Executor** engine. Unless
excplicitely stated, every workflow has to start with a **Data Stage**, has one
or more **Processing stages**, and end with an **Output stage**.

## Stages

**Data Stage**: The data stage is any callable that returns data. This can be a
function that reads data from a file, or a function that generates data. The
data stage is the first stage in a workflow, and is executed only once. The
output of the data stage is passed to the first processing stage.

**Processing Stage**: A processing stage is any callable that takes data as
input and returns data as output. The output of a processing stage is passed to
the next processing stage. The processing stages are executed in order, and can
be parallelised. In `fasthep-flow` these stages are represented by
**Operators**. Details on how this works and what is required to write one, can
be found in [Operators](./operators.md).

**Output Stage**: The output stage is any callable that takes data as input and
returns data as output. The output of the last processing stage is passed to the
output stage. The output stage is executed only once, and is the last stage in a
workflow. The output of the output stage is saved to disk.

### Exceptional stages

Of course, not all workflows are as simple as the above. In some cases, you may
want to checkpoint the process, write out intermediate results, or do some other
special processing. For this, `fasthep-flow` has the following special stages:

**Provenance Stage**: The provenance stage is a special stagen that typically
runs outside the workflow. It is used to collect information about the workflow,
such as the software versions used, the input data, and the output data. The
provenance stage is executed only once, and is the last stage in a workflow. The
output of the provenance stage is saved to disk.

**Caching stage**: The caching stage is a special stage that can be used to
cache the output of a processing stage. The caching stage can be added to any
processing stage, and will save the output of the processing stage to disk or
remote storage.

**Monitoring stage**: The monitoring stage is a special stage that can be used
to monitor the progress of the workflow. The monitoring stage can be added to
any processing stage, and can either store the information locally or send it in
intervals to a specified endpoint.

## Anatomy of an analysis workflow

In the most general terms, an analysis workflow consists of the following parts:

- **Data stage**: the data to be analysed
- **Processing stages**: the analysis steps
- **Output stage**: the output of the analysis

The following diagram shows the different parts of an analysis workflow,
including the data flow between them:

```{figure} /images/analysis_workflow.png
:align: center
:class: with-border

Analysis workflow: starting from input data, the workflow is split into stages, which are then executed in order. Tasks in each stage can be parallelised, and the output of each stage is passed to the next. In the end, the output of the last stage is saved to disk.
```

In `fasthep-flow` we attempt to map each part of an analysis workflow onto a
**stage** in the YAML file. By default each consecutive stage will be executed
in order, but this can be changed by specifying dependencies between stages.
Currently only one parallelisation strategy is supported, `split-by-file`, but
more will be added in the future. `fasthep-flow` will create **Tasks** for each
stage based on the parallelisation strategy. E.g. if the parallelisation
strategy is `split-by-file`, then each file will be processed in a separate
task.

The following diagram shows the different stages:

```{figure} /images/workflow_stages.png
---
class: with-border
---
Stages of a workflow: a workflow starts with a data stage, has one or more processing stages, and ends with an output stage.
```

Of course, this is a very simplified picture, a more realistic example is shown
in [CMS Public tutorial example](./examples/cms_pub_example.md).
