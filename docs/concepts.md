# Concepts

`fasthep-flow` does not implement any processing itself, but rather delegates
between user workflow description (the
[YAML configuration file](./configuration/index.md)), the workflow
representation (e.g. **Python Callables**), the **workflow management** and the
**Execution Environment**.

<!-- Unless excplicitely stated, every workflow has to start with a **Data task**,
has one or more **Processing tasks**, and end with an **Output task**. -->

A simplified few of this approach is shown below:

```{mermaid}
flowchart LR
    YAML
    subgraph Config interpretation
    FAST-HEP["fasthep-flow"]
    Your["custom"]
    end
    subgraph Internal Workflow Representation
    Repr[fasthep_flow.Workflow]
    end
    subgraph External Workflow management
    Hamilton
    Pipefunc
    Snakemake
    end
    subgraph Execution Environment
    local
    Dask
    Other
    end
    YAML --> FAST-HEP
    YAML --> Your
    FAST-HEP --> Repr
    Your --> Repr
    Repr --> Hamilton
    Repr --> Pipefunc
    Repr --> Snakemake
    Hamilton --> local
    Hamilton --> Dask
    Hamilton --> Other
    Pipefunc --> local
    Pipefunc --> Dask
    Pipefunc --> Other
    Snakemake --> local
    Snakemake --> Other

```

## Tasks

Tasks are the basic building blocks of a workflow. They are the
individual steps that make up the workflow. Each task is a callable that takes
data as input and returns data as output. The tasks can be executed in order (default),
or in parallel by defining the `needs` of a task.
The tasks are defined in the YAML configuration file, and any parameters are passed to the final python objects.
Typically, the first task is a data task, the last task is an output task, but that decision is up to the user.

## Task types

While tasks can be almost anything, `fasthep-flow` provides a few handles for specific types of tasks.
These are:

- **Data task**: a task that reads data from a file, or generates data.
- **Processing task**: a task that takes data as input and returns data as output.
- **Filter task**: a task that takes data as input and returns a subset of the data as output.
- **Output task**: a task that takes data as input and returns data as output.

**Data task**: For data tasks, `fasthep-flow` provides a multiplexing interpreter. This allows you to define how the data is split up across the following tasks.

```{mermaid}
flowchart TD
    subgraph Data task
    A[two files, split by file]
    end
    subgraph Processing task
    B[process file 1]
    C[process file 2]
    end
    subgraph Output task
    D[merge output]
    end
    A --> B
    A --> C
    B --> D
    C --> D

```

**Processing task**: Processing tasks are the default task type and have no special handling.

**Filter task**: Filter tasks are a special type of processing task that takes data as input and returns a subset of the data as output. Filter tasks can be operated in `filter` or `tagging` mode, and are executed in the order they are defined in the YAML file. To keep track of rejected and accepted events, `fasthep-flow` provides a few special plugins for handling this selection data. The most useful one, enabled by default, is the `SelectionTracker` plugin - its output can be used to produce selection tables:

| Selection     | Events before | Events after | Efficiency |
| ------------- | ------------- | ------------ | ---------- |
| MET > 200 GeV | 1000          | 200          | 20 %       |
| njet > 4      | 200           | 100          | 50 %       |

or in `individual` mode:

| Selection     | Events before | Events after | Efficiency |
| ------------- | ------------- | ------------ | ---------- |
| MET > 200 GeV | 1000          | 200          | 20 %       |
| njet > 4      | 1000          | 300          | 30 %       |

All the data is available and can be saved to disk and used for further analysis.

**Output task**: Output tasks are typically the last task in a workflow. By default, the output task has access to all the data from the previous tasks AND plugins (note: this allows for "meta-plugins" for output tasks). This can be changed by specifying the `needs` of the task. In addition, `fasthep-flow` provides a few special plugins for handling metadata and provenance. These are not required, but can be useful for tracking the workflow and the data.

## Plugin system

`fasthep-flow` has a plugin system that allows you to extend the functionality of the task system.
Plugins are defined in the YAML configuration file either as global plugins or task-specific, and can process the task and all its data before and after the task is executed.
This makes plugins a powerful tool to collect metadata, provenance and other information about the workflow.
Plugins can also be used to modify tasks given the environment, e.g. switching between CPU and GPU processing, or between local and remote storage.
Finally, plugins can be also used for caching the output of a task, or for monitoring the progress of the workflow.

**Provenance plugin**: The provenance plugin is a global plugin, meaning it should be defined and configured outside the workflow.
It is used to collect information about the workflow, such
as the software versions used, the environment the task is run in, the input data, and the output data. The
provenance task is executed only once, and is the last task in a workflow. The
output of the provenance task is saved to disk.

**Caching plugin**: As the name suggests, the caching plugin is used to cache the output of a task.
Before a task is executed, the caching plugin checks if the output of the task
is already available. If it is, the task is skipped and the cached output is used
instead. If the output is not available, the task is executed and the output is
cached. The caching plugin can be used to speed up the workflow, especially if
the tasks are expensive to execute. The caching plugin can be configured to
use different caching strategies, such as caching the output to disk, or
caching the output to a remote storage. The one provided by default is limited to file systems defined in `fsspec`.

**Monitoring plugin**: can be used to monitor the progress of the workflow.
The monitoring plugin is constructed from components that look at specific parts of a task.
These can be things like the number of events processed, the time taken to process, memory usage, etc. - the details are up to the user.
The plugin can either store the information locally or send it in
intervals to a specified endpoint (e.g. prometheus)

## Anatomy of an analysis workflow

In the most general terms, an analysis workflow consists of the following parts:

- **Data task**: the data to be analysed
- **Processing tasks**: the analysis steps
- **Output task**: the output of the analysis

The following diagram shows the different parts of an analysis workflow,
including the data flow between them:

```{mermaid}
flowchart TD

```

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
