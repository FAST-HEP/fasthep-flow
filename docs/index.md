# FAST-HEP-flow

## Introduction

`fasthep-flow` is a package for describing data analysis workflows in YAML and
converting them into serializable compute graphs that can be evaluated by
software like [Dask](https://www.dask.org/), as
[Interaction Combinator](https://www.semanticscholar.org/paper/Interaction-Combinators-Lafont/6cfe09aa6e5da6ce98077b7a048cb1badd78cc76)
or step-wise for with workflow managers like
[Snakemake](https://snakemake.readthedocs.io/en/stable/). `fasthep-flow` is
designed to be used with the [FAST-HEP](https://fast-hep.github.io/) package
ecosystem, but can be used independently.

The primary use-case of this package is to define a data processing workflow,
e.g. a High-Energy-Physics (HEP) analysis, in a YAML file, and then convert that
YAML file into a compute graphs that can be converted in meaningful node sets to
Direct Acyclic Graphs (DAGs) that can be processed by external (to fasthep-flow)
executors. These executors can then run on a local machine, or on a cluster
using [CERN's HTCondor](https://batchdocs.web.cern.ch/local/submit.html) (via
Dask) or [Google Cloud Composer](https://cloud.google.com/composer).

In `fasthep-flow`'s YAML files draw inspiration from Continuous Integration (CI)
pipelines and Ansible Playbooks to define the workflow, where each independent
task that can be run in parallel. `fasthep-flow` will check the parameters of
each task, and then generate the compute graph. The compute graph consists of
nodes that describe input/output data and the compute task and edges for the
dependencies between the tasks.

```{tip}

- `fasthep-flow` is still in early development, and the API is not yet stable.
  Please report any issues you find on the
  [GitHub issue tracker](https://github.com/FAST-HEP/fasthep-flow/issues).
- Curious how this looks in action? Have a quick look at the
  [CMS Public Tutorial example](./examples/cms_pub_example.md).

```

## Documentation

```{toctree}
:maxdepth: 2
changelog.md
installation.md
concepts.md
configuration/index.md
filters.md
operators.md
orchestration.md
provenance.md
examples/index.md
advanced_examples/index.md
command_line_interface.md
interactive_python.md
devcon/index.md
```
