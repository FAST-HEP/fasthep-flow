# FAST-HEP-flow

## Introduction

`fasthep-flow` is a package for converting YAML files into an Apache Airflow
DAG. It is designed to be used with the [fast-hep](https://fast-hep.github.io/)
package ecosystem, but can be used independently.

The goal of this package is to define a workflow, e.g. a HEP analysis, in a YAML
file, and then convert that YAML file into an Apache Airflow DAG. This DAG can
then be run on a local machine, or on a cluster using
[CERN's HTCondor](https://batchdocs.web.cern.ch/local/submit.html) (via Dask) or
[Google Cloud Composer](https://cloud.google.com/composer).

In `fasthep-flow`'s YAML files draws inspiration from Continuous Integration
(CI) pipelines and Ansible Playbooks to define the workflow, where each step is
a task that can be run in parallel. `fasthep-flow` will check the parameters of
each task, and then generate the DAG. The DAG will have a task for each step,
and the dependencies between the tasks will be defined by the `needs` key in the
YAML file. More on this under [Configuration](./configuration.md).

```{tip}

- `fasthep-flow` is still in early development, and the API is not yet stable.
  Please report any issues you find on the
  [GitHub issue tracker](https://github.com/FAST-HEP/fasthep-flow/issues).
- Curious how this looks in action? Have a quick look at the
  [CMS Public Tutorial example](./examples/cms_pub_example.md).

```

## Documentation

```{toctree}

changelog.md
installation.md
concepts.md
configuration.md
operators.md
examples/index.md
developers_corner.md
```
