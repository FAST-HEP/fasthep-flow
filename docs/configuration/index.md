# Configuration

`fasthep-flow` provides a way to describe a workflow in a YAML file. It does not
handle the input data specification, which is instead handled by
`fasthep-curator`. `fasthep-flow` checks the input parameters, imports specified
modules, and maps the YAML onto a workflow. The workflow can then be executed on
a local machine or on a cluster using
[CERN's HTCondor](https://batchdocs.web.cern.ch/local/submit.html) (via Dask) or
[Google Cloud Composer](https://cloud.google.com/composer).

```{toctree}
:hidden: true
global_settings.md
register.md
environments.md
variations.md
```

## Basic usage

Here's a simplified example of a YAML file:

```yaml
tasks:
  - name: printEcho
    type: "fasthep_flow.operators.BashOperator"
    kwargs:
      bash_command: echo "Hello World!"
  - name: printPython
    type: "fasthep_flow.operators.PythonOperator"
    kwargs:
      python_callable: print
      op_args: ["Hello World!"]
```

This YAML file defines two tasks, `printEcho` and `printPython`. The `printEcho`
task uses the `BashOperator`, and the `printPython` task uses the
`PythonOperator`. The `printEcho` task passes the argument `echo "Hello World!"`
to the `bash_command` argument of the `BashOperator`. To make it easier to use
Python callables, `fasthep-flow` provides the `PythonOperator`. This operator
takes a Python callable and its arguments, and then calls the callable with the
arguments.

```{note}
- you can test the validity of a config via `fasthep-flow lint <config.yaml>`
- there are many more custom operators available, see [here](../operators.md)
```
