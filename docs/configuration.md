# Configuration

`fasthep-flow` provides a way to describe a workflow in a YAML file. It does not handle the input data specification, which is instead handled by `fasthep-curator`. `fasthep-flow` checks the input parameters, imports specified modules, and maps the YAML onto an Apache Airflow DAG. The workflow can then be executed on a local machine or on a cluster using [CERN's HTCondor](https://batchdocs.web.cern.ch/local/submit.html) (via Dask) or [Google Cloud Composer](https://cloud.google.com/composer).

Here's a simplified example of a YAML file:

```yaml

stages:
  - name: printEcho
    type: "airflow.operators.bash.BashOperator"
    kwargs:
      bash_command: echo "Hello World!"
  - name: printPython
    type: "airflow.operators.bash.PythonOperator"
    kwargs:
      python_callable: print
      op_args: ["Hello World!"]

```

This YAML file defines two stages, `printEcho` and `printPython`. The `printEcho` stage uses the `BashOperator` from Apache Airflow, and the `printPython` stage uses the `PythonOperator` from Apache Airflow. The `printEcho` stage passes the argument `echo "Hello World!"` to the `bash_command` argument of the `BashOperator`. The `printPython` stage uses the

To make it easier to use Python callables, `fasthep-flow` provides a `pycall` operator. This operator takes a Python callable and its arguments, and then calls the callable with the arguments. The `printPython` stage can be rewritten using the `pycall` operator as follows:

```yaml

stages:
  - name: printEcho
    type: "airflow.operators.bash.BashOperator"
    kwargs:
      bash_command: echo "Hello World!"
  - name: printPython
    type: "fasthep_flow.operators.pycall.PyCallOperator"
    args: ["Hello World!"]
    kwargs:
      callable: print
```

:::{note}
 - you can test the validity of a config via `fasthep-flow lint <config.yaml>`
 - there are many more custom operators available, see [here](operators.md)


