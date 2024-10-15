# Hello world

Sometimes you just want to see some code. This section contains some real-life
examples of how to use `fasthep-flow`.

```yaml
tasks:
  - name: "hello_world in bash"
    type: "fasthep_flow.operators.BashOperator"
    kwargs:
      bash_command: echo "Hello World!"
```

Save this to a file called `hello_world.yaml`.

```bash
fasthep-flow execute hello_world.yaml
```

This will print "Hello World!" to the console.

So far so good, but what does it actually do? Let's to execute this
step-by-step.

## Creating a flow

The first thing that `fasthep-flow` does is to create a flow. This is done by
creating a `prefect.Flow` object, and adding a task for each step in the YAML
file. The task is created by the `fasthep-flow` operator, and the parameters are
passed to the task as keyword arguments.

We can do this ourselves by creating a flow and adding a task to it.

```python
from fasthep.operators import BashOperator
from prefect import Flow

flow = Flow("hello_world")
task = BashOperator(bash_command="echo 'Hello World!'")
flow.add_task(task)
```

## Running the flow

Next we have to decide how to execute this flow. By default, `fasthep-flow` will
run the flow on the local machine. This is done by calling `flow.run()`.

```python
flow.run()
```

## Running the flow on a cluster

The real strength of `fasthep-flow` is that it can run the flow on a cluster
with the same config file. Internally, this is done by creating a Dask workflow
first, and then running it on the specified cluster (e.g. HTCondor or Google
Cloud Composer). For now, let's just run it on a local Dask cluster.

```bash
fasthep-flow execute hello_world.yaml --workflow="{'transform':'prefect', 'kwargs':{'runner': 'DaskTaskRunner'}}"
```

This will start a Dask cluster on your local machine, and run the flow on it.
While the output will be the same, you will find additional output files for
Dask performance.

## Provenance

In a real-world scenario, you would want to keep track of the provenance of your
flow. This is done automatically by `fasthep-flow`, and you can find the
provenance in the `output/provenance` folder.

For more information, see [Provenance](../provenance.md).

So what does this look like for our hello world example?

```bash
tree output
```

## Next steps

This was a very simple example, but it shows the basic concepts of
`fasthep-flow`. For more realistic examples, see the experiment specific
examples in [Examples](./index.md). For more advanced examples, see
[Advanced Examples](../advanced_examples/index.md).

```

```
