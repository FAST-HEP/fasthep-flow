# Using fasthep-flow in interactive Python

Sometimes it is useful to use fasthep-flow interactively in Python, e.g. in a
Jupyter Notebook. For convenience, `fasthep-flow` provides a module for this. To
run the CMS Public Tutorial example in a Jupyter Notebook, run:

```python
import fasthep_flow.interactive as ffi

# verify the workflow config - this is optional and will throw an error if the config is invalid
ffi.lint("examples/cms_pub_example.yaml")

# load the workflow
workflow = ffi.load_workflow("examples/cms_pub_example.yaml")

# run the workflow
results = workflow.run()
```

Similarly to the CLI, you can specify the number of events to process, the
executor to use, and the stage to run:

```python
results = workflow.run(
    n_events=1000,
    executor="dask-local",
    stages=["Select events", "Histograms after selection"],
)

ffi.pretty_print(results)
```

## Advanced usage

### Using a custom executor

## Functionality not available in CLI

### Inspecting the workflow

Sometimes it is useful to inspect the workflow, e.g. to see what variables are
available at a given stage, optimizations applied, etc. This can be done using
the `inspect` method:

```python
dag = ffi.inspect(workflow)
```
