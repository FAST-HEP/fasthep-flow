# Products, outputs, and their configuration

Products are outputs of tasks, such as histograms, tables, in-memory data, files, etc. They are defined in the `products` section of the configuration file:

```yaml
tasks:
  - name: my_task
    type: fasthep_carpenter.Histogram
    kwargs:
      inputs:
        - variable1
        - variable2
      bins:
        - variable1: [0, 100, 200]
        - variable2: [0, 50, 100]
      overflow: True
    products:
      histograms:
        - from: fasthep_carpenter.Histogram
      files: [output_file.root]
```

In this case, the task `my_task` produces a histogram and an output file. The `products` section specifies the types of products that the task will produce.
`histograms` is a list of histograms produced by the task, the `from` will check the specified task type and fill the list with the histograms produced by that task. The `files` section specifies the output files that will be created by the task.
This sort of description is useful for tracking outputs and can be passed to other tools like Snakemake for workflow management.
Other product types can be defined similarly, such as `data`, `tables`, or any custom type that your workflow might produce (e.g. `mypackage.myproducer`).
`data` is a special type that indicates in-memory data, which can be used for tasks that produce data that does not need to be saved to a file.

Summary of product types:

- `histograms`: A list of histograms produced by the task.
- `files`: A list of output files produced by the task.
- `data`: In-memory data produced by the task.
- `tables`: Tables produced by the task.
- `mypackage.myproducer`: Custom product types defined by your workflow.

```{note}
Producers are implemented as plugins, so you can create your own product types by implementing a plugin that defines the product type and how it is produced.
They can be used either for tasks or as global plugins in the configuration file.
```
