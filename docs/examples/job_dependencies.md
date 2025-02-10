# Job Dependencies

By default all jobs are run in sequence and each job takes the output of the
previous job as input. However, you can specify dependencies between jobs to run
them in parallel.

```yaml
tasks:
  - name: "A"
    type: "fasthep_flow.operators.BashOperator"
    kwargs:
      bash_command: echo "A"
  - name: "B"
    type: "fasthep_flow.operators.BashOperator"
    kwargs:
      bash_command: echo "B"
  - name: "C"
    type: "fasthep_flow.operators.BashOperator"
    kwargs:
      bash_command: echo "C"
    dependencies:
      - "A"
  - name: "D"
    type: "fasthep_flow.operators.BashOperator"
    kwargs:
      bash_command: echo "D"
    dependencies:
      - "B"
  - name: "Y"
    type: "fasthep_flow.operators.BashOperator"
    kwargs:
      bash_command: echo "Y"
    dependencies:
      - "C"
      - "D"
```

which will create the following flow:

```{mermaid}
flowchart TD
    A["A()"]
    B["B()"]
    C["C(A, str)"]
    D["D(B, str)"]
    Y["Y(C, D, str)"]
    A --> C --> Y
    B --> D --> Y
```

## Next steps

This was a very simple example, but it shows the basic concepts of
`fasthep-flow`. For more realistic examples, see the experiment specific
examples in [Examples](./index.md). For more advanced examples, see
[Advanced Examples](../advanced_examples/index.md).
