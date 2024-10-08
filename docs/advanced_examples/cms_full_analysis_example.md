# A CMS analysis with paper-ready plots

The goal of this example is to show how to use `fasthep-flow` to perform a full
HEP analysis with paper-ready plots and tables. This includes more complex tasks
such as:

- systematic uncertainties for MC samples and/or physics objects
- control regions for background estimation
- scale factors for data/MC comparison
- statistical analysis of the results
- provenance tracking for reproducibility and accountability

```{note}
Looking for volunteers and public data to create this example!
```

## Control regions

Control regions are used to estimate the background in the signal region or
verify procedures outside the signal region (e.g. for searches). From a workflow
perspecite, they are effectively independent branches. To split a workflow, you
will need to use the `needs` keyword:

```yaml
tasks:
  - name: Data Input task
    ...
  - name: Common selection
    ...
  - name: signal selection
    needs: [Common selection]
    ...
  - name: control selection
    needs: ["Common selection"]
    ...
  - name: Create histograms for signal region
    needs: ["signal selection"]
    ...
    - name: Create histograms for control region
    needs: ["control selection"]
    ...
  - name: Output
    needs: ["Create histograms for signal region", "Create histograms for control region"]
    ...
```

This will create a DAG like this:

```{mermaid}
flowchart TD
    A[Data Input task] --> B(Common selection)
    B --> C[signal selection]
    B --> D[control selection]
    C --> E[Create histograms for signal region]
    D --> F[Create histograms for control region]
    E --> G[Output]
    F --> G
```
