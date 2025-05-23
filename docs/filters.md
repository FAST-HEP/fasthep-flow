# Filters

An important part of any analysis workflow is the ability to filter the data.
In `fasthep-flow` this is done via `Filter` tasks. These tasks do not filter the data, but produce a mask that can be used to filter the data.
The mask is a boolean array that is the same length as the input data.
The mask is then used to filter the data in the next task(s).

As the default filter, `fasthep-flow` provides a `Filter` that takes a dictionary of instructions and produces a mask.
The instructions are a dictionary of the form:

```yaml
tasks:
  ...
  event_selection:
    type: fasthep_flow.Filter
    kwargs:
      selection:
        All:
          - MET >= 200
          - njet >= 4
          - Any:
              - nelectron > 0
              - nmuon > 0
  ...
    # The rest of the tasks
```

The values used in the `selection` dictionary are the same as one would use to access the data (e.g. `MET = events.MET`).

You can also use multiple `Filter` tasks to filter the data - either sequential or parallel:

```yaml
tasks:
  ...
  event_selection:
    type: fasthep_flow.Filter
    kwargs:
      selection:
        All:
          - MET >= 200
          - njet >= 4
          - Any:
              - nelectron > 0
              - nmuon > 0
  control_region:
    type: fasthep_flow.Filter
    kwargs:
      selection:
        All:
          - MET < 200
          - njet > 2
          - njet < 4
          - Any:
              - nelectron == 1
              - nmuon == 1
  signal_task:
    type: fasthep_carpenter.Histogram
    needs: [event_selection]
    kwargs:
      inputs:
        - MET
        - njet
      bins:
        - MET: [0, 100, 200, 300]
        - njet: [4, 5, 6, 7]
      overflow: True
  control_task:
    type: fasthep_carpenter.Histogram
    needs: [control_region]
    kwargs:
      inputs:
        - MET
        - njet
      bins:
        - MET: [0, 100, 200]
        - njet: [2, 3]
      overflow: True
  ...
    # The rest of the tasks
```

This will produce two histograms, one for the signal region and one for the control region. The workflow will look like this:

```{mermaid}
flowchart LR
    subgraph Data task
    A[read data]
    end
    subgraph Filter tasks
    B[event_selection]
    C[control_region]
    end
    subgraph Histogram tasks
    signal_hf1[signal_task]
    control_hf1[control_task]
    end
    subgraph Output tasks
    out[write output]
    end
    A --> B
    A --> C
    B --> signal_hf1
    C --> control_hf1
    signal_hf1 --> out
    control_hf1 --> out

```
