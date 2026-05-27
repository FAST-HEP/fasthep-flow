# Execution model

`fasthep-flow` executes compiled execution plans rather than raw `author.yaml` files.

The execution model is based on:

- sources that introduce streams
- transforms that process streams
- sinks that consume streams or artifacts
- hooks and observers that inspect runtime behaviour
- backends that schedule and execute the work

The same workflow may therefore execute locally, distributed, or through alternative runtime backends without changing the original workflow description.

---

## From workflow to runtime

A typical workflow progresses through several stages:

```text
author.yaml
  → normalised workflow
  → execution plan
  → runtime execution
```

At runtime, the backend consumes the execution plan and evaluates the workflow graph.

The runtime therefore operates on fully resolved workflow structure rather than the original author YAML.

Runtime execution typically consists of several conceptual phases:

1. **Source execution**  
   Streams are introduced into the workflow.

2. **Transform execution**  
   Workflow stages process and derive data.

3. **Aggregation and finalisation**  
   Histograms and other artifacts are combined or reduced.

4. **Rendering and sinks**  
   Final outputs such as plots or files are produced.

5. **Diagnostics and observation**  
   Hooks and observers inspect workflow state and runtime behaviour.

The exact execution strategy depends on the active backend.

---
## Runtime graphs

Workflows execute as dependency graphs.

For example:

```{mermaid}
%%{init: {"flowchart": {"nodeSpacing": 20, "rankSpacing": 20, "diagramPadding": 5, "useMaxWidth": false}} }%%

flowchart LR
    Read["read.events"] --> Define["stage.BasicVars"]
    Define --> Hist["stage.MuonPt"]
    Hist --> Render["render.MuonPt.0"]
```

The runtime executes nodes according to dependency relationships rather than YAML ordering alone.

This allows:

- automatic dependency scheduling
- parallel execution
- backend-specific optimisation
- reusable execution plans


---

## Streams and artifacts

FAST-HEP workflows distinguish between streams and artifacts.

| Type | Meaning |
|---|---|
| stream | flowing event or tabular data |
| artifact | produced object or output |

Typical streams include:

- event records
- awkward arrays
- tabular datasets
- partitioned data streams

Typical artifacts include:

- histograms
- plots
- reports
- tables
- output files

Different operation types consume and produce different workflow objects.

---



## Hooks and observers

Hooks and observers allow workflows to inspect and react to runtime lifecycle events.

Typical use cases include:

- diagnostics
- schema inspection
- runtime summaries
- provenance collection
- monitoring
- validation

Hooks and observers are distinct from transforms because they primarily inspect workflow execution rather than modify analysis streams.

---

## Runtime backends

Different backends may execute the same workflow plan in different ways.

For example:

- local execution may process data directly in-process
- distributed execution may partition work across workers
- workflow managers may execute stages step-wise

Despite these differences, workflows remain largely backend-independent at the language level.

For more information, see {doc}`../extensibility/strategies-and-backends`.
