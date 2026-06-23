# Workflow language

`fasthep-flow` workflows describe analysis intent declaratively.

Rather than explicitly writing:

- event loops
- scheduling logic
- task orchestration
- execution ordering

users describe:

- data sources
- transformations
- histogramming
- rendering
- outputs
- dependencies

The workflow engine then determines how workflows should be validated, normalised, planned, and executed.

```{important}
FAST-HEP workflows describe intent rather than implementation details.
```

---

## A minimal workflow

A typical workflow begins as a human-authored YAML file:

```text
author.yaml
```

For example:

```yaml
version: 1.0

use:
  profiles:
    - registry
    - fasthep_carpenter:registry
    - fasthep_render:registry
    - fasthep_workshop:registry

data:
  datasets:
    - name: toy
      eventtype: mc
      files:
        - toy://dy

sources:
  events:
    kind: workshop.toy_source
    stream_type: event_stream

analysis:
  stages:
    - id: BasicVars
      op: hep.define
      params:
        variables:
          - name: Muon_Pt
            expr: "sqrt(Muon_Px ** 2 + Muon_Py ** 2)"
```

This workflow:

1. loads workflow profiles
2. defines a dataset
3. creates an event stream
4. derives a new variable

The workflow itself does not describe:

- execution order
- scheduling
- backend configuration
- parallel execution details

These are inferred automatically during workflow compilation.

---

## Workflow structure

FAST-HEP workflows are organised into sections.

Common top-level sections include:

| Section | Purpose |
|---|---|
| `use` | profiles, registries, presets |
| `data` | datasets and their defaults |
| `sources` | input stream definitions |
| `fields` | reusable analysis-facing input names |
| `outputs` | reusable output schemas for writers |
| `styles` | reusable rendering definitions |
| `analysis` | transforms and workflow stages |
| `observers` | diagnostics and inspection |
| `strategies` | backend/runtime tuning |

Not every workflow requires every section.

For example:

- simple workflows may omit `styles`
- local workflows may omit `strategies`
- workflows without rendering may omit render definitions entirely

### Configuration concerns

These sections describe distinct concerns and should not be combined:

| Section | Question answered |
|---|---|
| `datasets` | Where does the data come from? |
| `fields` | Which analysis-facing names map onto input quantities? |
| `outputs` | Which schema should a writer produce? |
| `styles` | How should rendered output look? |

An output layout can be reused by one or more writers. Version 1 layouts define
a ROOT tree name and selected branches:

```yaml
outputs:
  dimuon_candidates:
    tree: events
    keep:
      - Muon_Pt
      - Muon_Iso

analysis:
  stages:
    - id: SelectDimuonEvents
      op: hep.selection.cutflow
      write:
        - kind: root_tree
          path: dimuon_candidates.root
          use: dimuon_candidates
```

The compiler resolves `use` into writer parameters. Keeping the layout in the
normalized workflow also leaves a stable place for future dependency inference,
so output branches can later be propagated upstream without changing this YAML
syntax.

---

## Profiles and registries

Profiles load workflow capabilities into the current workflow.

For example:

```yaml
use:
  profiles:
    - registry
    - fasthep_carpenter:registry
    - fasthep_render:registry
```

These profiles register:

- operations
- sources
- sinks
- hooks
- rendering implementations

Profiles allow workflows to remain modular and composable.

```{note}
The first profile, `registry`, is the built-in fasthep-flow registry.

Future presets may hide explicit registry configuration from user-facing workflows while preserving fully resolved execution plans internally.
```

For more information, see {doc}`../extensibility/profiles-and-registries`.

---

## Datasets

Datasets describe logical analysis inputs.

For example:

```yaml
data:
  datasets:
    - name: dy
      eventtype: mc
      files:
        - data/DY.root
```

Datasets may represent:

- local ROOT files
- parquet datasets
- distributed storage
- virtual datasets
- generated tutorial data

The meaning of dataset files depends on the source implementation.

For example:

```yaml
files:
  - toy://dy
```

is interpreted internally by the workshop toy source rather than referencing a physical file.

```{note}
Sources may interpret datasets in different ways.

A source may:

- read local files
- prepend caches or redirectors
- stream remote datasets
- query databases
- generate synthetic data on the fly
- construct derived streams from other inputs

The only required contract is that sources produce workflow streams compatible with downstream operations.

In practice this typically means stream records behave like structured event or tabular data objects, for example dictionaries, awkward arrays, or backend-specific stream representations understood by the active runtime.
```

---

## Sources

Sources introduce streams into workflows.

For example:

```yaml
sources:
  events:
    kind: workshop.toy_source
```

This source creates an event stream named:

```text
events
```

which downstream workflow stages consume.

Sources may:

- read ROOT trees
- stream parquet datasets
- generate synthetic events
- query databases
- construct derived streams

---

## Declarative execution

FAST-HEP workflows describe what should be computed rather than how computations should be scheduled.

For example:

```yaml
- id: BasicVars
  op: hep.define
  params:
    variables:
      - name: Muon_Pt
        expr: "sqrt(Muon_Px ** 2 + Muon_Py ** 2)"
```

This workflow stage defines:

```
Muon_Pt
```

from:

```
Muon_Px
Muon_Py
```

The workflow engine automatically infers:

- required inputs
- data dependencies
- execution ordering
- downstream consumers

without requiring users to manually wire execution graphs together:

```(Muon_Px, Muon_Py) → Muon_Pt```

This separation between workflow intent and execution strategy allows workflows to remain portable across runtime backends.

```{note}
The expression syntax used here is currently provided by `fasthep-carpenter` through the `hep.define` operation.

The syntax intentionally remains close to `numexpr` while adding a number of domain-specific functions and symbols commonly used in analysis workflows.

**For example:**

`expr: "sqrt(Muon_Px ** 2 + Muon_Py ** 2)"` is interpreted by the operation implementation rather than by `fasthep-flow` itself.

Different operations may choose to support different expression syntaxes or evaluation models.

Users are also free to implement custom operations with entirely different expression systems.
```

---

## Streams and artifacts

FAST-HEP workflows distinguish between streams and artifacts.

- stream: flowing event/tabular data
- artifact: produced object or output

Examples of streams include:

- event records
- awkward arrays
- tabular data
- partitioned datasets

Examples of artifacts include:

- histograms (pkl)
- plots (PNG)
- tables
- reports
- other output files

This distinction is important because different operation types consume and produce different workflow objects.

---

## Operations

Workflow stages are implemented as operations.

Common operation categories include:

| Category | Purpose |
|---|---|
| sources | introduce streams |
| transforms | derive or modify data |
| sinks | persist or aggregate outputs |
| renderers | generate plots and reports |
| hooks | react to runtime lifecycle events |
| observers | inspect workflow state |

For example:

```yaml
- id: MuonPt
  op: hep.hist
```

fills a histogram artifact from an event stream.

Detailed operation specifications are documented in {doc}`../extensibility/operations-and-specs`.

---

## Workflow compilation

`author.yaml` workflows are not executed directly.

Instead, workflows are compiled through several stages:

```{mermaid}
flowchart TD

    subgraph Compile["Compilation and planning"]
        Author["author.yaml"]
        Profiles["profiles and registries"]
        Normalised["normalised workflow"]
        Dependency["dependency inference"]
        Plan["execution plan"]

        Author --> Normalised
        Profiles --> Normalised
        Normalised --> Dependency
        Dependency --> Plan
    end

    subgraph Execute["Runtime execution"]
        Runtime["runtime execution"]
        Outputs["artifacts and outputs"]

        Runtime --> Outputs
    end

    Plan --> Runtime
```

This compilation process allows workflows to be:

- validated before execution
- serialised and inspected
- transformed into backend-specific plans
- optimised independently of workflow logic
- executed reproducibly across environments

For more information, see:

- {doc}`compilation-pipeline`

---

## Language philosophy

The FAST-HEP workflow language is designed around several core principles.

1. **Portable**  
   Workflow definitions remain independent of execution infrastructure and runtime backends.

2. **Reproducible**  
   Execution plans and workflow state can be serialised and preserved.

3. **Inspectable**  
   Workflows can be validated, normalised, and planned before runtime execution begins.

4. **Declarative**  
   Users describe analysis intent rather than implementation details.

5. **Extensible**  
   Capabilities are composed dynamically through profiles, registries, and operation specifications.

---

## YAML reference

This page introduces the concepts and structure of the workflow language.

For the complete YAML reference, see:

- {doc}`../reference/yaml`
