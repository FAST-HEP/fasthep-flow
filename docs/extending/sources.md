# Sources

Sources introduce data into a Flow workflow.

They form the boundary between external or generated data and the products consumed by downstream operations:

```text
external or generated data
          ↓
        source
          ↓
        product
          ↓
   downstream operations
```

A source does not have to read a file. It may read a file format, query an external system, generate synthetic data, or obtain data through another mechanism.

Sources use the common {doc}`operation contract <operations-and-specs>`. This page focuses on what is distinctive about their role in the workflow.

---

## Sources in an workflow

Sources are declared in the top-level `sources` section.

For example, the FAST-HEP workshop provides a synthetic source:

```yaml
sources:
  events:
    kind: workshop.toy_source
    stream_type: event_stream
    nevents: 1000
    seed: 2601
```

Here:

```text
events
    workflow-visible source name

workshop.toy_source
    registered source capability

event_stream
    product introduced by the source
```

Compilation turns this declaration into a source node such as:

```text
read.events
    role: source
    impl: workshop.toy_source
```

Downstream operations consume the product produced by that node.

```{mermaid}
flowchart LR
    Source["<b>read.events</b><br/>source"]:::source
    Transform["<b>stage.BasicVars</b><br/>transform"]:::transform
    Consumer["<b>Downstream operation</b>"]:::transform

    Source -->|event stream| Transform
    Transform -->|event stream| Consumer
```

---

## Sources are more than file readers

File-backed sources are common:

```text
ROOT file → source → event stream
```

or:

```text
Parquet file → source → event stream
```

but the workshop toy source demonstrates that the abstraction is broader:

```text
configuration + seed → source → event stream
```

Other sources could obtain data from databases, network services, object stores, simulations, or experiment-specific data systems.

The source abstraction therefore describes **how a product enters the workflow**, not a particular storage technology.

```{note}
Flow should not need built-in knowledge of ROOT, Parquet, experiment file conventions, or remote data services.

Those details belong to the package providing the source.
```

---

## Why sources are named

With one input stream:

```yaml
sources:
  events:
    kind: workshop.toy_source
```

the source name may initially appear redundant.

It becomes important when a workflow has several sources.

For example, a ROOT dataset may expose several TTrees:

```yaml
sources:
  events:
    kind: root_tree
    tree: Events

  metadata:
    kind: root_tree
    tree: Metadata

  runs:
    kind: root_tree
    tree: Runs
```

Conceptually:

```text
dataset
├── Events    → events
├── Metadata  → metadata
└── Runs      → runs
```

Each source introduces a separately identifiable stream that operation specs can refer to explicitly.

This avoids building assumptions such as “a dataset has exactly one tree” or “all data enter through one stream” into Flow itself.

---

## Sources receive compiled data requirements

One of the most important source-specific behaviours is the interaction with dependency inference.

Consider the NASA exoplanet example:

```yaml
sources:
  planets:
    kind: workshop.parquet
    stream_type: event_stream
```

The workflow does not explicitly list the columns that should be read.

Instead, downstream operations refer to:

```text
name
planet_name
planet_radius
planet_period
```

Their operation specs expose these field requirements to Flow, which propagates them backwards to the source.

The compiled dependency information contains:

```yaml
required_sources:
  planets:
    data:
      - name
      - planet_name
      - planet_period
      - planet_radius
```

and the source node is planned with:

```yaml
branches:
  - name
  - planet_name
  - planet_period
  - planet_radius
```

Conceptually:

```{mermaid}
flowchart RL
    Operations["<b>Downstream operations</b>"]:::transform
    Requirements["<b>Field requirements</b>"]:::flow
    Source["<b>read.planets</b>"]:::source

    Operations --> Requirements --> Source
```

The source implementation can then use these requirements to read only the required data where the underlying format supports projection.

```{note}
A source does not have to support efficient field projection to participate in Flow.

When it does, dependency inference allows unnecessary I/O to be avoided without duplicating field lists throughout `workflow.yaml`.
```

---

## Field aliases resolve back to sources

Workflows can provide analysis-facing names for source fields:

```yaml
fields:
  analysis_trigger:
    stream: events
    branch: triggerIsoMu24
```

A downstream operation can refer to:

```text
analysis_trigger
```

while dependency inference resolves the requirement back to:

```text
events → triggerIsoMu24
```

This allows the analysis-facing vocabulary to remain separate from long or format-specific source names while retaining the correct physical data dependency.

See the authoring documentation for more on field aliases.

---

## Datasets and sources are different concepts

A dataset describes **which data should be processed**.

A source describes **how data enter the workflow**.

For example:

```yaml
data:
  datasets:
    - name: nasa_exoplanets
      files:
        - data/NASA/exoplanets.parquet

sources:
  planets:
    kind: workshop.parquet
    stream_type: event_stream
```

The two answer different questions:

```text
dataset
    what data belong to this logical dataset?

source
    how should Flow obtain a product from them?
```

This separation also accommodates sources such as synthetic generators that do not require file-backed datasets at all.

---

## Sources participate in partition planning

Sources also provide the information needed to turn input data into units of
execution.

Depending on the source contract, partitions may correspond to:

- datasets
- files
- entry ranges
- chunks
- another source-specific unit

Compilation uses this information to construct the partitions recorded in the
execution plan.

For example, the NASA workflow produces a partition resembling:

```yaml
- id: planets__nasa_exoplanets__0
  dataset: nasa_exoplanets
  file: data/NASA/exoplanets.parquet
  source: planets
  part: '0_0'
```

Conceptually:

```text
dataset
   ↓
partitions
   ↓
source
   ↓
partition products
```

The available partitioning model depends on the source contract and compiler configuration; the execution backend later determines how those planned partitions are scheduled.

See {doc}`../execution/plan` for how partitions are represented after compilation.

---

## Providing a source

Sources are registered capabilities and can be provided entirely by external packages.

For example, `fasthep-workshop` provides both:

```text
workshop.toy_source
workshop.parquet
```

through its registry.

Once the corresponding profile is active, Flow can compile those sources without containing any toy-generation or Parquet-specific implementation itself.

The common operation contract is described in {doc}`operations-and-specs`, and {doc}`registries-and-profiles` explains how source capabilities become available to a workflow.

The FAST-HEP workshop is the recommended place for step-by-step examples of implementing custom sources.

---

## Where next?

Sources introduce products into the workflow.

Continue with {doc}`transforms` for operations that consume and produce workflow products.

For related concepts:

- {doc}`operations-and-specs` — the common operation contract
- {doc}`registries-and-profiles` — making source capabilities available
- {doc}`../execution/plan` — source nodes and partitions after compilation
