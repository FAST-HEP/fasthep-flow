# Runtime execution

The Flow runtime executes a compiled execution plan.

By the time runtime execution begins, the workflow has already been resolved and planned. The runtime does not need to interpret `author.yaml`, infer field dependencies, or decide which implementation an operation refers to.

Instead, it receives an explicit plan describing:

- which nodes exist
- how those nodes are connected
- which implementations they use
- which input partitions are available
- which products they consume and produce
- at which scope they operate
- which execution backend should be used

```{mermaid}
flowchart LR
    Plan["execution<br/>plan"]
    Runtime["Flow<br/>runtime"]
    Backend["execution<br/>backend"]
    Implementations["registered<br/>implementations"]
    Artifacts["products and<br/>artifacts"]

    Plan --> Runtime
    Runtime --> Backend
    Backend --> Implementations
    Implementations --> Artifacts
```

Flow is responsible for **orchestration**. Registered implementations remain responsible for the actual work performed on the data.

---

## From plan to execution

Consider the filtering node from the exoplanet workflow:

```yaml
- id: stage.EarthSizedPlanets
  role: transform
  impl: workshop.tabular.filter

  inputs:
    - node_id: stage.PlanetRows
      output_name: stream
      input_name: stream

  params:
    expr: (planet_radius > 0.8) & (planet_radius < 1.2)

  outputs:
    stream: event_stream

  input_scope: partition
  output_scope: partition

  partitioning:
    mode: dataset

  materialize: never
```

The runtime does not need to understand what an Earth-sized planet is or how the expression should be evaluated.

Its responsibilities are instead approximately:

1. determine when the node is ready to run
2. obtain the required `stream` product
3. resolve `workshop.tabular.filter`
4. invoke its registered implementation with the planned parameters and runtime context
5. receive the resulting `event_stream`
6. make that product available to downstream nodes

The operation implementation owns the domain-specific computation.

This separation is central to Flow:

```text
Flow
    orchestrates computation

registered implementations
    perform computation
```

---

## The backend executes planned work

Flow separates runtime orchestration from the mechanism used to execute work.

The plan selects an execution configuration:

```yaml
execution:
  backend: local
  strategy: default
```

The resolved registry provides the available backend implementation:

```yaml
backends:
  local.default:
    impl: hepflow.backends:Local
```

The backend is responsible for carrying out the work described by the runtime.

This allows Flow to support different execution systems without embedding their scheduling mechanisms into workflow operations.

Conceptually:

```{mermaid}
flowchart TD
    Runtime["Flow runtime"]

    Local["local backend"]
    Distributed["distributed backend"]
    Future["other backends"]

    Runtime --> Local
    Runtime --> Distributed
    Runtime --> Future
```

The backend changes **how work is executed**, not what the individual workflow operations mean.

Execution environments and backend configuration are discussed in {doc}`environments`.

---

## Partition-level execution

Many data-processing operations execute independently for each input partition.

For example, suppose a dataset has three partitions:

```text
partition 1
partition 2
partition 3
```

A chain of partition-scoped transforms can be evaluated independently for each:

```{mermaid}
flowchart LR
    P1["partition 1"] --> A1["PlanetRows"] --> B1["EarthSizedPlanets"] --> C1["PlanetTable"]
    P2["partition 2"] --> A2["PlanetRows"] --> B2["EarthSizedPlanets"] --> C2["PlanetTable"]
    P3["partition 3"] --> A3["PlanetRows"] --> B3["EarthSizedPlanets"] --> C3["PlanetTable"]
```

Each execution follows the same nodes and parameters, but operates on a different partition of the input data.

This is one of the main sources of parallelism available to execution backends.

Flow describes these operations in terms of products and partitions rather than requiring implementations to manage distributed scheduling themselves.

---

## Products carry results between nodes

Operations communicate through products.

For the exoplanet workflow:

```text
read.planets
    │
    │ event_stream
    ▼
PlanetRows
    │
    │ event_stream
    ▼
EarthSizedPlanets
    │
    │ event_stream
    ▼
PlanetTable
    │
    │ event_stream
    ▼
console_table
```

Each node consumes named inputs and produces named outputs.

For example:

```yaml
inputs:
  - node_id: stage.PlanetRows
    output_name: stream
    input_name: stream

outputs:
  stream: event_stream
```

The runtime uses these relationships to route products between implementations.

It does not need to know the internal representation of every product type.

---

## Product types can define runtime behaviour

Some products require behaviour beyond simply passing a Python value from one operation to another.

For example, partition-level results may need to be combined before they can be consumed by a global operation.

Flow supports registered **product handlers** for this purpose.

A registry may contain entries such as:

```yaml
product_handlers:
  event_stream:
    merge: ...

  histogram:
    merge: ...
    materialize: ...
```

Product handlers allow packages that define product types to also define how those products behave at runtime.

Depending on the product, this may include:

- merging partition results
- converting a runtime product into a persistent artifact
- preparing a product for another execution scope

This follows the same extensibility principle as operations: Flow provides the orchestration mechanism, while packages that understand a product provide its specialised behaviour.

---

## Moving between scopes

Not every operation runs at partition scope.

The plan distinguishes between:

```text
partition
dataset
global
```

A partition-scoped operation can execute independently for each partition.

A dataset-scoped operation consumes information associated with a complete dataset.

A global operation consumes information across the complete workflow.

Consider the final table writer from the exoplanet example:

```yaml
input_scope: global
output_scope: global
```

while the preceding transform produces:

```yaml
output_scope: partition
```

The runtime therefore needs to cross a scope boundary:

```{mermaid}
flowchart TD
    P1["partition product"]
    P2["partition product"]
    P3["partition product"]

    Merge["product handling"]

    Global["global product"]

    Sink["global sink"]

    P1 --> Merge
    P2 --> Merge
    P3 --> Merge

    Merge --> Global --> Sink
```

How products are combined depends on their product type.

A histogram, event stream, cutflow, or custom analysis product may require different merge semantics.

Those semantics belong to the corresponding product handler rather than being hardcoded into Flow.

---

## Materialisation

Runtime products do not necessarily become persistent artifacts.

Intermediate products may exist only long enough to be consumed by downstream operations:

```yaml
materialize: never
```

Other nodes explicitly produce outputs that should survive execution:

```yaml
materialize: always
```

Materialisation may involve writing:

- files
- histograms
- tables
- reports
- other persistent artifacts

The details depend on the product and its registered capabilities.

This distinction allows Flow to reason separately about:

```text
a value needed during execution
```

and:

```text
an artifact that should remain after execution
```

---

## Sources participate in the same model

Sources are runtime implementations too.

For the exoplanet workflow, the source node is:

```yaml
- id: read.planets
  role: source
  impl: workshop.parquet

  params:
    stream_type: event_stream
    branches:
      - name
      - planet_name
      - planet_period
      - planet_radius

  outputs:
    stream: event_stream
```

The compiler has already determined which fields are required.

At runtime, the source implementation receives this resolved request together with information about the partition being processed.

The source then decides how to satisfy that request.

For a Parquet source this might mean column projection. A ROOT source may interpret the same idea as branch selection. Another source may obtain its data from an entirely different storage system.

Flow does not need to understand those storage details.

---

## Sinks complete data-flow paths

Sinks consume products and produce final outputs or artifacts.

The exoplanet workflow ends with:

```yaml
- id: write.PlanetTable.0
  role: sink
  impl: workshop.console_table

  inputs:
    - node_id: stage.PlanetTable
      output_name: stream
      input_name: target

  outputs:
    artifact: artifact

  input_scope: global
  output_scope: global

  materialize: always
```

The sink is responsible for turning the input product into the requested output.

In this example, that means producing the final text table:

```text
+---------------+----------------------+---------------+
| Planet        | Radius [Earth radii] | Period [days] |
+---------------+----------------------+---------------+
| Kepler-950 b  | 0.801                | 98.718        |
| Kepler-51 c   | 0.803                | 85.312        |
...
+---------------+----------------------+---------------+
```

Flow determines when the sink can execute and supplies its input. The sink implementation determines how the table is produced.

---

## Runtime context

Implementations often need more than their immediate input product and parameters.

The runtime provides context describing the execution in which an operation is running.

Depending on the operation and execution scope, this can include information about:

- the current dataset
- the current partition
- workflow-wide context
- artifact locations
- execution configuration
- registered capabilities
- runtime services

This avoids requiring operations to discover workflow state through global variables or hidden configuration.

The precise runtime context contract is part of the extension API and is documented alongside custom operation development.

---

## Runtime extensions

Not all runtime behaviour belongs in the data-flow graph.

Execution hooks allow extensions to react to runtime lifecycle events.

These can support concerns such as:

- diagnostics
- metrics
- logging
- provenance
- execution summaries
- resource monitoring

Conceptually:

```text
run starts
    │
    ├── execution hooks
    │
    ▼
partitions and nodes execute
    │
    ├── execution hooks
    │
    ▼
aggregation / finalisation
    │
    ├── execution hooks
    │
    ▼
run ends
```

This keeps cross-cutting runtime behaviour separate from scientific data-processing operations.

The set of hooks is resolved during compilation and recorded in the plan.

---

## Execution modifiers

Some execution environments require behaviour that affects how operations are executed without changing the operation itself.

Flow supports registered execution modifiers for this purpose.

Examples may include:

- preparing accelerator state
- loading runtime libraries
- compiling specialised kernels
- modifying execution context
- backend-specific setup

For example, an extension might provide capabilities such as:

```yaml
execution_modifiers:
  gpu.preload:
    impl: ...

  cuda.jit:
    impl: ...
```

The important distinction is that these modify **execution behaviour**, not workflow semantics.

An analysis operation should not need to become a different kind of workflow node merely because its implementation is executed using different hardware or runtime preparation.

---

## Runtime diagnostics and reports

Runtime execution can produce more than the scientific outputs of the workflow.

Depending on the active profiles and hooks, a run may also generate:

```text
debug/
├── dask/
├── logs/
└── performance/

reports/
├── diagnostics/
├── provenance/
└── schema/

run_summary.yaml
```

These outputs can describe:

- what was executed
- which backend was used
- runtime failures or warnings
- performance information
- provenance
- generated artifacts

Such capabilities are generally provided through extensions rather than being intrinsic to every Flow execution.

This keeps the runtime small while allowing richer environments to add inspection and diagnostics when required.

---

## Flow does not perform the scientific computation

The runtime deliberately has a narrow responsibility.

For a transform such as:

```text
workshop.tabular.filter
```

Flow does **not** implement filtering.

For a source such as:

```text
workshop.parquet
```

Flow does **not** implement Parquet reading.

For a HEP operation such as:

```text
hep.hist
```

Flow does **not** implement histogramming.

Instead:

```{mermaid}
flowchart TD
    Flow["Flow runtime<br/><b>orchestration</b>"]

    Source["source implementation"]
    Transform["transform implementation"]
    Sink["sink implementation"]
    Handler["product handler"]

    Flow --> Source
    Flow --> Transform
    Flow --> Sink
    Flow --> Handler
```

This separation allows implementations to evolve independently of the workflow engine.

A capability can be replaced with a new implementation — using a different library, data representation, algorithm, or computing architecture — while retaining the orchestration model and, where contracts remain compatible, the workflow itself.

---

## Runtime versus backend

The terms **runtime** and **backend** describe different layers.

The runtime understands Flow concepts:

- nodes
- products
- dependencies
- scopes
- partitions
- materialisation
- hooks

The backend understands how work is executed:

- directly in the current process
- on local workers
- through a distributed scheduler
- in another execution environment

A useful approximation is:

```{mermaid}
flowchart TD
    Plan["execution plan"]
    Runtime["Flow runtime<br/><b>what is ready to run?</b>"]
    Backend["backend<br/><b>where/how should it run?</b>"]
    Impl["implementation<br/><b>perform the work</b>"]

    Plan --> Runtime --> Backend --> Impl
```

The boundary allows execution infrastructure to evolve without becoming part of operation semantics.

---

## Execution produces artifacts and a run record

When execution completes, scientific outputs are written beneath the artifact directory.

For the exoplanet example:

```text
artifacts/
└── files/
    └── snippets/
        └── planets.txt
```

The run also produces:

```text
run_summary.yaml
```

The summary provides a top-level record of the execution and generated outputs.

Additional artifacts, reports, and diagnostics depend on the capabilities activated for the workflow.

---

## The runtime starts from the plan

The most important runtime boundary is therefore:

```text
                     compilation
                         │
                         ▼
                  ┌─────────────┐
                  │    plan     │
                  └─────────────┘
                         │
                         ▼
                      runtime
                         │
             ┌───────────┼───────────┐
             ▼           ▼           ▼
         backend     operations    extensions
             │           │           │
             └───────────┼───────────┘
                         ▼
                 products / artifacts
```

The runtime does not need to know how the plan was authored.

Its job is to orchestrate the resolved computation represented by that plan.

This is what makes the execution plan a meaningful interface between authoring, compilation, and execution.

---

## Where next?

This page described **how Flow executes a plan**.

Continue with {doc}`environments` for how execution is mapped onto local and distributed computing environments.

For the structure consumed by the runtime, see {doc}`plan`.

For how that plan is constructed, see {doc}`compilation`.

For implementing capabilities that Flow can orchestrate, see {doc}`../extending/index`.
