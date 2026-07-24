# The execution plan

The execution plan is the boundary between workflow compilation and runtime execution.

Compilation turns an author-facing workflow into a resolved plan. The runtime consumes that plan and orchestrates the implementations described by it.

```{mermaid}
flowchart LR
    Author["author<br/>description"]
    Compiler["compiler"]
    Plan["execution plan"]
    Runtime["runtime"]

    Author --> Compiler --> Plan --> Runtime
```

The plan makes information that was distributed or implicit in the author description explicit:

- which nodes will execute
- which implementations they use
- how nodes are connected
- which products flow between them
- which datasets and partitions are available
- which capabilities have been resolved
- which execution environment has been requested

A serialised copy of the plan is normally written to:

```text
compile/plan.yaml
```

This makes the planned computation inspectable before the analysis itself is executed.

---

## The plan as an interface

The plan is more than a debugging artifact.

It is the interface between the compiler and the runtime:

```text
authoring
    ↓
compiler
    ↓
┌──────────────────────┐
│    execution plan    │
└──────────────────────┘
    ↓
runtime
    ↓
operation implementations
```

The standard FAST-HEP author language is one way to produce this plan, but it is not required by the runtime.

Other authoring tools or compilers can construct compatible plans and use Flow for orchestration directly.

This separation allows:

- authoring syntax to evolve independently of runtime execution
- alternative authoring systems to target Flow
- plans to be inspected before execution
- runtime behaviour to operate on an explicit contract rather than the original YAML

The plan should therefore be thought of as a runtime-oriented representation of the workflow rather than another form of the author description.

---

## Plan structure

A plan contains several related kinds of information.

At a high level, a typical plan looks like:

```yaml
context:
  ...

nodes:
  ...

partitions:
  ...

registry:
  ...

provenance:
  ...

execution:
  ...

execution_hooks:
  ...

reports:
  ...

data_flow:
  ...
```

These sections answer different questions:

| Section | Purpose |
|---|---|
| `context` | resolved datasets and workflow-wide information |
| `nodes` | executable workflow graph |
| `partitions` | units of input data available for execution |
| `registry` | resolved capabilities and implementations |
| `provenance` | how the execution environment was assembled |
| `execution` | requested backend and execution configuration |
| `execution_hooks` | runtime lifecycle extensions |
| `reports` | configured reporting behaviour |
| `data_flow` | compiler-derived field dependencies |

The exact plan schema may evolve while Flow is under active development. This page focuses on the concepts represented by the plan rather than providing an exhaustive schema reference.

---

## Nodes describe executable work

The `nodes` section contains the executable workflow graph.

For example, the exoplanet workflow contains a filtering node:

```yaml
- id: stage.EarthSizedPlanets
  graph_node_id: stage.EarthSizedPlanets
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
    chunk_size: null

  materialize: never
```

This contains considerably more execution information than the original author declaration:

```yaml
- id: EarthSizedPlanets
  op: workshop.tabular.filter
  params:
    expr: "(planet_radius > 0.8) & (planet_radius < 1.2)"
```

The compiler has resolved the author declaration into an explicit runtime node.

---

## Node identity and roles

Every node has an identity and a role.

For example:

```yaml
id: stage.EarthSizedPlanets
role: transform
```

Typical roles include:

```text
source
transform
observer
sink
```

Roles describe how a node participates in the workflow.

A source introduces data:

```text
read.planets
```

Transforms process products:

```text
stage.PlanetRows
stage.EarthSizedPlanets
stage.PlanetTable
```

A sink consumes a product and produces an artifact or other final result:

```text
write.PlanetTable.0
```

The complete exoplanet workflow therefore becomes:

```{mermaid}
flowchart LR
    Source["read.planets<br/>source"]
    Explode["stage.PlanetRows<br/>transform"]
    Filter["stage.EarthSizedPlanets<br/>transform"]
    Project["stage.PlanetTable<br/>transform"]
    Write["write.PlanetTable.0<br/>sink"]

    Source --> Explode --> Filter --> Project --> Write
```

Roles are part of Flow's orchestration model. What a particular node actually does is determined by its registered capability.

---

## Implementations

A plan node identifies the implementation that should perform its work:

```yaml
impl: workshop.tabular.filter
```

This is a resolved capability name, not a Python function embedded directly into the node.

The corresponding registry entry connects it to the actual implementation:

```yaml
transforms:
  workshop.tabular.filter:
    spec: fasthep_workshop.transforms.tabular:TABULAR_FILTER_SPEC
    impl: fasthep_workshop.transforms.tabular:run_tabular_filter
```

This distinction allows the plan to remain declarative about implementation selection while the registry provides the concrete Python entry points.

The runtime can therefore resolve:

```text
workshop.tabular.filter
            ↓
fasthep_workshop.transforms.tabular:run_tabular_filter
```

The specification was used by the compiler to reason about the operation. The implementation is used by the runtime to perform it.

See {doc}`../extending/operations-and-specs` for the operation contract.

---

## Inputs, outputs, and products

Nodes are connected through named products.

The filter node declares:

```yaml
inputs:
  - node_id: stage.PlanetRows
    output_name: stream
    input_name: stream
```

This means:

```text
stage.PlanetRows
    output: stream
         │
         ▼
stage.EarthSizedPlanets
    input: stream
```

The node then produces:

```yaml
outputs:
  stream: event_stream
```

Here:

- `stream` is the output name
- `event_stream` is the product type

The following node can consume that product in turn.

The final writer has:

```yaml
inputs:
  - node_id: stage.PlanetTable
    output_name: stream
    input_name: target
```

and produces:

```yaml
outputs:
  artifact: artifact
```

Products therefore provide the interface between nodes.

Flow orchestrates these connections without needing to understand the internal scientific meaning of every product.

---

## Parameters

The plan contains the resolved parameters passed to implementations.

For example:

```yaml
params:
  expr: (planet_radius > 0.8) & (planet_radius < 1.2)
```

For the final table sink, the parameters are more extensive:

```yaml
params:
  path: snippets/planets.txt
  when: run_end

  fields:
    - name
    - planet_name
    - planet_radius
    - planet_period

  sort_by:
    - planet_radius
    - name
    - planet_name

  limit: 15
```

These parameters originate from the author workflow but may have been validated, normalised, expanded, or supplemented during compilation.

The runtime does not need to reconstruct the author-facing configuration. It receives the parameters required by the selected implementation directly from the plan.

---

## Scope

Products can exist at different execution scopes.

A transform in the exoplanet workflow contains:

```yaml
input_scope: partition
output_scope: partition
```

The final writer instead has:

```yaml
input_scope: global
output_scope: global
```

Scopes describe the level at which a node consumes and produces its products.

Conceptually:

```text
partition
    work associated with one input partition

dataset
    work associated with a complete dataset

global
    work associated with the complete workflow
```

This matters when execution is distributed.

A partition-scoped transform can potentially run independently across many partitions:

```{mermaid}
flowchart TD
    P1["partition 1"] --> T1["transform"]
    P2["partition 2"] --> T2["transform"]
    P3["partition 3"] --> T3["transform"]

    T1 --> Global["global consumer"]
    T2 --> Global
    T3 --> Global
```

A global consumer cannot generally run until the required partition-level products have been combined or otherwise made available at global scope.

Scope therefore forms part of the contract between planning and runtime orchestration.

---

## Partitioning

Nodes also carry partitioning information.

For example:

```yaml
partitioning:
  mode: dataset
  chunk_size: null
```

Partitioning describes how execution of that node relates to the available data partitions.

This is distinct from scope:

- **partitioning** describes how work is divided
- **scope** describes the level at which products are consumed or produced

The compiler places this information in the plan so that the runtime does not need to infer it again.

---

## Materialisation

Nodes can specify whether their output needs to be materialised:

```yaml
materialize: never
```

or:

```yaml
materialize: always
```

Intermediate event streams often do not need to be persisted as standalone artifacts.

A final sink, however, may explicitly produce something that should survive execution:

```yaml
outputs:
  artifact: artifact

materialize: always
```

Materialisation is therefore separate from simply producing a product.

A product may exist because another node needs it during execution without becoming a persistent user-facing artifact.

The runtime uses this information when managing products and execution boundaries.

---

## Context and datasets

Workflow-wide information is stored in the plan context.

For the exoplanet example:

```yaml
context:
  datasets:
    nasa_exoplanets:
      name: nasa_exoplanets
      files:
        - data/NASA/exoplanets.parquet
      nevents: null
      eventtype: data
      group: nasa_exoplanets
      meta: {}

  dataset_names:
    - nasa_exoplanets

  globals: {}

  author_path: examples/NASA/exoplanets/author.yaml
```

This gives runtime components access to the resolved dataset definitions without requiring them to read the original author workflow.

Context can also contain workflow-wide values needed by capabilities during execution.

---

## Partitions describe available work

The plan explicitly lists the input partitions available to the runtime.

For the small exoplanet example:

```yaml
partitions:
  - id: planets__nasa_exoplanets__0
    dataset: nasa_exoplanets
    file: data/NASA/exoplanets.parquet
    source: planets
    part: '0_0'
    start: null
    stop: null
```

This associates the partition with:

- a dataset
- a physical file
- a source
- a partition identifier
- optional boundaries within the input

The example has only one small file and therefore one partition.

A larger analysis may contain many partitions:

```text
dataset
├── file 1
│   ├── partition 1
│   └── partition 2
├── file 2
│   ├── partition 3
│   └── partition 4
└── file 3
    └── partition 5
```

The plan describes these units of work. The execution backend determines how that work is scheduled.

---

## Resolved capabilities

The plan contains the registry used to compile and execute the workflow.

For example:

```yaml
registry:
  sources:
    workshop.parquet:
      spec: fasthep_workshop.sources.parquet:PARQUET_SOURCE_SPEC
      impl: fasthep_workshop.sources.parquet:run_parquet_source

  transforms:
    workshop.tabular.filter:
      spec: fasthep_workshop.transforms.tabular:TABULAR_FILTER_SPEC
      impl: fasthep_workshop.transforms.tabular:run_tabular_filter

  sinks:
    workshop.console_table:
      spec: fasthep_workshop.sinks.table:CONSOLE_TABLE_SPEC
      impl: fasthep_workshop.sinks.table:run_console_table

  backends:
    local.default:
      impl: hepflow.backends:Local
```

Keeping the resolved registry with the plan means the runtime receives the capability environment against which the workflow was compiled.

The process by which profiles and registries are resolved is described in {doc}`compilation`.

---

## Provenance

The plan also records how its capability environment was assembled.

For example:

```yaml
provenance:
  registry_layers:
    - name: builtin
      kind: builtin

    - name: registry
      kind: profile
      path: package:hepflow.profiles/registry.yaml

    - name: fasthep_workshop:registry
      kind: profile
      path: package:fasthep_workshop.profiles/registry.yaml

    - name: author
      kind: author
      path: examples/NASA/exoplanets/author.yaml
```

The provenance section can also record which registry entries were added or overwritten by each layer.

This information is primarily useful for:

- debugging capability resolution
- understanding overrides
- inspecting the compiled environment
- reproducibility

Provenance support continues to evolve, including work to associate resolved capabilities with the package versions that supplied them.

---

## Execution configuration

The requested execution environment is represented separately from the workflow nodes:

```yaml
execution:
  backend: local
  strategy: default
  profiles: []
  resources: {}
  pools: {}
  environment: {}
  config: {}
```

This separation is deliberate.

The workflow graph describes the computation:

```text
read
  ↓
transform
  ↓
transform
  ↓
write
```

The execution configuration describes how and where that computation should run.

The same computational plan can therefore remain largely independent of whether work is executed locally or through a distributed backend.

See {doc}`environments` for execution environments and backend configuration.

---

## Execution hooks and reports

The plan can also contain capabilities that participate in runtime lifecycle events:

```yaml
execution_hooks: []
reports: []
```

These are empty in the exoplanet example, but other profiles can populate them.

Execution hooks can provide behaviour around runtime execution without becoming ordinary data-processing nodes in the workflow graph.

Reports similarly allow runtime and installed capabilities to produce structured information about execution.

Keeping these explicit in the plan means runtime extensions are part of the resolved execution description rather than hidden global behaviour.

---

## Data-flow information

The plan currently also contains the dependency information derived during compilation:

```yaml
data_flow:
  required_sources:
    planets:
      data:
        - name
        - planet_name
        - planet_period
        - planet_radius
```

and:

```yaml
consumers:
  planet_radius:
    - stage.PlanetRows
    - stage.EarthSizedPlanets
    - stage.PlanetTable
    - write.PlanetTable.0
```

This information explains why particular fields are required and which nodes consume them.

For example:

```{mermaid}
flowchart LR
    Source["read.planets<br/>planet_radius"]
    Explode["PlanetRows"]
    Filter["EarthSizedPlanets"]
    Project["PlanetTable"]
    Write["console table"]

    Source --> Explode
    Source --> Filter
    Source --> Project
    Source --> Write
```

The graph above represents **field dependency**, not the product execution graph: `planet_radius` flows through intermediate stream products while remaining required by those downstream operations.

This distinction is useful when debugging why a source has been asked to read a particular field.

See {doc}`compilation` for how these requirements are inferred.

---

## The plan contains execution decisions, not analysis code

A useful way to think about the plan is that it records the decisions needed to orchestrate the workflow without absorbing the implementations themselves.

For a node, Flow knows things such as:

```text
which implementation?
which inputs?
which outputs?
which parameters?
which scope?
which partitioning?
when must the product be materialised?
```

The registered implementation remains responsible for the actual computation.

```{mermaid}
flowchart LR
    Plan["plan node"]

    Runtime["Flow runtime"]

    Impl["registered<br/>implementation"]

    Product["output<br/>product"]

    Plan --> Runtime
    Runtime --> Impl
    Impl --> Product
```

This boundary is what allows analysis capabilities to be replaced or extended without embedding their behaviour in Flow itself.

---

## Plans can come from elsewhere

The standard route to a plan is:

```text
author.yaml
    ↓
Flow compiler
    ↓
plan.yaml
```

But the runtime boundary is the plan, not `author.yaml`.

Another system could instead produce:

```text
alternative authoring system
    ↓
alternative compiler
    ↓
compatible execution plan
    ↓
Flow runtime
```

This is an intentional architectural property.

Flow provides a standard authoring and compilation path, but orchestration is not intrinsically tied to that author language.

It also means that future authoring conveniences can evolve without requiring equivalent changes to the runtime contract.

---

## Plan versus reference schema

This page describes how to understand an execution plan.

It is not intended to document every plan field or every allowed value.

The distinction is:

```text
this page
    conceptual model and guided tour

plan reference
    complete machine-oriented schema
```

As the plan format stabilises, the detailed reference should be generated from the implementation wherever practical so that it remains synchronized with the runtime contract.

---

## Where next?

The plan describes **what Flow has decided should be executed**.

The next step is to see how the runtime turns that description into actual execution.

Continue with {doc}`runtime`.

For how the plan is produced, see {doc}`compilation`.

For operation contracts and how custom capabilities participate in planning and execution, see {doc}`../extending/operations-and-specs`.
