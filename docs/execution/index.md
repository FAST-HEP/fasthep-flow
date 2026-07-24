# Compilation and execution

Flow separates **describing a workflow** from **executing it**.

An author workflow is first compiled into an explicit execution plan. The runtime then consumes that plan and invokes the registered implementations needed to perform the work.

```{mermaid}
flowchart LR
    Author["author.yaml"]
    Normalised["normalised<br/>workflow"]
    IR["workflow<br/>representation"]
    Dependencies["dependency<br/>analysis"]
    Plan["execution<br/>plan"]
    Runtime["runtime"]

    Author --> Normalised --> IR --> Dependencies --> Plan --> Runtime
```

This separation allows Flow to reason about a workflow before processing its data.

Compilation can determine:

- which capabilities are required
- how operations depend on one another
- which data fields are needed
- what products flow between operations
- which implementations are available
- how the workflow should be partitioned
- which outputs should be produced

The resulting execution plan forms the main boundary between **workflow compilation** and **runtime execution**.

---

## From author description to execution plan

Author descriptions are designed for people. They may contain defaults, profiles, references, shorthand, and other conveniences that do not need to appear directly in the runtime representation.

Compilation progressively resolves this information into explicit, machine-oriented representations.

A typical Flow run exposes several of these as compilation artifacts:

```text
compile/
├── analysis.ir.yaml
├── dataset_entries.json
├── deps.yaml
├── normalized.yaml
├── plan.yaml
└── report.compile.yaml
```

At a high level, these provide different views of the workflow:

```text
author.yaml        what the user wrote
      ↓
normalized.yaml    canonical resolved description
      ↓
analysis.ir.yaml   workflow intermediate representation
      ↓
deps.yaml          inferred data requirements
      ↓
plan.yaml          executable description
      ↓
runtime            execution
```

These files are useful representations of the compilation process rather than a guarantee that Flow will always be implemented as exactly this sequence of compiler passes.

---

## Normalisation makes the workflow explicit

Normalisation converts the author-facing workflow into a canonical, explicit representation.

Depending on the workflow, this includes resolving:

- defaults
- included configuration
- profiles
- registry contributions
- references
- authoring conveniences

For example, an author workflow may simply request:

```yaml
use:
  profiles:
    - registry
    - fasthep_workshop:registry
```

The normalised representation contains the resulting registry entries, including the specifications and implementations provided by those profiles.

The normalised workflow is therefore no longer just what the user wrote. It represents the environment in which the workflow will be compiled.

This distinction also allows the author language to evolve independently of later compilation and runtime representations.

---

## Specifications expose what operations need

Flow needs to understand an operation before executing its implementation.

Registered capabilities therefore provide **specifications** that describe the information needed during compilation.

Consider the selection from the exoplanet example:

```yaml
- id: EarthSizedPlanets
  op: workshop.tabular.filter
  params:
    expr: "(planet_radius > 0.8) & (planet_radius < 1.2)"
```

The operation specification allows Flow to determine that this stage requires the field:

```text
planet_radius
```

Flow combines these requirements across the workflow and records the resulting data-flow information.

For the exoplanet workflow, part of `deps.yaml` looks like:

```yaml
consumers:
  planet_radius:
    - stage.PlanetRows
    - stage.EarthSizedPlanets
    - stage.PlanetTable
    - write.PlanetTable.0

required_sources:
  planets:
    data:
      - name
      - planet_name
      - planet_period
      - planet_radius
```

This connects a field used by an operation to the source that ultimately needs to provide it.

The operation and specification contracts are described further in {doc}`../extending/operations-and-specs`.

---

## Dependencies propagate towards sources

Dependency analysis is particularly useful for columnar data.

The author description of the exoplanet source does not explicitly list which Parquet columns should be read:

```yaml
sources:
  planets:
    kind: workshop.parquet
    stream_type: event_stream
```

Instead, Flow determines the required fields from the operations downstream.

The compiled execution plan consequently contains:

```yaml
- id: read.planets
  role: source
  impl: workshop.parquet
  params:
    branches:
      - name
      - planet_name
      - planet_period
      - planet_radius
```

The relationship is therefore approximately:

```{mermaid}
flowchart LR
    Author["author<br/>operations"]
    Specs["operation<br/>specifications"]
    Deps["inferred<br/>requirements"]
    Plan["source fields<br/>in plan"]
    Reader["source<br/>implementation"]

    Author --> Specs --> Deps --> Plan --> Reader
```

By default, only fields required by the compiled workflow need to be requested from the data source.

How a particular source implements that request is outside Flow itself. For example, the Parquet reader used here is provided by `fasthep-workshop`, while HEP-specific readers are provided by other packages.

---

## The workflow becomes an explicit graph

Compilation also connects operations according to their inputs and outputs.

The compiled exoplanet workflow can be visualised as:

```{image} ../_static/images/nasa_workflow.svg
:alt: Compiled Flow graph for the NASA exoplanet example
:align: center
:width: 60%
```

Nodes represent capabilities participating in the workflow, while edges represent products passed between them.

For example, the plan records an input explicitly:

```yaml
- id: stage.EarthSizedPlanets
  role: transform
  impl: workshop.tabular.filter
  inputs:
    - node_id: stage.PlanetRows
      output_name: stream
      input_name: stream
```

This explicit representation allows Flow to determine execution ordering without requiring runtime implementations to discover their own dependencies.

The graph can also support:

- validation
- debugging
- visualisation
- provenance
- execution planning

---

## The execution plan

The execution plan is the main boundary between compilation and runtime execution.

It contains the resolved information needed to execute the workflow, including:

- workflow nodes and their implementations
- inputs and outputs
- operation parameters
- datasets and partitions
- execution configuration
- registry state
- inferred data-flow requirements
- provenance information

A heavily abbreviated version of the exoplanet plan looks like:

```yaml
context:
  datasets:
    nasa_exoplanets: ...

nodes:
  - id: read.planets
    role: source
    impl: workshop.parquet
    outputs:
      stream: event_stream

  - id: stage.EarthSizedPlanets
    role: transform
    impl: workshop.tabular.filter
    inputs:
      - node_id: stage.PlanetRows
        output_name: stream
        input_name: stream
    params:
      expr: (planet_radius > 0.8) & (planet_radius < 1.2)

registry: ...

execution:
  backend: local
  strategy: default

data_flow: ...

provenance: ...
```

Once a plan has been produced, the runtime does not need to interpret the original author description.

This also means that Flow's runtime does not fundamentally require the standard FAST-HEP author language. Another tool can construct a compatible execution plan and use Flow to execute it.

---

## Runtime executes the resolved workflow

At runtime, Flow follows the plan and invokes the implementations selected during compilation.

Conceptually:

```{mermaid}
flowchart LR
    Plan["execution plan"]
    Runtime["Flow runtime"]

    Source["source<br/>implementation"]
    Transform["transform<br/>implementation"]
    Sink["sink<br/>implementation"]

    Plan --> Runtime
    Runtime --> Source
    Runtime --> Transform
    Runtime --> Sink
```

For the exoplanet example:

- `workshop.parquet` reads the data
- `workshop.tabular.explode` expands the planet arrays
- `workshop.tabular.filter` applies the radius selection
- `workshop.tabular.project` selects the output fields
- `workshop.console_table` produces the final table

All of these capabilities are provided by `fasthep-workshop`.

Flow provides the orchestration that connects and executes them. It does not need to contain their domain-specific implementations.

---

## Execution environments can vary

The plan describes the computation separately from the environment in which it runs.

Execution configuration determines how the planned workflow is carried out. The exoplanet example, for instance, uses:

```yaml
execution:
  backend: local
  strategy: default
```

Other execution environments may differ in:

- how work is partitioned
- where workers run
- how resources are allocated
- how products are transported or combined
- which execution modifiers are active

This separation is what allows workflow semantics and execution infrastructure to evolve independently.

Not every capability is necessarily compatible with every execution environment. Specifications, product contracts, and runtime capabilities determine which combinations are valid.

---

## Compilation is extensible

The core compilation process produces artifacts such as the normalised workflow, dependency information, intermediate representation, and execution plan.

Extensions can also participate in compilation through **compile hooks**.

Compile hooks can inspect or augment the workflow during compilation and may produce additional artifacts.

For example, extensions can:

- inspect source files and enrich dataset metadata
- validate workflow properties
- generate diagnostics
- render the compiled workflow graph

The graph shown above can itself be generated through such an extension.

This follows the same principle as the rest of Flow: functionality around the workflow does not need to be hardcoded into the orchestration engine.

---

## Inspecting a run

The compilation artifacts provide several ways to understand what Flow actually resolved.

They can help answer questions such as:

- What did the author description normalise to?
- Which implementation was selected for an operation?
- Why is a particular field being read?
- Which operations consume that field?
- How are two operations connected?
- How was the input data partitioned?
- Which profiles and registries contributed to the workflow?
- What did the runtime actually receive?

This makes the compiler output useful for debugging, validation, provenance, and reproducibility.

---

## The main boundary

The overall model can be summarised as:

```{mermaid}
flowchart LR
    subgraph Compile["Compilation"]
        Author["author<br/>description"]
        Contracts["registered<br/>contracts"]
        Workflow["resolved<br/>workflow"]

        Author --> Workflow
        Contracts --> Workflow
    end

    Plan["execution<br/>plan"]

    subgraph Execute["Execution"]
        Runtime["runtime"]
        Implementations["registered<br/>implementations"]

        Runtime --> Implementations
    end

    Workflow --> Plan --> Runtime
```

Before the plan, Flow is primarily concerned with **understanding, resolving, and validating** the workflow.

After the plan, Flow is primarily concerned with **executing the resolved computation**.

Keeping this boundary explicit allows the author language, operation implementations, and execution environments to evolve independently.

---

## Where next?

The following pages explore these parts of Flow in more detail:

- {doc}`compilation` — the compilation representations and planning process
- {doc}`runtime` — how execution plans are carried out
- {doc}`../extending/operations-and-specs` — how capabilities expose planner-visible contracts and runtime implementations

For runnable workflows, including the exoplanet example used on this page, see the [FAST-HEP workshop](https://fasthep-workshop.readthedocs.io/en/latest/).

```{toctree}
:maxdepth: 1
:hidden:

index
compilation
graph-structure
plan
runtime
environments
```