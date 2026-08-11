# Authoring workflows

Flow workflows are typically described using a `workflow.yaml` file.

The workflow description is the **user-facing representation of a workflow**.
It brings together the data, capabilities, and operations needed to describe
what should be computed.

Flow compiles that description into increasingly explicit representations used
for analysis, planning, and execution.

```{mermaid}
flowchart LR
    Workflow["<b>workflow.yaml</b><br/>workflow description"]:::input
    Normalised["<b>Normalised workflow</b>"]:::flow
    Graph["<b>Logical graph</b>"]:::flow
    Plan["<b>Execution plan</b>"]:::plan

    Workflow --> Normalised --> Graph --> Plan
```

The YAML syntax is therefore an authoring interface rather than Flow's runtime
representation.

For most users, `workflow.yaml` is the normal entry point. Internally, however,
Flow executes the compiled plan rather than interpreting the YAML directly.
This separation also allows alternative frontends and workflow generators to
produce compatible plans without using `workflow.yaml`.

---

## The workflow description

A workflow description brings together several related parts of a computation.

At a high level:

```{mermaid}
flowchart TD
    Workflow["<b>Workflow description</b>"]:::input

    Use["<b>use</b><br/>workflow environment"]:::capability
    Data["<b>data</b><br/>available datasets"]:::capability
    Sources["<b>sources</b><br/>introduce data"]:::capability
    Analysis["<b>analysis</b><br/>scientific operations"]:::capability

    Workflow --> Use
    Workflow --> Data
    Workflow --> Sources
    Workflow --> Analysis
```

The exact syntax available within these sections may evolve, but their responsibilities are useful for understanding the workflow model:

- **profiles** assemble the capabilities and configuration available to the workflow
- **datasets** describe the data being processed
- **sources** introduce that data into the workflow
- **analysis stages** connect operations that transform data or produce new products
- **outputs** can be attached to produced products to describe how they leave the workflow

Other features can extend this model with additional configuration or execution behaviour.

The important distinction is between **describing the workflow** and **implementing the capabilities it uses**.

---

## A running example

The {doc}`../getting-started/index` example analyses a small NASA exoplanet catalogue.

Its workflow description starts by selecting the profiles available to the workflow:

```yaml
use:
  profiles:
    - registry
    - fasthep_workshop:registry
```

It then describes the dataset:

```yaml
data:
  datasets:
    - name: nasa_exoplanets
      files: [data/NASA/exoplanets.parquet]
      eventtype: data
```

and how that data should enter the workflow:

```yaml
sources:
  planets:
    kind: workshop.parquet
    stream_type: event_stream
```

Finally, analysis stages describe what should happen to the resulting data.

For example:

```yaml
- id: EarthSizedPlanets
  op: workshop.tabular.filter
  params:
    expr: "(planet_radius > 0.8) & (planet_radius < 1.2)"
```

The workflow description brings these pieces together, but does not implement them.

`workshop.parquet` and `workshop.tabular.filter` are capabilities supplied by `fasthep-workshop`. Flow resolves and connects them during compilation.

At a glance, the workflow selects an environment, describes its data, introduces a source, and connects analysis operations. The following sections look at each part in turn.

---

## Profiles describe the workflow environment

Profiles determine which capabilities and configuration are available while compiling a workflow.

In the exoplanet example:

```yaml
use:
  profiles:
    - registry
    - fasthep_workshop:registry
```

the workshop profile makes operations such as the Parquet source and tabular transforms available to the workflow.

Profiles are composable. Packages, experiments, or individual projects can therefore build environments from smaller reusable profiles rather than requiring Flow to know about every available capability.

The relationship is roughly:

```{mermaid}
flowchart LR
    Profile["<b>Profile</b><br/>compose environment"]:::input
    Registry["<b>Registry</b><br/>resolve capabilities"]:::capability
    Spec["<b>Specification</b><br/>compile-time contract"]:::flow
    Impl["<b>Implementation</b><br/>runtime behaviour"]:::runtime

    Profile --> Registry
    Registry --> Spec
    Registry --> Impl
```

The details of profile composition and registry resolution are covered in {doc}`../extending/registries-and-profiles`.

---

## Datasets describe available data

The `data` section describes datasets known to the workflow.

For example:

```yaml
data:
  datasets:
    - name: nasa_exoplanets
      files: [data/NASA/exoplanets.parquet]
      eventtype: data
```

Dataset descriptions provide information about the data independently of the operation that will read them.

This distinction becomes useful when the same dataset metadata is needed by different parts of a workflow or when data access changes independently of the scientific description.

Dataset metadata can also contribute to provenance and reproducibility without being embedded in individual processing operations.

---

## Sources introduce data

Sources describe how external data enter the workflow.

The exoplanet example uses:

```yaml
sources:
  planets:
    kind: workshop.parquet
    stream_type: event_stream
```

For a simple workflow with a single dataset and a single source, the distinction
between the dataset description and the source may appear redundant.

The separation becomes more useful when a dataset exposes several logical data sources. For example, a ROOT file may contain multiple TTrees that need to be introduced as
separate streams:

```text
dataset
├── events
├── runs
└── metadata
```

The dataset describes the files and their associated metadata, while sources
describe the different ways data within those files enter the workflow.

Keeping these concepts separate allows multiple sources to share the same
dataset definition without duplicating file or dataset metadata.

`workshop.parquet` identifies a registered source capability.

The workflow description does not contain the Python code required to read Parquet files. Instead, Flow resolves the source through the active registry and uses its specification to understand how it participates in the workflow.

This same mechanism allows another package to provide a different source without changing Flow itself.

---


## Fields provide an analysis-facing vocabulary

Data sources do not always use names that are convenient for analysis code.

A workflow can define aliases for fields provided by its sources:

```yaml
fields:
  analysis_trigger:
    stream: events
    branch: triggerIsoMu24
```

Analysis operations can then refer to the alias:

```yaml
analysis:
  stages:
    - id: TriggerSelection
      op: hep.selection.cutflow
      params:
        selection:
          All:
            - "analysis_trigger == 1"
```

rather than the original source field:

```text
triggerIsoMu24
```

For a single short branch name this may appear unnecessary. It becomes more useful when source schemas contain long, structured, or implementation-specific names.

For example, an analysis might define:

```yaml
fields:
  ss_nSingleScatters: {stream: scatters, branch: "ss./ss.nSingleScatters"}
  s1_phd:             {stream: scatters, branch: "ss./ss.s1Area_phd"}
  s2_raw:             {stream: scatters, branch: "ss./ss.s2Area_phd"}
  s2_corrected:       {stream: scatters, branch: "ss./ss.correctedS2Area_phd"}
  s2_x:               {stream: scatters, branch: "ss./ss.correctedX_cm"}
  s2_y:               {stream: scatters, branch: "ss./ss.correctedY_cm"}
```

The rest of the workflow can then use concise names such as:

```text
s1_phd
s2_corrected
s2_x
s2_y
```

without carrying the source schema throughout the analysis description.

### Aliases participate in dependency inference

Field aliases are resolved during compilation.

If an operation requires:

```text
analysis_trigger
```

and the workflow defines:

```yaml
fields:
  analysis_trigger:
    stream: events
    branch: triggerIsoMu24
```

Flow can trace that requirement back to:

```text
events → triggerIsoMu24
```

The source therefore still knows which underlying field must be read.

Conceptually:

```{mermaid}
flowchart LR
    Source["<b>source</b><br/>triggerIsoMu24"]:::source
    Alias["<b>field alias</b><br/>analysis_trigger"]:::transform
    Selection["<b>selection</b><br/>analysis_trigger == 1"]:::transform

    Source --> Alias --> Selection
```

Aliases therefore change the **name used by the analysis**, not the underlying data dependency.

This provides a useful boundary between the physical data schema and the vocabulary used by the workflow:

```text
external data schema
        ↓
     fields
        ↓
analysis vocabulary
        ↓
analysis operations
```

A workflow can consequently remain readable even when the source format uses names chosen for storage conventions rather than analysis ergonomics.

---

## Analysis stages connect operations

Analysis stages describe computations performed on products flowing through the workflow.

For example:

```yaml
- id: EarthSizedPlanets
  op: workshop.tabular.filter
  params:
    expr: "(planet_radius > 0.8) & (planet_radius < 1.2)"
```

The stage has:

- an identifier, `EarthSizedPlanets`
- an operation, `workshop.tabular.filter`
- configuration for that operation

The operation's specification tells Flow how to reason about it during compilation. Its implementation performs the actual work at runtime.

Specifications also allow Flow to determine which data fields an operation
requires. For example, a specification can inspect fields referenced by an
expression and expose them as dependencies during compilation.

```{mermaid}
flowchart LR
    Operation["<b>Operation</b><br/>uses fields A, B"]:::capability
    Spec["<b>Operation spec</b><br/>exposes requirements"]:::flow
    Compiler["<b>Dependency analysis</b>"]:::flow
    Source["<b>Source projection</b><br/>fields A, B"]:::source

    Operation --> Spec --> Compiler --> Source
```

By default, Flow therefore requests only the fields required by the compiled
workflow rather than reading every available field from a data source. The
resulting source requirements are recorded explicitly in the execution plan.

This is particularly useful for columnar data, where avoiding unused fields can
substantially reduce I/O.

This separation between **workflow-visible behaviour** and **implementation** is central to Flow's extension model.

See {doc}`../extending/operations-and-specs` for the operation contracts themselves.

### Explicit stage dependencies with `needs`

Analysis stages have an implicit dependency on the previous stage by default.
This preserves the sequential structure of existing workflows without requiring
every dependency to be written explicitly.

The `needs` field can override this implicit ordering:

* no `needs` — depend implicitly on the previous analysis stage
* `needs: [StageA, StageB]` — depend explicitly on those stages instead
* `needs: []` — add no stage-ordering dependency

For example, a workflow can expose two independent analysis branches and join
them again later:

```yaml
- id: DiMuonRegion
  op: hep.selection.flag
  needs:
    - DiMuonSelection
    - Recoil_diMuon_CR_jec_Nominal
  params:
    ...

- id: DiElectronRegion
  op: hep.selection.flag
  needs:
    - DiElectronSelection
    - Recoil_diElectron_CR_jec_Nominal
  params:
    ...

- id: FilterChannel
  op: hep.selection.cutflow
  needs:
    - DiMuonRegion
    - DiElectronRegion
```

This produces a logical graph with two independent branches:

```{mermaid}
flowchart LR
    MuSel["<b>DiMuonSelection</b>"]:::transform
    MuRecoil["<b>Recoil_diMuon_CR_jec_Nominal</b>"]:::transform
    ElSel["<b>DiElectronSelection</b>"]:::transform
    ElRecoil["<b>Recoil_diElectron_CR_jec_Nominal</b>"]:::transform

    MuRegion["<b>DiMuonRegion</b>"]:::transform
    ElRegion["<b>DiElectronRegion</b>"]:::transform

    Filter["<b>FilterChannel</b>"]:::transform

    MuSel --> MuRegion
    MuRecoil --> MuRegion

    ElSel --> ElRegion
    ElRecoil --> ElRegion

    MuRegion --> Filter
    ElRegion --> Filter
```

The muon and electron branches can proceed independently and only converge at
`FilterChannel`.

`needs` expresses **ordering only**. It does not select an output product or
bind an input port. Use `from` when an operation consumes a specific upstream
product:

```yaml
from:
  - node: HistA
    port: hist
    as: reference
```

Other dependencies are unaffected by `needs`. Operation specifications, `from`
bindings, source bindings, and parameter-derived field requirements still
contribute to the compiled graph.

Explicit stage dependencies make independent branches visible in the logical
graph and allow them to be scheduled independently where the execution
environment supports it.


---

## Outputs leave the workflow

Products produced by operations can be consumed by further operations or passed
to registered output capabilities.

In the exoplanet example, the final tabular product is passed to
`workshop.console_table`, which writes the resulting table.

As with sources and transforms, Flow does not implement the output itself. The
workflow identifies the capability to use, its specification describes how it
participates in the workflow, and the registered implementation performs the
work.

---

## A common extension model

Sources, transforms, observers, and sinks all follow the same underlying idea: the workflow refers to capabilities through contracts rather than embedding their implementations.

A useful way to think about a workflow is:

```{mermaid}
flowchart LR
    Workflow["<b>workflow.yaml</b><br/>requested capability"]:::input
    Registry["<b>Registry</b><br/>resolution"]:::capability
    Spec["<b>Spec</b><br/>compile-time contract"]:::flow
    Impl["<b>Implementation</b><br/>runtime behaviour"]:::runtime

    Workflow --> Registry
    Registry --> Spec
    Registry --> Impl
```

This separation means an implementation can be replaced without requiring the workflow language or orchestration engine to be redesigned.

For example, a source could adopt a different I/O library, or a transform could use a different computing architecture, while continuing to expose the same workflow contract.

Conversely, projects can introduce entirely new capabilities through the same extension mechanisms.

---

## From workflow description to execution

The workflow description is intentionally more convenient and partially implicit than the representation used by the runtime.

During compilation, Flow resolves that description into increasingly explicit forms:

```{mermaid}
flowchart LR
    Workflow["<b>Workflow description</b>"]:::input
    Normalised["<b>Normalised workflow</b>"]:::flow
    Graph["<b>Logical graph</b>"]:::flow
    Plan["<b>Execution plan</b>"]:::plan
    Runtime["<b>Runtime</b>"]:::runtime

    Workflow --> Normalised --> Graph --> Plan --> Runtime
```

This process resolves capabilities, validates their configuration, determines dependencies, and produces the plan used for execution.

These intermediate representations are useful for inspection and debugging as well as execution.

The compilation process is covered in {doc}`../execution/index`.

---

## Workflow syntax and the workflow model

The standard Flow workflow language currently uses YAML, but its YAML syntax
should not be confused with the underlying workflow and compiler
representations.

The workflow syntax is expected to continue evolving as Flow approaches its
first stable release, including work to make common workflows more concise.

The underlying separation remains:

```text
workflow.yaml
     │
     ▼
normalised workflow
     │
     ▼
logical graph
     │
     ▼
execution plan
```

This allows authoring conveniences to evolve without requiring corresponding changes to the execution model.

```{note}
The examples in this section use the current workflow syntax. During the alpha
development period, consult the reference documentation for the exact fields
supported by the version of Flow you are using.
```

---

## Syntax reference

This section focuses on the **meaning and structure** of workflow descriptions rather than documenting every accepted YAML field.

The reference documentation will provide the exact workflow schema, including:

- available fields
- accepted types
- required and optional values
- defaults
- validation rules

Where practical, this reference will be generated from the same models used by Flow itself so that the documentation remains aligned with the implementation.

---

## Where next?

If you want to understand what Flow does with a workflow description, continue with {doc}`../execution/index`.

If you want to provide your own sources, transforms, sinks, or other capabilities, see {doc}`../extending/index`.

For complete runnable workflows and step-by-step examples, see the [FAST-HEP workshop](https://fasthep-workshop.readthedocs.io/en/latest/).
