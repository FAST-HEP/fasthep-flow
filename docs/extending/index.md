# Extending Flow

Flow is designed around replaceable capabilities.

Flow provides workflow compilation, dependency reasoning, planning, and runtime
orchestration. Domain- and application-specific behaviour can be supplied by
external packages through registered extensions.

This separation allows packages, experiments, and individual analyses to add
capabilities without modifying Flow itself.

---

## Extension points

Extensions participate at different points in the workflow lifecycle.

```{mermaid}
flowchart LR
    Workflow["<b>workflow.yaml</b><br/>workflow description"]:::input
    Compile["<b>Compilation</b>"]:::flow
    Plan["<b>Execution plan</b>"]:::plan
    Runtime["<b>Flow runtime</b>"]:::runtime
    Backend["<b>Backend</b>"]:::capability
    Infra["<b>Computing resources</b>"]:::capability

    Hook["<b>Compile hook</b>"]:::capability
    Modifier["<b>Execution modifier</b>"]:::capability

    Source["<b>Source</b>"]:::source
    Transform["<b>Transform</b>"]:::transform
    Observer["<b>Observer</b>"]:::observer
    Sink["<b>Sink</b>"]:::sink

    Workflow --> Compile --> Plan --> Runtime --> Backend --> Infra

    Hook -.-> Compile
    Modifier -.-> Runtime

    Source --> Transform --> Sink
    Transform -.-> Observer
```

The main extension points are:

| Extension | Role |
|---|---|
| {doc}`sources` | introduce data into the workflow |
| {doc}`transforms` | consume and produce workflow products |
| {doc}`observers` | inspect products without becoming part of the main transformation path |
| {doc}`sinks` | consume products and produce artifacts or external outputs |
| {doc}`compile-hooks` | extend the compilation process |
| {doc}`execution-modifiers` | adapt runtime execution behaviour |
| {doc}`backends` | map executable work onto computing infrastructure |

The first four participate directly in the executable workflow graph. Compile hooks, execution modifiers, and backends extend the surrounding compilation and execution machinery.

```{note}
Not every extension point has exactly the same contract.

Data-flow operations use planner-visible specifications so that Flow can reason about them before execution. Other extension interfaces have contracts appropriate to their position in the workflow lifecycle.
```

---

## Registered capabilities

Workflow authors refer to capabilities by name rather than importing implementations directly.

For example:

```yaml
- id: BasicVars
  op: hep.define
  params:
    variables:
      - name: Muon_Pt
        expr: "sqrt(Muon_Px ** 2 + Muon_Py ** 2)"
```

The active registry resolves `hep.define` to the capability supplied by an installed package:

```yaml
transforms:
  hep.define:
    spec: fasthep_carpenter.operations.define:DEFINE_SPEC
    impl: fasthep_carpenter.operations.define:run_define_transform
```

Conceptually:

```{mermaid}
flowchart LR
    Workflow["<b>workflow.yaml</b><br/><code>hep.define</code>"]:::input
    Registry["<b>Registry</b><br/>capability resolution"]:::capability
    Contract["<b>Specification</b><br/>compile-time contract"]:::flow
    Impl["<b>Implementation</b><br/>runtime behaviour"]:::runtime

    Workflow --> Registry
    Registry --> Contract
    Registry --> Impl
```

Here, `fasthep-carpenter` provides the HEP-oriented operation while Flow provides the machinery that compiles and orchestrates it.

See {doc}`registries-and-profiles` for registry composition, layering, and capability resolution.

---

## Contracts separate planning from implementation

For executable operations, a specification describes what Flow needs to know about a capability:

```text
spec
    tells Flow how to reason about the operation

impl
    performs the work
```

This can include:

- accepted parameters
- required and produced fields
- inputs and outputs
- product types
- execution scopes
- dependency information

Flow can therefore validate and plan an operation without understanding its domain-specific implementation.

The operation contract is described in {doc}`operations-and-specs`.

```{note}
The same registry infrastructure is used for extension points outside the data-flow graph, but their exact contracts may differ. For example, compile hooks and execution backends participate at different stages of the lifecycle and expose different information to Flow.
```

---

## Extensions can live outside Flow

Extensions are ordinary Python packages and do not need to live inside `fasthep-flow`.

Conceptually, a package might contain:

```text
my_extension/
├── pyproject.toml
└── src/
    └── my_extension/
        ├── profiles/
        │   └── registry.yaml
        ├── sources/
        ├── transforms/
        ├── sinks/
        └── compile_hooks/
```

Its registry can expose capabilities such as:

```yaml
registry:
  transforms:
    my.calculate:
      spec: my_extension.transforms.calculate:CALCULATE_SPEC
      impl: my_extension.transforms.calculate:run_calculate
```

An workflow can then use:

```yaml
- id: CalculateSomething
  op: my.calculate
```

without Flow itself knowing anything about the package's domain.

This is the same mechanism used across FAST-HEP: Flow provides compiler and runtime contracts, while packages such as `fasthep-carpenter`, `fasthep-curator`, and `fasthep-render` provide specialised capabilities.

```{note}
Analysis repositories can use exactly the same mechanism for capabilities specific to an experiment, collaboration, or individual analysis. An extension does not need to become part of a general-purpose FAST-HEP package.
```

---

## Supporting extension mechanisms

The registry contains some capabilities that do not fit directly into the lifecycle roles above.

These include:

- functions and constants used by expressions
- product handlers that provide runtime behaviour for custom product types
- other supporting runtime hooks

For example, a custom product may require specialised behaviour when partition-level results are merged or materialised. A product handler allows the package defining that product to provide those semantics without teaching Flow about the product itself.

These mechanisms follow the same general principle:

> Flow owns orchestration; extensions provide specialised behaviour behind explicit interfaces.

They will be documented in more detail as their extension APIs stabilise.

---

## Choosing an extension point

A useful starting question is **where the capability needs to participate**:

```text
Does it introduce data?
    → source

Does it compute or transform a workflow product?
    → transform

Does it inspect workflow products or execution?
    → observer

Does it consume products to produce an artifact or external output?
    → sink

Does it inspect or augment compilation?
    → compile hook

Does it modify runtime preparation or execution behaviour?
    → execution modifier

Does it map executable work onto an execution system?
    → backend
```

These boundaries keep scientific or application-level computation separate from compilation and infrastructure concerns.

---

## Learn by building an extension

The FAST-HEP workshop contains runnable examples of external capabilities.

For example, the NASA exoplanet workflow uses a Parquet source provided entirely by `fasthep-workshop`:

```yaml
sources:
  planets:
    kind: workshop.parquet
    stream_type: event_stream
```

The source deliberately lives in the workshop rather than `fasthep-carpenter`.

It demonstrates that an external package can provide a data source through exactly the same registry mechanism used by the rest of the FAST-HEP ecosystem.

The workshop is the appropriate place for step-by-step extension tutorials, while these Flow pages document the contracts those extensions interact with.

---

## Where next?

Start with {doc}`operations-and-specs` for the contract between Flow and executable operations, then {doc}`registries-and-profiles` for how capabilities are assembled into a workflow environment.

The individual extension points are documented in:

- {doc}`sources`
- {doc}`transforms`
- {doc}`observers`
- {doc}`sinks`
- {doc}`compile-hooks`
- {doc}`execution-modifiers`
- {doc}`backends`

```{toctree}
:maxdepth: 1
:hidden:

operations-and-specs
registries-and-profiles
sources
transforms
observers
sinks
execution-modifiers
compile-hooks
backends
```
