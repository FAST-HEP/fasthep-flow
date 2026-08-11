# Graph structure

A workflow description may look like a sequence of analysis stages, but its compiled structure is a graph.

Operations can share upstream computation, branch into independent paths, participate only in particular contexts, and be expanded into variations without requiring the author to manually duplicate complete workflows.

```{mermaid}
flowchart LR
    Source["<b>Source</b>"]:::source
    Common["<b>Common processing</b>"]:::transform
    A["<b>Path A</b>"]:::transform
    B["<b>Path B</b>"]:::transform
    C["<b>Path C</b>"]:::transform

    Source --> Common
    Common --> A
    Common --> B
    Common --> C
```

Flow constructs this structure during compilation using several kinds of information:

- **dependencies** determine what connects to what
- **context** determines where operations participate
- **variations** describe controlled alternatives to parts of the workflow

The workflow describes the intended computation; the compiler turns that description into an explicit graph.

---

## Workflows are not necessarily linear

A simple workflow may form a linear chain:

```{mermaid}
flowchart LR
    Source["<b>Source</b>"]:::source
    Define["<b>Derive fields</b>"]:::transform
    Select["<b>Select rows</b>"]:::transform
    Hist["<b>Summarise</b>"]:::transform

    Source --> Define --> Select --> Hist
```

but this is only one possible graph shape.

The output of an operation may be consumed by several downstream operations:

```{mermaid}
flowchart LR
    Source["<b>Source</b>"]:::source
    Common["<b>Common processing</b>"]:::transform
    A["<b>Analysis path A</b>"]:::transform
    B["<b>Analysis path B</b>"]:::transform

    Source --> Common
    Common --> A
    Common --> B
```

Flow represents these relationships explicitly in the logical graph and execution plan.

This allows common processing to be shared while downstream parts of the workflow evolve independently.

---

## Branching

Branching occurs when a workflow develops multiple downstream paths from shared upstream computation.

Branches can arise naturally from data dependencies, and stages can also be restricted to particular workflow contexts.

For example, consider a workflow containing both data and simulated datasets:

```yaml
data:
  datasets:
    - {name: data, eventtype: data, files: [toy://data]}
    - {name: ttbar, eventtype: mc, files: [toy://ttbar]}
```

Both datasets pass through a common reconstruction stage:

```yaml
- id: MuonColumns
  op: hep.define
  params:
    variables:
      - name: Muon_Pt
        expr: "sqrt(Muon_Px ** 2 + Muon_Py ** 2)"
```

and can contribute to the corresponding histogram.

A second path uses generator-level information that only exists for simulated events:

```yaml
- id: MCLeptonColumns
  op: hep.define
  applies_to:
    eventtype: mc
  params:
    variables:
      - name: MCLepton_Pt
        expr: "sqrt(MCLepton_Px ** 2 + MCLepton_Py ** 2)"

- id: MCLeptonPt
  op: hep.hist
  applies_to:
    eventtype: mc
  params:
    axes:
      - name: MCLepton_Pt
        source: MCLepton_Pt
        type: regular
        bins: {low: 0, high: 180, nbins: 60}
```

The compiled graph contains shared processing followed by two downstream paths:

```{mermaid}
flowchart TD
    Read["<b>read.events</b>"]:::source
    Muons["<b>MuonColumns</b>"]:::transform

    MuonPt["<b>MuonPt</b><br/>histogram"]:::transform
    MuonRender["<b>MuonPt</b><br/>render"]:::sink

    MCColumns["<b>MCLeptonColumns</b><br/>MC only"]:::transform
    MCLeptonPt["<b>MCLeptonPt</b><br/>MC only"]:::transform
    MCRender["<b>MCLeptonPt</b><br/>render"]:::sink

    Read --> Muons

    Muons --> MuonPt
    MuonPt --> MuonRender

    Muons --> MCColumns
    MCColumns --> MCLeptonPt
    MCLeptonPt --> MCRender
```

The graph shows the available data-flow paths. Context determines which of those paths participate for a particular dataset.

Conceptually:

```text
data
  └── MuonColumns
        └── MuonPt

MC
  └── MuonColumns
        ├── MuonPt
        └── MCLeptonColumns
              └── MCLeptonPt
```

This is useful for workflows containing:

- signal and control regions
- data- and simulation-specific processing
- several selections derived from a common preselection
- multiple summaries of the same data
- feature preparation and machine-learning paths
- validation and diagnostic branches

Branching is therefore not a special execution mode. It follows naturally from the graph constructed from workflow dependencies and context.

---

## Dependencies and context determine the graph

The compiled graph is determined by more than the order in which stages appear in the workflow description.

Flow combines information from:

- operation inputs and outputs
- field-level dependencies exposed through operation specifications
- dataset and workflow context
- conditions such as `applies_to`

to determine which operations participate and how they are connected in the logical graph.

An important distinction is:

> `applies_to` determines whether an operation participates in a context; its operation specification describes what the operation requires when it does participate.

For example, an expression such as:

```text
sqrt(Muon_Px ** 2 + Muon_Py ** 2)
```

introduces dependencies on:

```text
Muon_Px
Muon_Py
```

If another operation consumes the resulting `Muon_Pt`, the compiler can connect those requirements through the workflow graph.

The same dependency information is propagated back to data sources. By default, sources therefore need to read only the fields required by the compiled workflow.

In the MC-specific branch above, generator-level fields are required only by operations that participate for simulated datasets.

This makes conditional workflow structure part of the declarative description rather than requiring conditional control flow inside operation implementations.

---

## Conditional participation

`applies_to` provides the current workflow-level mechanism for restricting an operation to particular contexts.

For example:

```yaml
applies_to:
  eventtype: mc
```

restricts a stage to datasets whose context identifies them as simulated data.

This keeps contextual decisions outside the operation implementation itself.

Rather than writing an operation that internally behaves like:

```text
if this is simulation:
    perform generator-level calculation
```

the workflow declares that the operation belongs only to that context.

The operation can therefore remain focused on performing its computation, while Flow is responsible for deciding where it participates in the workflow.

`applies_to` currently provides the basic mechanism for conditional participation. More expressive controls may be added as the workflow language evolves.

---

## Shared computation

Branching makes common computation explicit.

Consider two analysis paths that require the same initial transformation:

```text
read
  ↓
common transformation
  ├── path A
  └── path B
```

The workflow does not need to describe two independent copies of the common transformation merely because the workflow later diverges.

The graph can represent the common node once and connect multiple consumers to its output.

This becomes increasingly useful as workflows grow beyond simple linear analyses.

---

## Variations

Variations describe controlled changes to an otherwise shared workflow.

A variation might change:

- a weight
- a field used by downstream operations
- a dataset
- another declared part of the computation

For example:

```text
nominal workflow
      │
      ├── weight variation
      │     TriggerEffWeight
      │       → TriggerEffWeight_up
      │
      ├── field variation
      │     Muon_Pt
      │       → Muon_Pt_scale_up
      │
      └── dataset variation
            dataset
              → alternative dataset
```

The important idea is that a variation describes **what changes**, rather than requiring the author to reproduce the complete workflow containing that change.

Flow can use this information during compilation to construct the corresponding variant execution paths.

---

### Weight variations

A weight variation changes how downstream weighted computations are evaluated.

Conceptually:

```text
nominal:
    EventWeight

variation:
    EventWeight × TriggerEffWeight_up
```

The analysis stages that consume the weight can then be evaluated for the variation without requiring separate copies of their workflow-level definitions.

Uncertainty calculations are one important use case, but the underlying mechanism is simply a controlled modification to the inputs of downstream computation.

---

### Field variations

A variation can replace one field with another.

For example:

```yaml
replace:
  Muon_Pt: Muon_Pt_scale_up
```

Downstream consumers that normally use:

```text
Muon_Pt
```

can instead use:

```text
Muon_Pt_scale_up
```

for that variation.

Because Flow has planner-visible dependency information, the compiler can reason about which parts of the workflow depend on the affected field.

```{mermaid}
flowchart LR
    Common["<b>Common processing</b>"]:::transform

    Nominal["<b>Muon_Pt</b><br/>nominal"]:::transform
    Varied["<b>Muon_Pt_scale_up</b><br/>variation"]:::transform

    HistN["<b>Downstream computation</b><br/>nominal"]:::transform
    HistV["<b>Downstream computation</b><br/>varied"]:::transform

    Common --> Nominal --> HistN
    Common --> Varied --> HistV
```

Unrelated parts of the workflow do not conceptually need to become separate workflow-level-description workflows simply because one field has changed.

The exact execution structure and opportunities for sharing work are determined during compilation and planning.

---

### Dataset variations

Variations can also replace input datasets.

For example:

```yaml
datasets:
  replace:
    toy: toy_alt
```

This describes a workflow variant in which one dataset is replaced by another while retaining the same downstream analysis definition.

Conceptually:

```text
              ┌── nominal dataset
analysis  ←───┤
              └── alternative dataset
```

This is useful whenever the same computation should be evaluated against alternative inputs.

Again, the workflow describes the difference rather than duplicating the complete analysis.

---

### Variations can be selective

A variation does not necessarily apply to every dataset or every operation.

For example, a variation may apply only to simulated data:

```yaml
applies_to: mc
```

or to selected datasets:

```yaml
applies_to:
  datasets: [toy]
```

A variation may also require a particular stage:

```yaml
requires:
  - stage.TriggerEfficiencyWeights
```

These constraints give the compiler additional information about where the variation participates in the workflow.

This follows the same broad principle as conditional stages: context and dependencies determine where a declared capability is relevant.

---

### Dependencies and variations work together

Dependency information makes variations substantially more useful.

Suppose a workflow contains two downstream paths:

```{mermaid}
flowchart TD
    Source["<b>Source</b>"]:::source
    Define["<b>Derive fields</b>"]:::transform

    Muon["<b>Uses Muon_Pt</b>"]:::transform
    Other["<b>Does not use Muon_Pt</b>"]:::transform

    Source --> Define
    Define --> Muon
    Define --> Other
```

A variation affecting `Muon_Pt` is relevant to the first path but not inherently to the second.

Flow can use planner-visible dependency information to determine which parts of the workflow are affected by the declared change.

Conceptually:

```text
shared computation
      │
      ├── unaffected path
      │
      └── affected path
             ├── nominal
             └── variation
```

How much work can actually be shared at runtime depends on the operation contracts, product semantics, and resulting execution plan.

---

## One description, many workflow paths

Dependencies, context, and variations play different but complementary roles:

```text
dependencies
    determine what connects to what

context
    determines where operations participate

variations
    describe controlled alternatives
```

Together they allow a relatively compact workflow description to represent a much richer execution graph.

For example:

```{mermaid}
flowchart TD
    Common["<b>Shared processing</b>"]:::transform

    Data["<b>Data path</b>"]:::transform
    MC["<b>Simulation path</b>"]:::transform

    Nominal["<b>Nominal</b>"]:::transform
    Up["<b>Variation up</b>"]:::transform
    Down["<b>Variation down</b>"]:::transform

    Common --> Data
    Common --> MC

    MC --> Nominal
    MC --> Up
    MC --> Down
```

This becomes particularly useful for workflows containing:

- multiple analysis regions
- conditional processing
- alternative datasets
- systematic variations
- training and inference paths
- validation workflows
- many output products

The workflow describes the structure and differences that matter. Flow expands those declarations into an explicit graph that can be inspected and executed.

---

## The compiled graph remains inspectable

Concise authoring does not mean that the resulting computation is hidden.

Flow exposes compilation artifacts describing the graph, dependencies, and execution plan produced from the workflow.

This means a workflow can use something conceptually compact:

```text
shared workflow
+ conditional branch
+ variations
```

while still inspecting the concrete computation Flow intends to execute.

This distinction is intentional:

```text
workflow.yaml
    concise workflow description

compiled graph and plan
    explicit representation of the resulting computation
```

This is particularly useful when a small declaration expands into several execution paths.

---

## Graph expansion happens during compilation

Conditional participation and variations are resolved as part of workflow compilation.

The runtime should not need to reinterpret the high-level workflow declarations or independently reconstruct their intended graph structure.

Instead:

```{mermaid}
flowchart LR
    Workflow["<b>workflow.yaml</b><br/>workflow description"]:::input
    Compiler["<b>Compilation</b><br/>construct + resolve"]:::flow
    Graph["<b>Resolved graph structure</b>"]:::flow
    Plan["<b>Execution plan</b>"]:::plan
    Runtime["<b>Flow runtime</b>"]:::runtime

    Workflow --> Compiler --> Graph --> Plan --> Runtime
```

By the time execution begins, the relevant structure is represented explicitly in the plan.

This preserves the boundary used throughout Flow:

> compilation determines the computation; runtime orchestrates the resulting plan.

---

## Syntax and examples

This page describes graph structure as a Flow concept rather than providing the complete workflow-language syntax for branching, conditional participation, or variations.

The workflow language is still evolving, including work towards more compact ways of expressing workflows.

The reference documentation will provide the authoritative syntax as these interfaces stabilise.

Runnable examples belong in the FAST-HEP workshop, where the workflow description can be explored together with its compiled graph, execution plan, and outputs.

---

## Where next?

For how workflow descriptions become an execution graph, see {doc}`compilation`.

For the explicit representation consumed by the runtime, see {doc}`plan`.

For how nodes and products are executed, see {doc}`runtime`.

For runnable examples of conditional paths and variations, see the FAST-HEP workshop.
