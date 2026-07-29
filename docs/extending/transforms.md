# Transforms

Transforms consume workflow products, perform computation, and produce products for downstream operations.

They form the main computational paths through a workflow:

```text
input product
     ↓
  transform
     ↓
output product
```

Examples include:

- defining derived fields
- filtering records
- selecting objects
- restructuring data
- calculating weights
- constructing higher-level objects
- producing histograms or other analysis products

Transforms use the common {doc}`operation contract <operations-and-specs>`. This page focuses on what is distinctive about their role in the workflow.

---

## Transforms in an author workflow

Transforms normally appear as stages under `analysis.stages`.

For example, the NASA exoplanet workflow contains:

```yaml
analysis:
  stages:
    - id: EarthSizedPlanets
      op: workshop.tabular.filter
      params:
        expr: "(planet_radius > 0.8) & (planet_radius < 1.2)"
```

Here:

```text
EarthSizedPlanets
    stage identifier

workshop.tabular.filter
    registered transform capability

params
    configuration understood by that transform
```

Compilation turns the stage into a transform node:

```{mermaid}
flowchart LR
    Previous["stage.PlanetRows"]
    Filter["stage.EarthSizedPlanets<br/><b>transform</b>"]
    Next["stage.PlanetTable"]

    Previous -->|stream| Filter
    Filter -->|stream| Next
```

The execution plan contains the explicit product connections even though the author description remains compact.

---

## Transforms operate on products

A transform does not fundamentally operate on "the workflow". It consumes one or more products and produces one or more products.

A transform may preserve the product type:

```text
event_stream
     ↓
 transform
     ↓
event_stream
```

change it:

```text
event_stream
     ↓
 histogram operation
     ↓
 histogram
```

or combine several inputs:

```text
product A ─┐
           ├─→ transform → product C
product B ─┘
```

The operation contract exposes these relationships to Flow so that compilation can construct the corresponding graph.

This product-oriented model allows transforms to compose without Flow needing to understand their scientific meaning.

---

## Transform-specific configuration belongs in `params`

Operation-specific configuration lives under `params`.

For example:

```yaml
- id: BasicVars
  op: hep.define
  params:
    variables:
      - name: Muon_Pt
        expr: "sqrt(Muon_Px ** 2 + Muon_Py ** 2)"
```

`variables` and `expr` belong to the `hep.define` contract. They are not keywords in Flow's core author language.

Conceptually:

```text
Flow syntax
    id
    op
    params
        ↓
operation-specific syntax
```

A different transform can therefore expose a completely different parameter vocabulary without extending the core workflow language.

The operation spec makes planner-relevant parts of those parameters visible during compilation. For the `hep.define` example above, Flow can infer that:

```text
requires
    Muon_Px
    Muon_Py

provides
    Muon_Pt
```

See {doc}`operations-and-specs` for how specs map operation-specific parameters into planner-visible requirements and products.

```{note}
Keeping operation-specific concepts inside `params` is an important extension boundary.

A concept does not need to become part of Flow's author syntax merely because the compiler needs to reason about it. The operation spec can expose the relevant semantics instead.
```

---

## Transforms can change the contents or shape of a product

Some transforms add fields.

For example:

```yaml
- id: BasicVars
  op: hep.define
  params:
    variables:
      - name: Muon_Pt
        expr: "sqrt(Muon_Px ** 2 + Muon_Py ** 2)"
```

Conceptually:

```text
input
├── Muon_Px
└── Muon_Py
      │
      ↓
   hep.define
      │
      ↓
output
├── Muon_Px
├── Muon_Py
└── Muon_Pt
```

Other transforms restructure the product.

The NASA example uses three small transforms:

```yaml
- id: PlanetRows
  op: workshop.tabular.explode
  params:
    fields:
      - planet_name
      - planet_radius
      - planet_period
    keep_fields:
      - name

- id: EarthSizedPlanets
  op: workshop.tabular.filter
  params:
    expr: "(planet_radius > 0.8) & (planet_radius < 1.2)"

- id: PlanetTable
  op: workshop.tabular.project
  params:
    fields:
      - name
      - planet_name
      - planet_radius
      - planet_period
```

Together they form:

```text
planet records
     ↓
   explode
     ↓
planet rows
     ↓
   filter
     ↓
selected rows
     ↓
   project
     ↓
table-ready rows
```

A transform may also produce a completely different product type, such as turning an event stream into a histogram.

What makes all of these transforms is not the particular computation, but their position in the data-flow graph:

> they consume workflow products and produce workflow products.

---

## Transform dependencies shape the graph

Analysis stages do not have to form one linear chain.

A product can feed several downstream transforms:

```{mermaid}
flowchart TD
    Read["read.events"]
    Common["stage.MuonColumns"]
    Hist["stage.MuonPt"]
    MC["stage.MCLeptonColumns"]
    MCHist["stage.MCLeptonPt"]

    Read --> Common
    Common --> Hist
    Common --> MC
    MC --> MCHist
```

This can represent structures such as:

- control regions
- alternative analysis paths
- diagnostic branches
- feature preparation
- machine-learning training paths

The important point is that these paths arise from **product and field dependencies**.

The author describes what each operation needs and produces; compilation turns those relationships into graph connections.

```{note}
The order in which stages appear in `author.yaml` should not be confused with manually scheduling an execution sequence.

The compiled graph describes the actual dependencies between operations.
```

---

## Flow context can restrict transforms

Some aspects of a stage belong to Flow rather than the transform itself.

For example:

```yaml
- id: MCLeptonColumns
  op: hep.define
  applies_to:
    eventtype: mc
  params:
    variables:
      - name: MCLepton_Pt
        expr: "sqrt(MCLepton_Px ** 2 + MCLepton_Py ** 2)"
```

Here:

```text
applies_to
    Flow-level workflow semantics

params
    hep.define-specific semantics
```

`hep.define` does not need to understand the meaning of an MC dataset. Flow uses workflow context to determine where the stage applies.

For a workflow containing both data and MC, the resulting paths may therefore differ:

```{mermaid}
flowchart TD
    Common["stage.MuonColumns"]

    Data["data path"]
    MC["MC path"]

    MuonPt["stage.MuonPt"]
    MCLeptons["stage.MCLeptonColumns"]
    MCLeptonPt["stage.MCLeptonPt"]

    Common --> Data
    Common --> MC

    Data --> MuonPt
    MC --> MuonPt
    MC --> MCLeptons
    MCLeptons --> MCLeptonPt
```

This allows one authored workflow to describe context-dependent graph structure without embedding those rules inside individual transform implementations.

See {doc}`../execution/graph-structure` for conditional paths and other graph-level workflow structure.

---

## Providing a transform

Transforms can be supplied entirely by external packages.

For example:

```text
fasthep-carpenter
    HEP analysis transforms

fasthep-workshop
    tutorial and example transforms

experiment packages
    experiment-specific transforms

analysis packages
    analysis-specific transforms
```

Once registered through an active profile, Flow can compile these capabilities in the same way.

Flow therefore provides the orchestration mechanism without needing to become a catalogue of scientific operations.

The common operation contract is described in {doc}`operations-and-specs`, and {doc}`registries-and-profiles` explains how transform capabilities become available to a workflow.

Step-by-step implementation examples belong in `fasthep-workshop`.

---

## Where next?

Transforms form the computational paths through a workflow.

Continue with:

- {doc}`observers` for inspecting workflow products without joining the transformation path
- {doc}`sinks` for consuming products at output boundaries

For related concepts:

- {doc}`operations-and-specs` — the common operation contract
- {doc}`registries-and-profiles` — making transform capabilities available
- {doc}`../execution/graph-structure` — branching, conditional paths, and variations
- {doc}`../execution/plan` — transform nodes after compilation
