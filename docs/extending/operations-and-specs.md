# Operations and specifications

Operations are the executable building blocks of a Flow workflow.

Sources, transforms, observers, and sinks have different roles in the workflow graph, but share a common principle:

```{mermaid}
flowchart LR
    Capability["<b>Registered capability</b>"]:::capability
    Spec["<b>Specification</b><br/>compile-time contract"]:::flow
    Impl["<b>Implementation</b><br/>runtime behaviour"]:::runtime

    Capability --> Spec
    Capability --> Impl
```

The **specification**, or **spec**, exposes the contract Flow needs to understand an operation before it runs.

The **implementation**, or **impl**, provides the code that performs the actual work.

Together they separate planning semantics from runtime behaviour, allowing Flow to reason about a workflow without executing arbitrary operation code.

---

## The operation contract

An operation has two audiences:

```text
compiler
    needs to understand the operation

runtime
    needs to execute the operation
```

For example, an operation may:

- consume an event stream
- require particular fields
- produce new fields
- return a different product type
- require a particular execution scope

Flow needs this information while compiling the workflow.

The implementation, meanwhile, needs to know how to perform the actual computation.

A registry therefore connects an operation name to both parts:

```yaml
transforms:
  hep.define:
    spec: fasthep_carpenter.operations.define:DEFINE_SPEC
    impl: fasthep_carpenter.operations.define:run_define_transform
```

When the workflow contains:

```yaml
- id: BasicVars
  op: hep.define
  params:
    variables:
      - name: Muon_Pt
        expr: "sqrt(Muon_Px ** 2 + Muon_Py ** 2)"
```

the registry gives Flow the corresponding contract and implementation:

```{mermaid}
flowchart LR
    Operation["<b>hep.define</b><br/>requested capability"]:::input
    Registry["<b>Registry</b><br/>resolution"]:::capability

    Spec["<b>DEFINE_SPEC</b><br/>compile-time contract"]:::flow
    Compiler["<b>Compiler</b>"]:::flow

    Impl["<b>run_define_transform</b><br/>runtime behaviour"]:::runtime
    Runtime["<b>Runtime</b>"]:::runtime

    Operation --> Registry
    Registry --> Spec --> Compiler
    Registry --> Impl --> Runtime
```

The compiler uses the spec while constructing the workflow. The runtime later invokes the implementation described by the resulting plan.

---

## Specs expose planner-visible semantics

A spec describes behaviour that matters to Flow, not the algorithm used to implement it.

For example, the `hep.define` specification is:

```python
DEFINE_SPEC = {
    "name": "hep.define",
    "kind": "transform",
    "version": "1.0",
    "input": {"name": "stream", "kind": "event_stream", "required": True},
    "params": {"variables": {"type": "list[mapping]", "required": True}},
    "result": {
        "kind": "event_stream",
        "description": "Event stream with newly defined fields.",
    },
    "requires": {
        "symbols": [
            {"from": "params.variables.*.expr", "kind": "expr"},
            {"from": "params.variables.*.reduce.over", "kind": "expr_or_field"},
        ]
    },
    "provides": {
        "symbols": [
            {"from": "params.variables.*.name", "kind": "field_list"},
        ]
    },
}
```

This exposes several aspects of the operation that matter during compilation:

| Spec entry | Meaning to Flow |
|---|---|
| `kind` | this capability is a transform |
| `input` | it consumes an `event_stream` |
| `params` | `variables` is a required list of mappings |
| `result` | it produces an `event_stream` |
| `requires.symbols` | configured expressions and fields introduce dependencies |
| `provides.symbols` | configured variable names introduce new fields |

Importantly, the spec also describes **where Flow should look inside operation-specific parameters** for planner-visible information.

Consider:

```yaml
params:
  variables:
    - name: Muon_Pt
      expr: "sqrt(Muon_Px ** 2 + Muon_Py ** 2)"
```

The spec maps:

```text
params.variables.*.expr
    ↓
expression
    ↓
Muon_Px
Muon_Py
    ↓
required fields
```

while:

```text
params.variables.*.name
    ↓
Muon_Pt
    ↓
provided field
```

Flow can therefore derive:

```text
requires
    Muon_Px
    Muon_Py

provides
    Muon_Pt
```

without knowing how `hep.define` evaluates the expression at runtime.

The implementation remains free to choose the appropriate array library, expression evaluator, or other computational machinery.

```{note}
Operation-specific syntax can remain inside `params`.

The spec tells Flow which values represent expressions, fields, products, or other planner-visible information.
An operation-specific concept does not need to become part of the core workflow language merely so that the compiler can reason about it.
```

## Parameter-derived requirements and outputs

Specs can often express parameter-derived dependencies declaratively, without requiring operation-specific compiler logic.

Many operations use compact workflow parameters but still need explicit compiled
dependencies. Flow provides reusable `requires.symbols` and `provides.symbols`
rules for common parameter-derived patterns, so operation modules do not need
custom dependency parsers for ordinary field expansion.

Useful requirement rules include:

| Rule | Use |
|---|---|
| `field_list` | A parameter already names one or more complete fields. |
| `field_prefix` with `suffixes` | One or more collection prefixes require fixed field suffixes, such as `eta` and `phi`. |
| `field_prefix` with `suffixes_from` | A collection prefix requires fields listed in another parameter, such as `params.keep`. |
| `relative_expr` | Expressions are written relative to one collection and their symbols should be prefixed with `params.collection`. |
| `scoped_expr` | Expressions use an operation-defined namespace, such as `object_1_pt` and `object_2_pt`, with the spec declaring how symbols map to source fields. |

Useful output rules include:

| Rule | Use |
|---|---|
| `field_list` | A parameter names complete output fields. |
| `field_prefix` with `suffixes_from` | An output collection prefix provides fields from another parameter. |
| `count` | A collection prefix provides the deterministic count field `n<prefix>`. |
| `template` | A deterministic output name is composed from parameters. |

For example, an object-selection operation can expose compact workflow params:

```yaml
params:
  collection: Muon
  output: selected_tight_Muon
  selection:
    - pt >= 20
  keep:
    - pt
    - eta
```

with a declarative spec contract:

```python
"requires": {
    "symbols": [
        {
            "from": "params.collection",
            "kind": "field_prefix",
            "suffixes_from": "params.keep",
        },
        {
            "from": "params.selection",
            "kind": "relative_expr",
            "prefix_from": "params.collection",
        },
    ]
},
"provides": {
    "symbols": [
        {
            "from": "params.output",
            "kind": "field_prefix",
            "suffixes_from": "params.keep",
        },
        {"from": "params.output", "kind": "count"},
    ]
},
```

Flow then derives `Muon_pt`, `Muon_eta`, `selected_tight_Muon_pt`,
`selected_tight_Muon_eta`, and `nselected_tight_Muon`.

For pair builders or similar operations with multiple expression contexts,
the spec can declare scoped symbols:

```python
{
    "from": "params.selection.pair",
    "kind": "scoped_expr",
    "symbol_prefixes": ["object_1_", "object_2_"],
    "prefixes_from": "params.collections",
}
```

This keeps the runtime focused on array operations while Flow handles
expression inspection, symbol validation, and graph-visible dependencies.
Use a custom `dependency_parser` only when the contract cannot be expressed
with these reusable rules.

---

## Specs drive dependency inference

Planner-visible requirements can be propagated through the workflow.

Suppose one operation produces:

```text
Muon_Pt
```

from:

```text
Muon_Px
Muon_Py
```

and a downstream operation consumes `Muon_Pt`.

Flow can reason backwards:

```{mermaid}
flowchart LR
    Source["<b>Source</b><br/>Muon_Px, Muon_Py"]:::source
    Define["<b>Define</b><br/>Muon_Pt"]:::transform
    Consumer["<b>Consumer</b><br/>Muon_Pt"]:::transform

    Source --> Define --> Consumer
```

and determine that the source ultimately needs to provide:

```text
Muon_Px
Muon_Py
```

This is what allows Flow to request only the source fields required by the compiled workflow.

Field aliases declared through `fields` participate in the same dependency reasoning.

The same mechanism also applies when an operation-specific parameter contains references that are not part of Flow's core syntax. The operation spec exposes those references to the compiler and turns them into graph or field dependencies as appropriate.

---

## Operations communicate through products

Dependencies are not limited to fields.

Operations communicate through named **products**.

For example:

```text
event_stream
     ↓
 transform
     ↓
event_stream
```

or:

```text
event_stream
     ↓
 histogram operation
     ↓
 histogram
     ↓
   sink
```

The operation contract describes these interfaces so that Flow can construct explicit graph connections between compatible nodes.

The resulting execution plan records those connections as named inputs and outputs.

---

## Scope is part of the contract

Products may exist at different execution scopes.

An event-processing transform may operate independently on partitions:

```text
partition
    ↓
transform
    ↓
partition
```

while another operation may require a dataset-level or global product.

Flow uses operation and product contracts to plan the required scope transitions so that implementations receive products at the scope they expect.

The implementation can therefore remain focused on its local computation rather than orchestrating the surrounding workflow.

See {doc}`../execution/plan` and {doc}`../execution/runtime` for the execution model.

---

## Implementations receive planned work

By the time an implementation runs, Flow has already resolved much of the surrounding structure:

```text
workflow.yaml
     ↓
operation names + parameters
     ↓
registry resolution
     ↓
operation contracts
     ↓
dependency and scope reasoning
     ↓
execution plan
     ↓
implementation
```

The implementation generally does not need to rediscover the workflow around it.

Its responsibility is local: perform the work represented by the planned node according to its contract.

This boundary makes implementations easier to replace and allows the compiler to inspect workflows without executing them.

```{note}
An alternative implementation may use a different library, algorithm, or execution technology.

As long as it satisfies the contract Flow relies upon, those implementation choices do not need to become part of the workflow language.
```

---

## Operation roles

The common operation model is specialised by the role an operation plays in the graph:

| Role | Contract in the graph |
|---|---|
| {doc}`sources` | external data → product |
| {doc}`transforms` | product → product |
| {doc}`observers` | inspect products or execution without becoming part of the main transformation path |
| {doc}`sinks` | product → external output or artifact |

The role-specific pages focus on what is distinctive about each operation rather than repeating the common spec/implementation model described here.

---

## Specs outside data-flow operations

The same architectural idea can also apply outside ordinary runtime nodes.

For example, compile hooks currently expose both a spec and implementation:

```yaml
compile_hooks:
  fasthep.render.graph_d2:
    spec: fasthep_render.graph.compile_hooks:GRAPH_D2_RENDER_HOOK_SPEC
    impl: fasthep_render.graph.compile_hooks:render_graph_d2_hook
```

The contract is different because the capability participates during compilation rather than in the runtime data-flow graph, but the same separation remains useful:

```text
declarative contract
    tells Flow what it needs to know

implementation
    provides the specialised behaviour
```

Other extension points are still evolving.

Execution modifiers are currently registered by implementation:

```yaml
execution_modifiers:
  gpu.preload:
    impl: fasthep_carpenter.runtime.modifiers.gpu_preload:GPUPreloadModifier
```

and backends similarly expose their implementations:

```yaml
backends:
  local.default:
    impl: hepflow.backends:Local

  dask:
    impl: hepflow.backends:Dask
```

Their contracts may gain richer planner-visible specifications as those interfaces stabilise.

In particular, backend specifications are expected to provide a structured way to describe supported execution strategies and their configuration.

```{note}
The mature operation-spec model described on this page applies primarily to executable workflow operations.

Flow's other extension points use the same registry/profile architecture, but their declarative contracts are still being developed where needed.

The goal is the same: make behaviour that Flow must reason about explicit rather than hiding it inside implementation code.
```

See {doc}`compile-hooks`, {doc}`execution-modifiers`, and {doc}`backends` for their respective lifecycle roles.

---

## Specs make compilation inspectable

Because Flow has a machine-readable description of operation behaviour, it can inspect a workflow before processing the full dataset.

Compilation can determine information such as:

- which implementation was resolved
- which fields operations require and produce
- how nodes are connected
- which source fields are required
- which products cross execution scopes

These decisions can be recorded in compilation outputs such as the dependency description and execution plan.

The spec is therefore not merely input validation. It is part of the information from which Flow constructs its interpretation of the workflow description.
A spec is therefore more than input validation: it is part of the compiler-visible semantics of an operation.

---

## The exact spec API

This page describes the role of operation specifications rather than every field of the current spec objects.

The spec API is still evolving as Flow's contracts become more expressive.

Reference documentation should ultimately be generated from the implementation itself so that:

- available spec fields
- accepted values
- defaults
- validation rules

remain synchronised with the installed version of Flow.

These conceptual pages can then remain focused on what the contract means and why it exists.

---

## Where next?

See {doc}`registries-and-profiles` for how specs and implementations are made available to workflows.

For role-specific contracts:

- {doc}`sources`
- {doc}`transforms`
- {doc}`observers`
- {doc}`sinks`

Other extension points are described in:

- {doc}`compile-hooks`
- {doc}`execution-modifiers`
- {doc}`backends`

For step-by-step examples of implementing custom capabilities, see the FAST-HEP workshop.
