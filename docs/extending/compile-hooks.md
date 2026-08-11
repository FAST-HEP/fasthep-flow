# Compile hooks

Compile hooks extend the workflow compilation process.

Unlike sources, transforms, observers, and sinks, they are not runtime operations in the data-flow graph. Instead, they run while Flow turns an authored workflow into an execution plan.

Conceptually:

```{mermaid}
flowchart LR
    Workflow["<b>workflow.yaml</b>"]:::input
    Compile["<b>Compilation</b>"]:::flow
    Plan["<b>Execution plan</b>"]:::plan
    Runtime["<b>Runtime</b>"]:::runtime
    Hook["<b>Compile hooks</b><br/>compile-time extensions"]:::capability

    Workflow --> Compile --> Plan --> Runtime
    Hook -.-> Compile
```

Compile hooks are useful when compilation needs additional information or when another package wants to inspect or augment the compilation process without adding that behaviour to Flow itself.

```{note}
Compile hooks and execution modifiers act on opposite sides of the execution plan.

**Compile hooks** extend how the plan is constructed; **execution modifiers** extend how the resulting computation is executed.
```

---

## Compile hooks in the registry

Compile hooks are registered capabilities.

For example, `fasthep-curator` provides a hook for inspecting ROOT datasets:

```yaml
compile_hooks:
  dataset_metadata.root_tree:
    spec: fasthep_curator.compile_hooks.root_tree_metadata:ROOT_TREE_DATASET_METADATA_SPEC
    impl: fasthep_curator.compile_hooks.root_tree_metadata:inspect_root_tree_datasets
```

and `fasthep-render` provides a hook for rendering the compiled workflow graph with D2:

```yaml
compile_hooks:
  fasthep.render.graph_d2:
    spec: fasthep_render.graph.compile_hooks:GRAPH_D2_RENDER_HOOK_SPEC
    impl: fasthep_render.graph.compile_hooks:render_graph_d2_hook
```

Once made available through an active profile, Flow can invoke these capabilities at the appropriate point during compilation.

Compile hooks therefore use the same registry and profile infrastructure as other Flow extensions.

---

## Enriching compilation with external information

Some information required for planning cannot be determined from the workflow description alone.

For example, a workflow may provide a ROOT file:

```text
dataset
   ↓
ROOT file
```

but information such as the number of entries may only become available by inspecting the file itself.

A compile hook can perform this inspection during compilation:

```text
workflow dataset
      ↓
dataset metadata hook
      ↓
inspect input
      ↓
discovered metadata
      ↓
compiler
      ↓
execution plan
```

The `dataset_metadata.root_tree` hook from `fasthep-curator` follows this pattern.

This allows information discovered from external data to become part of planning without teaching Flow how to inspect ROOT files.

```{note}
This separation is an important package boundary.

Flow can make use of discovered dataset metadata while remaining unaware of ROOT-specific inspection logic. That knowledge belongs to the package providing the compile hook.
```

---

## Producing compilation artifacts

Compile hooks can also use the compiled workflow to produce additional artifacts.

For example, `fasthep-render` can render the workflow graph using D2:

```text
compiled graph
     ↓
D2 compile hook
     ↓
graph representation
     ↓
SVG
```

The resulting graph is useful for inspecting and documenting the workflow, but graph rendering is not itself part of the analysis data-flow graph.

It therefore belongs naturally around compilation rather than as a transform or sink operating on analysis products.

```{note}
Compile hooks are not limited to modifying compilation state.

They can also inspect compilation state and produce diagnostics, visualisations, or other auxiliary outputs from it.
```

---

## Hooks can contribute additional compile outputs

A normal Flow compilation already produces useful intermediate representations such as:

```text
compile/
├── analysis.ir.yaml
├── dataset_entries.json
├── deps.yaml
├── normalized.yaml
├── plan.yaml
└── report.compile.yaml
```

Compile hooks can add information or artifacts alongside these standard outputs.

For example:

```text
compile/
├── ...
└── graph/
    └── graph.svg
```

or contribute metadata that is subsequently reflected in the compiled plan.

This makes the compilation outputs useful not only for Flow itself, but as a record of how the authored workflow was interpreted and prepared for execution.

```{note}
The exact files produced by a compile hook belong to that extension's contract. Flow does not prescribe that every hook must produce an artifact.
```

---

## Compile hooks and reproducibility

Because compile hooks can inspect external state, they can affect the information used to construct a plan.

For example:

```text
workflow.yaml
    +
input-file metadata
    ↓
compiled plan
```

The resulting compile outputs provide a record of the information available when the workflow was compiled.

This is particularly useful when metadata discovered during compilation influences decisions such as partitioning or execution planning.

A compile hook should therefore make planner-relevant results explicit rather than leaving important decisions hidden inside arbitrary side effects.

---

## What belongs in a compile hook?

Compile hooks are a good fit when behaviour needs access to the compilation process but does not belong in Flow core.

Examples include:

- inspecting datasets
- discovering metadata required for planning
- validating external resources
- generating graph representations
- producing compile-time diagnostics
- enriching compile reports

The common question is:

> Does this behaviour operate on the workflow **while it is being compiled**, rather than on its runtime data products?

If so, a compile hook may be the appropriate extension point.

```{note}
If the behaviour inspects products during execution, consider an {doc}`observer <observers>`.

If it consumes a runtime product to produce an output, consider a {doc}`sink <sinks>`.

If it changes how the compiled workflow executes, consider an {doc}`execution modifier <execution-modifiers>`.
```

---

## Extending Flow with compile hooks

A package can register a compile hook through its registry:

```yaml
compile_hooks:
  my.compile_hook:
    spec: my_package.compile_hooks:MY_HOOK_SPEC
    impl: my_package.compile_hooks:run_my_hook
```

The spec describes the configuration and compile-time contract visible to Flow,
while the implementation performs the extension-specific work.

{doc}`registries-and-profiles` describes how compile hooks are registered and
made available to Flow. The detailed compile-hook contract is specific to this
extension point.

Step-by-step implementation examples belong in `fasthep-workshop`.

---

## Where next?

Compile hooks provide the compile-time counterpart to execution modifiers.
The remaining extension point, backends, connects runtime orchestration to execution infrastructure.

Together they enter at different parts of the workflow lifecycle:

```{mermaid}
flowchart LR
    Workflow["<b>Workflow</b>"]:::input
    Compile["<b>Compilation</b>"]:::flow
    Plan["<b>Execution plan</b>"]:::plan
    Runtime["<b>Runtime</b>"]:::runtime

    Hook["<b>Compile hook</b>"]:::capability
    Modifier["<b>Execution modifier</b>"]:::capability

    Source["<b>Source</b>"]:::source
    Transform["<b>Transform</b>"]:::transform
    Observer["<b>Observer</b>"]:::observer
    Sink["<b>Sink</b>"]:::sink

    Workflow --> Compile --> Plan --> Runtime
    Hook -.-> Compile
    Modifier -.-> Runtime

    Source --> Transform --> Sink
    Transform -.-> Observer
```

For related concepts:

- {doc}`operations-and-specs` — the common extension model
- {doc}`registries-and-profiles` — making extensions available
- {doc}`execution-modifiers` — extending runtime execution
- {doc}`../execution/compilation` — the compilation pipeline
- {doc}`../execution/plan` — the resulting execution plan
