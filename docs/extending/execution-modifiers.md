# Execution modifiers

Execution modifiers adapt how a compiled workflow is executed.

Unlike sources, transforms, observers, and sinks, they do not represent operations in the workflow's data-flow graph. Instead, they act on execution behaviour around that graph.

Conceptually:

```text
author workflow
      ↓
 compilation
      ↓
execution plan
      ↓
execution modifiers
      ↓
   runtime
```

This provides an extension point for execution concerns that should not become part of the scientific workflow description.

```{note}
An execution modifier is not a transform.

A transform changes products flowing through the workflow. An execution modifier changes **how the compiled workflow is executed**.
```

Execution modifiers use the same extension infrastructure described in {doc}`operations-and-specs`, but enter the Flow lifecycle at execution rather than as ordinary data-flow operations.

---

## Why execution modifiers?

Some execution behaviour applies to an existing workflow without representing another computational step in that workflow.

For example, execution may need to:

- prepare data for an accelerator
- compile code for a particular device
- initialise runtime-specific state
- adapt products for an execution environment
- perform execution-specific setup or optimisation

Representing these concerns as ordinary transforms would mix execution mechanics with the workflow's computational structure.

Instead:

```text
workflow graph
    describes the computation

execution modifiers
    adapt its execution
```

This allows the same logical workflow to be executed under different runtime conditions without rewriting its analysis stages.

---

## GPU execution

The GPU support provided by `fasthep-carpenter` gives two concrete examples of execution modifiers:

```yaml
execution_modifiers:
  gpu.preload:
    impl: fasthep_carpenter.runtime.modifiers.gpu_preload:GPUPreloadModifier

  cuda.jit:
    impl: fasthep_carpenter.runtime.modifiers.cuda_jit:CUDAJitModifier
```

These capabilities are implemented outside Flow but participate in execution through Flow's modifier interface.

They address two different aspects of GPU execution:

```text
gpu.preload
    prepare or preload data for GPU execution

cuda.jit
    support just-in-time compilation for CUDA execution
```

Neither operation represents another scientific transformation of the data.

Conceptually:

```{mermaid}
flowchart LR
    Plan["compiled plan"]
    Prepare["execution modifiers"]
    Runtime["runtime"]
    GPU["GPU execution"]

    Plan --> Prepare
    Prepare --> Runtime
    Runtime --> GPU
```

The workflow graph remains a description of the computation, while the modifiers provide execution-specific behaviour around it.

---

## Execution concerns stay outside the analysis graph

Consider a transform that can execute on either CPU or GPU.

The scientific workflow might simply contain:

```text
source → transform A → transform B → sink
```

Executing this on a GPU may require additional preparation:

```text
data preparation
device transfer
JIT compilation
runtime setup
```

Those steps are important for execution, but they are not necessarily meaningful parts of the authored analysis.

Without a separate execution extension mechanism, authors could be forced to describe something resembling:

```text
source
  ↓
GPU preload
  ↓
compile CUDA kernel
  ↓
transform A
  ↓
transform B
  ↓
sink
```

That exposes implementation details which may be irrelevant when the same workflow runs on another execution environment.

Execution modifiers allow these concerns to remain separate.

```{note}
Execution modifiers are appropriate for behaviour that changes **how an existing computation is carried out**.

If the operation changes the logical result or introduces a new analysis product, it normally belongs in the workflow graph instead.
```

---

## Modifiers can come from external packages

Flow defines the execution-modifier extension point, but it does not need to own every modifier.

The GPU examples deliberately live in `fasthep-carpenter`:

```text
fasthep-flow
    execution modifier mechanism
            ↓
fasthep-carpenter
    GPU-specific implementations
```

This follows the same package boundary used elsewhere in FAST-HEP.

Flow provides the orchestration contract; packages that understand particular operations, devices, or runtime requirements can provide the corresponding execution behaviour.

Once registered through a profile, those capabilities become available to Flow without adding GPU-specific logic to the core package.

---

## Execution modifiers and backends

Execution modifiers and backends both affect runtime execution, but they solve different problems.

A backend determines **where or through what execution system** the plan runs.

For example:

```text
compiled plan
      ↓
backend
      ↓
local execution
Dask
external workflow system
```

Execution modifiers adapt aspects of **how operations are prepared or executed within that runtime**.

Conceptually:

```text
compiled plan
      ↓
execution modifiers
      ↓
backend / runtime
      ↓
execution infrastructure
```

This separation allows execution-specific capabilities to be composed without making them part of the scientific data-flow graph.

```{note}
The exact boundary between execution modifiers and backends is intentionally narrow and may evolve as Flow's execution interfaces mature.

A useful distinction is that a backend owns the execution strategy, while modifiers provide additional execution behaviour used within that strategy.
```

---

## Execution modifiers are not graph variations

Flow also supports author-level mechanisms that can expand or branch the execution graph, such as variations and context-dependent paths.

Those are part of the workflow language and are described in {doc}`../execution/graph-structure`.

Execution modifiers serve a different purpose:

```text
variations and graph structure
    describe which computations should exist

execution modifiers
    adapt how those computations are executed
```

Keeping these concepts separate prevents runtime implementation details from leaking into the author-facing workflow description.

---

## Extending Flow with execution modifiers

Execution modifiers are registered capabilities and can be supplied by external packages.

For example:

```yaml
execution_modifiers:
  my.modifier:
    impl: my_package.runtime:MyExecutionModifier
```

Once exposed through an active registry/profile, Flow can make the modifier available during execution.

Unlike ordinary data-flow operations, execution modifiers do not need to appear as source, transform, observer, or sink nodes in the workflow graph.

The common extension infrastructure is described in {doc}`operations-and-specs`, while {doc}`registries-and-profiles` explains how packages expose capabilities to Flow.

```{note}
The execution-modifier API is still evolving. The examples here describe the current extension mechanism; the detailed contract will be documented alongside the corresponding extension tutorials as it stabilises.
```

---

## Where next?

Execution modifiers extend runtime behaviour without becoming part of the workflow's scientific data-flow graph.

Continue with {doc}`compile-hooks`, which provide the corresponding extension point on the other side of the execution plan: **during compilation**.

For related concepts:

- {doc}`operations-and-specs` — the common extension model
- {doc}`registries-and-profiles` — making extensions available
- {doc}`../execution/graph-structure` — branches, context and variations
- {doc}`../execution/plan` — the compiled execution representation
- {doc}`../execution/runtime` — executing the plan
- {doc}`../execution/environments` — execution environments
