# Backends

Backends connect Flow's execution model to computing infrastructure.

After compilation has produced an execution plan, the Flow runtime uses the selected backend to map execution onto an execution system.

Conceptually:

```{mermaid}
flowchart LR
    Workflow["<b>workflow.yaml</b>"]:::input
    Compile["<b>Compilation</b>"]:::flow
    Plan["<b>Execution plan</b>"]:::plan
    Runtime["<b>Flow runtime</b>"]:::runtime
    Backend["<b>Backend</b>"]:::capability
    Infra["<b>Computing<br/>infrastructure</b>"]:::runtime

    Workflow --> Compile --> Plan --> Runtime --> Backend --> Infra
`````

This makes backends different from operations in the data-flow graph. Sources, transforms, and sinks describe computation; a backend determines how that computation is carried out.

---

## Selecting a backend

Execution configuration selects a backend and strategy.

For example, Flow currently provides a local Dask profile:

```yaml
execution:
  backend: dask
  strategy: local
  config:
    workers: 4
    threads_per_worker: 1
    processes: true
    memory_limit: 4GB
    dashboard_address: ":8787"
```

Here:

- `backend` selects the execution implementation
- `strategy` selects an execution strategy supported by that backend
- `config` provides backend- and strategy-specific configuration

This keeps infrastructure configuration separate from the analysis operations themselves.

```{note}
Execution strategies are part of the evolving backend interface. Their configuration and validation are expected to become more structured as the backend API stabilises.
```

---

## Backends in the registry

Backends are resolved through the same registry and profile infrastructure as other Flow extensions.

The built-in registry currently contains:

```yaml
backends:
  local.default:
    impl: hepflow.backends:Local

  dask:
    impl: fasthep_distributed._dask._common:DaskBackend
```

The local backend executes a plan directly. When `fasthep-distributed` is
installed, its Dask backend maps execution onto Dask infrastructure.

The important abstraction is not either particular implementation, but the boundary they establish:

```{mermaid}
flowchart LR
    Plan["<b>Execution plan</b>"]:::plan
    Runtime["<b>Flow runtime</b>"]:::runtime
    Backend["<b>Backend interface</b>"]:::capability
    System["<b>Execution system</b>"]:::runtime

    Plan --> Runtime --> Backend --> System
```

A backend therefore provides the bridge between Flow's representation of computation and the infrastructure on which that computation runs.

---

## Backends and execution modifiers

Backends and {doc}`execution modifiers <execution-modifiers>` both participate in runtime execution, but have different responsibilities.

```{mermaid}
flowchart LR
    Runtime["<b>Flow runtime</b>"]:::runtime
    Backend["<b>Backend</b><br/>maps work onto infrastructure"]:::capability
    Modifier["<b>Execution modifier</b><br/>adapts execution behaviour"]:::capability
    Infra["<b>Execution<br/>infrastructure</b>"]:::runtime

    Runtime --> Backend --> Infra
    Modifier -.-> Runtime
```

For example, GPU-specific preparation may be implemented as an execution modifier, while the backend remains responsible for mapping and submitting the resulting work to the execution system.

Keeping these concerns separate allows execution capabilities to be composed without introducing infrastructure details into the analysis graph.

---

## An evolving extension point

The backend interface is currently under active development.

The local backend lives in Flow because it is the baseline execution mechanism.
External packages provide additional backends through the same registry
mechanism.

This requires a more explicit contract for:

* backend specifications
* supported execution strategies
* strategy-specific configuration
* configuration validation
* mapping plan semantics onto backend capabilities

```{note}
This page describes the current architectural role of backends rather than a stable backend-authoring API.

The backend interface is expected to evolve, including a standard specification mechanism and a structured way for backends to expose execution strategies.
```


---

## Where next?

Backends sit at the boundary between Flow's execution model and computing infrastructure.

For related concepts:

- {doc}`../execution/plan` — the execution representation consumed by the runtime
- {doc}`../execution/runtime` — runtime orchestration
- {doc}`../execution/environments` — mapping execution onto computing resources
- {doc}`execution-modifiers` — adapting runtime execution behaviour
- {doc}`registries-and-profiles` — making backend capabilities available
