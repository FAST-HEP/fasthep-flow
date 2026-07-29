# Backends

Backends connect Flow's execution model to computing infrastructure.

After compilation has produced an execution plan, the selected backend is responsible for mapping that plan onto an execution system.

Conceptually:

```text
author.yaml
    ↓
compilation
    ↓
execution plan
    ↓
backend
    ↓
computing infrastructure
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

* `backend` selects the execution implementation
* `strategy` selects how that backend should be used
* `config` provides backend- and strategy-specific configuration

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
    impl: hepflow.backends:Dask
```

The local backend executes a plan directly, while the Dask backend maps execution onto Dask infrastructure.

The important abstraction is not either particular implementation, but the boundary they establish:

```text
Flow execution plan
        ↓
backend interface
        ↓
execution system
```

A backend therefore provides the bridge between Flow's representation of computation and the infrastructure on which that computation runs.

---

## Backends and execution modifiers

Backends and {doc}`execution modifiers <execution-modifiers>` both participate in runtime execution, but have different responsibilities.

```text
execution modifier
    adapts execution behaviour

backend
    maps execution onto infrastructure
```

For example, GPU-specific preparation may be implemented as an execution modifier, while the backend remains responsible for scheduling and executing the resulting work.

Keeping these concerns separate allows execution capabilities to be composed without introducing infrastructure details into the analysis graph.

---

## An evolving extension point

The backend interface is currently under active development.

The existing local and Dask backends live in Flow because their execution models are domain-agnostic. However, the intention is for backends to become fully externalisable capabilities.

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

Once that contract has stabilised, external packages should be able to provide additional execution backends without changes to Flow core.

---

## Where next?

Backends sit at the boundary between Flow's execution model and computing infrastructure.

For related concepts:

* {doc}`../execution/plan` — the execution representation consumed by backends
* {doc}`../execution/runtime` — runtime execution
* {doc}`../execution/environments` — environments in which operations execute
* {doc}`execution-modifiers` — adapting runtime execution behaviour
* {doc}`registries-and-profiles` — selecting capabilities and execution configuration
