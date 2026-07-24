# Execution environments

Flow separates the description of a computation from the environment in which that computation runs.

The execution plan describes nodes, products, partitions, scopes, and dependencies. The execution environment determines how that planned work is mapped onto computing resources.

```{mermaid}
flowchart LR
    Plan["execution<br/>plan"]
    Runtime["Flow<br/>runtime"]
    Backend["execution<br/>backend"]
    Resources["computing<br/>resources"]

    Plan --> Runtime --> Backend --> Resources
```

This separation allows the same workflow structure to be used across different execution environments without embedding infrastructure-specific scheduling logic into analysis operations.

---

## Execution configuration

The requested execution environment is recorded in the plan:

```yaml
execution:
  backend: local
  strategy: default
  profiles: []
  resources: {}
  pools: {}
  environment: {}
  config: {}
```

For the exoplanet example, this selects the default local execution environment.

More complex workflows can provide additional information about:

- backend selection
- execution strategy
- resource requirements
- resource pools
- worker environments
- backend-specific configuration

These settings describe **how the planned computation should be executed**, rather than changing the scientific meaning of the workflow.

---

## Backends

Backends connect the Flow runtime to an execution system.

Like other Flow capabilities, backends are registered rather than hardcoded into workflow semantics.

For example, the exoplanet plan contains:

```yaml
registry:
  backends:
    local.default:
      impl: hepflow.backends:Local

    dask:
      impl: hepflow.backends:Dask
```

The runtime works with Flow concepts such as nodes, products, scopes, and partitions. The backend provides the mechanism for carrying out that work.

Conceptually:

```text
Flow runtime
    decides what work is ready

backend
    determines how that work is executed

operation implementation
    performs the work
```

This distinction allows execution infrastructure to change independently of the operations being orchestrated.

---

## Built-in and external backends

The current local and Dask backend implementations are provided by `fasthep-flow`.

They currently live in Flow because their responsibilities are considered domain-agnostic: they provide general mechanisms for executing planned work rather than HEP-specific analysis behaviour.

This package boundary is not fundamental to the execution model.

Backends are registered capabilities:

```yaml
backends:
  local.default:
    impl: hepflow.backends:Local

  dask:
    impl: hepflow.backends:Dask
```

The runtime therefore does not require backend implementations to come from fasthep-flow itself. An installed package can register additional backends in exactly the same way.

Conceptually:
```
fasthep-flow
├── local backend
└── Dask backend

external package
├── specialised backend
└── site or infrastructure integration
```

This already allows execution integrations to be developed outside Flow without modifying the workflow engine.

The current package boundaries may change as the execution interfaces mature. In particular, specialised or infrastructure-specific backends may be better maintained as independent packages while Flow retains only the domain-agnostic contracts and orchestration machinery.

## Local execution

The simplest environment executes the workflow locally.

For example:

```bash
fasthep run author.yaml
```

with the default execution configuration resolves to the local backend.

Local execution is useful for:

- development
- debugging
- tutorials
- testing
- small workflows

The same compilation and runtime concepts still apply. Sources produce products, transforms consume and produce them, scope transitions are handled by the runtime, and sinks produce final artifacts.

Local execution is therefore not a separate workflow model. It is one way of executing the same plan.

---

## Distributed execution

Partitioned workflows naturally expose work that can be executed concurrently.

Consider a dataset split into several partitions:

```{mermaid}
flowchart TD
    Plan["execution plan"]

    P1["partition 1"]
    P2["partition 2"]
    P3["partition 3"]

    Plan --> P1
    Plan --> P2
    Plan --> P3
```

A distributed backend can map those units of work onto multiple workers.

```{mermaid}
flowchart TD
    Runtime["Flow runtime"]

    Scheduler["distributed scheduler"]

    W1["worker"]
    W2["worker"]
    W3["worker"]

    Runtime --> Scheduler

    Scheduler --> W1
    Scheduler --> W2
    Scheduler --> W3
```

Flow currently provides integration with Dask for distributed execution.

The backend is responsible for interacting with the execution system. Analysis operations should not need to contain Dask-specific scheduling logic simply because they are executed through Dask.

This preserves an important boundary:

```text
workflow semantics
        ≠
scheduler semantics
```

---

## Resources

Some operations require particular computing resources.

Examples may include:

- CPU cores
- memory
- GPUs
- specialised accelerators
- local scratch space

Execution configuration can describe these requirements separately from the analysis operation itself.

Conceptually:

```yaml
execution:
  resources:
    ...
```

The execution environment can then use this information when deciding where work should run.

This becomes particularly important for heterogeneous computing environments where different operations may have different resource requirements.

The exact resource configuration and scheduling behaviour are still evolving and are documented in the reference documentation where available.

---

## Resource pools

A computing environment may contain different groups of resources.

For example:

```text
computing environment
├── general CPU workers
├── high-memory workers
└── GPU workers
```

Flow models these as resource pools where appropriate.

Pools allow execution requirements to be expressed in terms of available classes of resources rather than embedding site-specific worker names or scheduler configuration into analysis operations.

A backend can then map planned work onto the corresponding infrastructure.

This provides a separation between:

```text
"I need this kind of resource"
```

and:

```text
"run this on worker gpu-17.example.org"
```

The first can be part of a portable execution description. The second belongs to the infrastructure managing the resources.

---

## Worker environments

Resources describe **what computing capacity is available**.

Worker environments describe **the software environment in which work executes**.

Depending on the backend, this may involve:

- installed Python environments
- containers
- environment variables
- shared software installations
- accelerator runtimes
- site-specific setup

Execution configuration provides a place to describe or select these environments without requiring operations to configure workers themselves.

```{mermaid}
flowchart LR
    Operation["planned<br/>operation"]
    Requirements["execution<br/>requirements"]
    Backend["backend"]
    Worker["suitable worker<br/>environment"]

    Operation --> Requirements --> Backend --> Worker
```

The exact mechanism depends on the backend and infrastructure.

---

## Credentials

Distributed execution and remote data access may require credentials.

Examples include access to:

- schedulers
- storage systems
- data services
- experiment infrastructure

Flow treats credentials as an execution-environment concern rather than part of scientific workflow semantics.

In particular, sensitive credentials should not need to be embedded directly into portable workflow descriptions or serialised execution plans.

Credential provisioning is generally the responsibility of the execution environment and the services being used.

---

## Strategies

A backend answers the broad question:

> **Which execution system should carry out the work?**

A strategy can refine how that backend is used.

For example, different strategies might make different decisions about:

- partition sizing
- scheduling behaviour
- resource allocation
- worker configuration
- caching
- infrastructure-specific optimisation

The plan represents backend and strategy separately:

```yaml
execution:
  backend: local
  strategy: default
```

This allows execution policy to vary without redefining the workflow graph or its operations.

Strategies and their configuration are still evolving as Flow's execution interfaces mature.

---

## Execution profiles

Execution environments often require several related settings to be activated together.

The execution configuration therefore supports profiles:

```yaml
execution:
  profiles: []
```

Execution profiles can provide reusable configuration for particular environments rather than requiring every workflow to repeat infrastructure details.

Conceptually, an environment might provide a profile for:

```text
local development
```

while another provides one for:

```text
institutional Dask cluster
```

or:

```text
experiment batch infrastructure
```

The workflow can then select an appropriate execution environment without incorporating all of its infrastructure configuration directly.

This follows the same general principle as other Flow profiles: reusable configuration should be composed rather than duplicated.

---

## Execution modifiers

Some differences between computing environments affect how an implementation must be prepared for execution.

For example, accelerator execution may require:

- loading libraries
- initialising device state
- preparing kernels
- runtime compilation

These concerns do not necessarily require a different scientific operation.

Execution modifiers provide an extension point for changing runtime preparation while preserving the operation's workflow semantics.

For example, a registry may provide capabilities such as:

```yaml
execution_modifiers:
  gpu.preload:
    impl: ...

  cuda.jit:
    impl: ...
```

This is particularly useful for experimenting with new computing technologies.

An implementation can evolve from one execution approach to another without requiring those infrastructure details to become part of Flow's core workflow language.

---

## Portable does not mean identical

Separating workflows from execution environments does not imply that every workflow can run unchanged on every possible backend.

An operation may require:

- capabilities unavailable in a particular environment
- specific hardware
- external services
- particular software dependencies

Instead, the goal is to make those requirements explicit and keep them separate from the scientific structure of the workflow where practical.

This allows Flow to determine whether an execution environment can satisfy the requirements of a plan and gives infrastructure tooling a clear place to provide the necessary capabilities.

---

## Infrastructure remains outside Flow

Flow provides the abstraction between workflow execution and infrastructure, but it is not itself a batch system, cluster manager, or resource provisioner.

For example, Flow may interact with an environment backed by:

```text
Dask
  ↓
HTCondor
  ↓
worker nodes
```

The responsibilities remain separate:

```{mermaid}
flowchart TD
    Flow["Flow<br/>workflow orchestration"]
    Dask["distributed execution"]
    Batch["batch / resource management"]
    Workers["compute resources"]

    Flow --> Dask --> Batch --> Workers
```

Flow does not need to reproduce the functionality of those systems.

Instead, backend and environment integrations connect the workflow model to existing execution infrastructure.

---

## Evolving execution infrastructure

One reason for keeping execution infrastructure outside workflow semantics is that computing environments change.

Analyses may need to move between:

- laptops and workstations
- institutional clusters
- batch systems
- distributed schedulers
- CPU and GPU resources
- future accelerator architectures

Libraries and execution frameworks also evolve.

By keeping these concerns behind explicit interfaces, Flow aims to allow new execution approaches to be introduced without requiring the analysis model to be redesigned each time.

This is the same principle used throughout FAST-HEP: components that perform or execute work should be replaceable behind stable contracts where practical.

---

## Where next?

This completes the path from author description to execution environment:

```text
author description
        ↓
compilation
        ↓
execution plan
        ↓
runtime orchestration
        ↓
execution environment
```

For how the workflow is compiled, see {doc}`compilation`.

For the compiler/runtime interface, see {doc}`plan`.

For runtime orchestration, see {doc}`runtime`.

For implementing capabilities that participate in this process, continue with {doc}`../extending/index`.
