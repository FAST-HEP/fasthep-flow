# FAST-HEP Flow

`fasthep-flow`, or **Flow**, is a domain-independent workflow compilation and orchestration framework.

Flow separates three concerns:

* **authoring** — describing what should be computed
* **compilation** — turning that description into an explicit execution plan
* **orchestration** — executing that plan using replaceable capabilities

```{mermaid}
flowchart LR
    Workflow["<b>workflow.yaml</b><br/>scientific workflow"]:::input
    Compile["<b>Compilation</b><br/>normalise, analyse,<br/>validate, plan"]:::flow
    Plan["<b>Execution plan</b><br/>backend-independent"]:::plan
    Runtime["<b>Runtime</b><br/>orchestration"]:::runtime
    Outputs["<b>Artifacts</b><br/>results + provenance"]:::artifact

    Workflow --> Compile --> Plan --> Runtime --> Outputs
```

The standard FAST-HEP workflow language uses YAML, but Flow's runtime operates
on the resulting execution plan. Alternative workflow languages, frontends, or
compilation tools can therefore produce compatible plans and use the same
runtime.

Flow is developed as the workflow compiler and runtime of the [FAST-HEP](https://fast-hep.github.io/)  toolkit, but its workflow model, compiler, and runtime are
intentionally independent of High Energy Physics.

---

(declarative-workflows)=

## Declarative workflows

A declarative workflow describes **what should happen** without prescribing every implementation or execution detail.

A workflow can describe:

* data entering the workflow
* operations and their configuration
* dependencies between products
* requested outputs
* execution requirements

Flow compiles this description into an explicit graph and execution plan before runtime execution begins.

```{mermaid}
flowchart LR
    Workflow["<b>workflow.yaml</b><br/>workflow description"]:::input
    Normal["<b>Normalised workflow</b>"]:::flow
    Graph["<b>Logical graph</b>"]:::flow
    Plan["<b>Execution plan</b>"]:::plan
    Runtime["<b>Runtime</b>"]:::runtime

    Workflow --> Normal --> Graph --> Plan --> Runtime
```

This explicit representation makes workflows easier to inspect, validate, debug, and execute in different computing environments.

For the user-facing workflow model, see {doc}`authoring/index`.

---

## Replaceable capabilities

Flow does not implement the scientific processing performed by a workflow.

Instead, it orchestrates capabilities provided through explicit contracts.

```{mermaid}
flowchart LR
    Source["<b>Source</b><br/>introduce data"]:::source
    Transform["<b>Transform</b><br/>compute products"]:::transform
    Observer["<b>Observer</b><br/>inspect"]:::observer
    Sink["<b>Sink</b><br/>produce artifacts"]:::sink

    Source --> Transform --> Sink
    Transform -.-> Observer
```

Capabilities can be supplied by FAST-HEP packages, experiments, external projects, or individual analyses.

Flow reasons about capabilities through their registered contracts rather than their internal implementations.
This allows implementations to evolve independently as software libraries, data formats, algorithms, and computing hardware change.

For more detail, see {doc}`extending/operations-and-specs` and {doc}`extending/registries-and-profiles`.

---

## Flow in FAST-HEP

Flow provides the common workflow compilation, planning, and runtime infrastructure while other packages supply domain-specific capabilities.

For example:

* `fasthep-carpenter` provides HEP data-processing operations
* `fasthep-curator` provides metadata, provenance, and diagnostics
* `fasthep-render` provides plotting and reporting capabilities
* `fasthep-cli` provides the user-facing command-line interface

These packages use the same extension mechanisms available to third-party and analysis-specific packages.

For an overview of the complete toolkit, see the [FAST-HEP documentation](https://fast-hep.github.io/).

---

## Where to start

If you are new to Flow:

1. Start with {doc}`getting-started/index` to run a small workflow.
2. Read {doc}`authoring/index` to understand the authoring model.
3. Continue with {doc}`execution/index` to see how workflows are compiled and executed.
4. See {doc}`extending/index` if you want to provide your own operations or runtime capabilities.

Runnable HEP examples and step-by-step extension tutorials are available in the [FAST-HEP workshop](https://fasthep-workshop.readthedocs.io/en/latest/).

---

## Project status

Flow is under active development and has not yet reached its first stable release.

The core architecture is settling, but parts of the workflow language and some
runtime interfaces are still evolving. In particular, the workflow syntax is
expected to become more concise while preserving the underlying workflow model.

Current behaviour and interfaces should therefore be checked against the reference documentation for the version being used.

```{toctree}
:maxdepth: 1
:hidden:

getting-started/index
authoring/index
execution/index
extending/index
reference/index
developer/index
```
