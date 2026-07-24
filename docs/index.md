# FAST-HEP Flow

`fasthep-flow`, or **Flow**, is a domain-independent workflow compilation and orchestration framework.

Flow separates three concerns:

* **authoring** — describing what should be computed
* **compilation** — turning that description into an explicit execution plan
* **orchestration** — executing that plan using replaceable capabilities

```{mermaid}
flowchart LR
    Author["workflow description<br/><b>authoring</b>"]
    Compile["compiler<br/><b>normalise and plan</b>"]
    Plan["execution plan"]
    Flow["Flow<br/><b>orchestration</b>"]
    Outputs["products and artifacts"]

    Author --> Compile --> Plan --> Flow --> Outputs
```

The standard FAST-HEP authoring layer uses YAML, but Flow itself operates on the resulting execution plan. Other authoring or compilation tools can therefore produce compatible plans and use the same runtime.

Flow is developed as the orchestration layer of the [FAST-HEP](https://fast-hep.github.io/) toolkit, but its workflow model and runtime are intentionally independent of High Energy Physics.

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
    Description["workflow description"]
    Graph["dependency graph"]
    Plan["execution plan"]
    Runtime["runtime"]

    Description --> Graph --> Plan --> Runtime
```

This explicit representation makes workflows easier to inspect, validate, debug, and execute in different computing environments.

For the user-facing authoring model, see {doc}`authoring/index`.

---

## Replaceable capabilities

Flow does not implement the scientific processing performed by a workflow.

Instead, it orchestrates capabilities provided through explicit contracts.

```{mermaid}
flowchart TD
    Flow["Flow<br/><b>orchestration</b>"]

    Source["source<br/><b>introduce data</b>"]
    Transform["transform<br/><b>process data</b>"]
    Observer["observer<br/><b>inspect execution</b>"]
    Sink["sink<br/><b>produce outputs</b>"]

    Flow --> Source
    Flow --> Transform
    Flow --> Observer
    Flow --> Sink
```

Capabilities can be supplied by FAST-HEP packages, experiments, external projects, or individual analyses.

Flow only needs to understand the contract exposed by a capability, not its internal implementation. This allows implementations to evolve independently as software libraries, data formats, algorithms, and computing hardware change.

For more detail, see {doc}`extending/operations-and-specs` and {doc}`extending/registries-and-profiles`.

---

## Flow in FAST-HEP

Within FAST-HEP, Flow provides the common workflow infrastructure while other packages supply domain-specific capabilities.

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

The core architecture is settling, but parts of the user-facing authoring language and some runtime interfaces are still evolving. In particular, the authoring syntax is expected to become more concise while preserving the underlying workflow model.

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
