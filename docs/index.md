# FAST-HEP Flow

## Introduction

`fasthep-flow` is a workflow compilation and orchestration framework for describing data analysis workflows in YAML and converting them into serialisable execution graphs.

Users describe *what* should be computed using {ref}`declarative-workflows`, while `fasthep-flow` determines *how* workflows should be validated, normalised, planned, and executed.

```{mermaid}
flowchart TD

    subgraph Compile["Compilation and planning"]
        Author["author.yaml"]
        Profiles["profiles and registries"]
        Normalised["normalised workflow"]
        Dependency["dependency inference"]
        Plan["execution plan"]

        Author --> Normalised
        Profiles --> Normalised
        Normalised --> Dependency
        Dependency --> Plan
    end

    subgraph Execute["Runtime execution"]
        Runtime["runtime execution"]
        Outputs["artifacts and outputs"]

        Runtime --> Outputs
    end

    Plan --> Runtime
```

These execution graphs can be:

- executed locally
- distributed with systems such as [Dask](https://www.dask.org/)
- evaluated step-wise with workflow managers such as [Snakemake](https://snakemake.readthedocs.io/en/stable/)
- lowered into alternative execution and optimisation backends

While designed as the orchestration layer of the [FAST-HEP](https://fast-hep.github.io/) ecosystem, `fasthep-flow` is **intentionally domain-agnostic** and can be used independently of High Energy Physics workflows.

---

(declarative-workflows)=
## Declarative workflows

`fasthep-flow`describe intent rather than implementation details.

Rather than explicitly writing event loops, scheduling logic, or task orchestration code, users describe:

- data sources
- transformations
- histogramming
- rendering
- outputs
- dependencies

The workflow engine then infers execution order and dependencies automatically.

This allows workflows to scale from:

- small local analyses
- interactive prototyping
- distributed cluster execution
- reproducible production workflows

while preserving the same high-level workflow description.

For more information on the language, please see {doc}`overview/workflow-language`
---

## FAST-HEP ecosystem

`fasthep-flow` acts as the orchestration and compilation layer for the broader FAST-HEP ecosystem.

Typical workflows combine:

| Package | Purpose |
|---|---|
| `fasthep-flow` | workflow compilation and orchestration |
| `fasthep-carpenter` | transforms, histogramming, awkward/ROOT processing |
| `fasthep-curator` | metadata, diagnostics, provenance |
| `fasthep-render` | rendering, plots, reports |
| `fasthep-cli` | command-line interface |
| `fasthep-workshop` | tutorials and example workflows |

```{note}
Despite the `hep` in its name, `fasthep-flow` aims to remain domain-agnostic.

The workflow language, planning system, runtime abstraction, and execution model are intentionally designed to be reusable outside High Energy Physics workflows.
```

---

## Current status

`fasthep-flow` is under active development.

The workflow language and architecture are stabilising rapidly, but some APIs and runtime interfaces may still evolve before the first stable release.

The current focus areas include:

- workflow language refinement
- execution planning
- runtime backends
- extensibility APIs
- inspection and validation tooling
- tutorial and workshop infrastructure

---


```{toctree}
:maxdepth: 1
:hidden:
overview/index
extensibility/index
custom-operations/index
developer-architecture/index
reference/index
```
