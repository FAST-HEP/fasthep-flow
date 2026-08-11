# Getting started

Flow is a workflow compiler and orchestrator. It does not provide the data-processing capabilities used by a workflow itself.

Instead, capabilities such as data sources, transformations, and outputs are provided by extension packages and connected through Flow's standard contracts.

This example uses a small set of teaching capabilities from `fasthep-workshop` to analyse a catalogue of NASA exoplanets.

By the end, you will have:

- run a declarative workflow
- produced a table of Earth-sized exoplanets
- seen how external capabilities are used by Flow
- inspected the workflow that Flow executes

```{note}
This example deliberately uses non-HEP data.

Flow is developed as part of FAST-HEP, but its workflow model and orchestration are domain-independent.
```

## The example

The example uses a small NASA exoplanet catalogue stored as Parquet data.

We will select planets whose measured radius satisfies:

{math}`0.8 < R_\mathrm{planet} / R_\mathrm{Earth} < 1.2`

and print a table containing their names, radii, and orbital periods.

The complete example lives in the FAST-HEP workshop:

`examples/NASA/exoplanets/`

The workshop provides both the example workflow and the capabilities it uses.

## Get the workshop

Clone the workshop repository:

```console
git clone https://github.com/FAST-HEP/fasthep-workshop.git
cd fasthep-workshop
```

The workshop uses [Pixi](https://pixi.sh/) to provide a reproducible environment containing Flow and the other packages required by its examples.

Install the environment:

```console
pixi install
```

## Run the workflow

From the workshop repository, run:

```console
pixi run --environment dev fasthep run examples/NASA/exoplanets/workflow.yaml \
    --outdir build/examples/NASA/exoplanets
```

The workflow reads the exoplanet catalogue, selects planets between 0.8 and 1.2 Earth radii, and writes a small table.

You should see output similar to:

```text
+---------------+----------------------+---------------+
| Planet        | Radius [Earth radii] | Period [days] |
+---------------+----------------------+---------------+
| Kepler-950 b  | 0.801                | 98.718        |
| Kepler-51 c   | 0.803                | 85.312        |
| Kepler-817 b  | 0.806                | 3.990         |
| Kepler-46 b   | 0.808                | 33.601        |
| SWEEPS-4 b    | 0.810                | 4.200         |
| K2-289 b      | 0.812                | 13.157        |
| K2-139 b      | 0.813                | 28.381        |
| Kepler-702 b  | 0.818                | 10.526        |
| Kepler-1654 b | 0.819                | 1047.836      |
| Kepler-695 b  | 0.822                | 3.040         |
| Kepler-9 c    | 0.823                | 38.910        |
| HAT-P-38 b    | 0.825                | 4.640         |
+---------------+----------------------+---------------+
```

The generated artifacts and workflow information are written beneath:

```text
build/examples/NASA/exoplanets/
```

## Look at the workflow

The workflow itself is described in:

```text
examples/NASA/exoplanets/workflow.yaml
```

Rather than containing Python implementations, it describes the capabilities that should be connected together.

For example, the data enter through a registered source:

```yaml
sources:
  planets:
    kind: workshop.parquet
    stream_type: event_stream
```

and the radius selection is another registered operation:

```yaml
- id: EarthSizedPlanets
  op: workshop.tabular.filter
  params:
    expr: "(planet_radius > 0.8) & (planet_radius < 1.2)"
```

The workflow describes **what should happen**. It does not contain the implementation of a Parquet reader or filtering operation.

## Where do the operations come from?

Near the beginning of `workflow.yaml`, the workflow activates profiles:

```yaml
use:
  profiles:
    - registry
    - fasthep_workshop:registry
```

The workshop registry provides the capabilities used by this example.

Conceptually, the workflow is:

```{mermaid}
flowchart LR
    Read["workshop.parquet"]
    Explode["workshop.tabular.explode"]
    Filter["workshop.tabular.filter"]
    Project["workshop.tabular.project"]
    Write["workshop.console_table"]

    Read --> Explode --> Filter --> Project --> Write
```

None of these data-processing operations are implemented by Flow itself.

The workshop provides the Parquet reader, tabular operations, and table writer through Flow's standard extension contracts. Flow resolves those capabilities, constructs the workflow, determines how they are connected, and orchestrates their execution.

Put another way:

> Flow does not need to know what an exoplanet is, how Parquet data are read, or how the table is formatted.

It needs to understand the contracts exposed by the registered capabilities.

## What Flow provides

This distinction is central to the architecture.

| Flow | Workshop extension |
|---|---|
| workflow processing | Parquet source |
| profile and registry resolution | tabular transforms |
| dependency graph construction | console-table sink |
| execution planning | operation implementations |
| runtime orchestration | data-processing behaviour |

The workshop capabilities are intentionally small teaching implementations. They use the same extension mechanisms available to analysis packages, experiments, and other third-party projects.

## Inspecting the workflow

Flow constructs an explicit workflow graph before executing the operations.

The exoplanet workflow produces the following graph:

```{image} ../_static/images/nasa_workflow.svg
:alt: Compiled Flow graph for the NASA exoplanet example
:align: center
:width: 60%
```

This is the workflow Flow derived from the declarative description. Each node represents a registered capability, while the edges describe how products move between them.

The graph is constructed before the scientific operations are executed. Flow uses the specifications associated with each capability to determine how the operations connect and to construct the execution plan.

This explicit representation provides a foundation for:

* validation
* workflow inspection
* debugging
* provenance
* alternative execution environments

We will look more closely at how the workflow description becomes this graph and execution plan in {doc}`../execution/index`.

## Where next?

This example deliberately treats the workshop operations as building blocks without explaining their implementation.

Continue with:

- {doc}`../authoring/index` to understand how workflows are described
- {doc}`../execution/index` to see how descriptions become executable plans
- {doc}`../extending/index` to learn how capabilities such as `workshop.parquet` are provided

The [FAST-HEP workshop](https://fasthep-workshop.readthedocs.io/en/latest/) contains runnable examples and step-by-step tutorials covering more complete analysis workflows.
