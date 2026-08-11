# Sinks

Sinks consume workflow products and turn them into artifacts or other external outputs.

Typical sinks include:

- file writers
- tables
- reports
- console output
- exported datasets

Conceptually:

```{mermaid}
flowchart LR
    Source["<b>Source</b>"]:::source
    Transform1["<b>Transform</b>"]:::transform
    Transform2["<b>Transform</b>"]:::transform
    Sink["<b>Sink</b>"]:::sink
    Artifact["<b>Artifact</b>"]:::artifact

    Source --> Transform1 --> Transform2 --> Sink
    Sink --> Artifact
```

Unlike a transform, a sink normally terminates a path rather than producing another product for downstream analysis operations.

Sinks use the same {doc}`specification and implementation model <operations-and-specs>` as sources, transforms, and observers.
This page focuses on what is distinctive about their role in a workflow.

```{note}
A sink does not have to write a file.

The defining property is that it **consumes a workflow product for an external effect or artifact**, rather than continuing the normal transformation path.
```

---

## Sinks in a workflow

Sinks are commonly attached to the stage whose product they should consume.

For example, the NASA exoplanet workflow produces a small console table:

```yaml
- id: PlanetTable
  op: workshop.tabular.project
  params:
    fields:
      - name
      - planet_name
      - planet_radius
      - planet_period
  write:
    - kind: workshop.console_table
      path: snippets/planets.txt
      when: final
      fields:
        - name
        - planet_name
        - planet_radius
        - planet_period
      columns:
        - template: "{name} {planet_name}"
          header: Planet
        - field: planet_radius
          header: Radius [Earth radii]
          format: ".3f"
        - field: planet_period
          header: Period [days]
          format: ".3f"
      limit: 12
```

The `PlanetTable` transform produces the data; `workshop.console_table` consumes that product and creates the final representation.

Compilation turns this into a separate sink node:

```{mermaid}
flowchart LR
    Filter["<b>stage.EarthSizedPlanets</b>"]:::transform
    Project["<b>stage.PlanetTable</b>"]:::transform
    Table["<b>write.PlanetTable.0</b><br/>workshop.console_table"]:::sink
    Artifact["<b>planets.txt</b>"]:::artifact

    Filter --> Project --> Table --> Artifact
```

The sink is therefore explicit in the execution graph even though the workflow syntax keeps it close to the stage whose output it consumes.

```{note}
Turning a histogram into a PNG file or a D2 graph into an SVG file follows the same principle: the sink consumes a workflow product and outputs one or more artifacts.
```

---

## Writing analysis data

For scientific workflows, one of the most common sinks is a file writer.

For example, a workflow may select events and then persist the resulting stream:

```yaml
- id: SelectedEvents
  op: hep.selection
  params:
    ...

  write:
    - kind: root_tree
      path: selected_events.root
      fields:
        - EventNumber
        - Muon_Pt
        - Muon_Eta
```

Conceptually:

```text
event stream
     ↓
selection
     ↓
selected event stream
     ↓
ROOT writer
     ↓
selected_events.root
```

The same pattern could be used by a Parquet writer or another output format.

The important separation is:

```text
transform
    determines which data are produced

sink
    determines how those data leave the workflow
```

This allows the analysis computation and persistence format to evolve independently.

```{note}
Just as for sources, sink outputs are not restricted to local files and may refer to remote locations. The Flow manifest keeps track of these external outputs.

For integration with file-oriented backends such as Snakemake, it can nevertheless be useful to produce a small local auxiliary file that represents the remote output for artifact tracking.
```

---

## Sinks can use dependency information

Like other data-flow operations, sink specifications can expose which parts of their input they require.

For example:

```yaml
fields:
  - EventNumber
  - Muon_Pt
  - Muon_Eta
```

can contribute to the workflow's field dependencies.

Those requirements can be propagated backwards through the graph:

```text
sink fields
    ↓
transform requirements
    ↓
upstream dependencies
    ↓
source requirements
```

A requested output field may therefore affect what upstream transforms need to produce and ultimately what a source needs to read.

This is another example of why sink configuration participates in compilation rather than being treated as an unrelated post-processing step.

---

## When sinks run

Sinks can consume products at different execution scopes. The workflow expresses
the requested lifecycle through `when`.

For example, an output may be produced for each dataset:

```yaml
when: dataset
```

or only after upstream results have been combined:

```yaml
when: final
```

In the NASA example, the console table is a final output:

```yaml
write:
  - kind: workshop.console_table
    when: final
```

Compilation resolves this workflow-level lifecycle into the corresponding execution scope.

This distinction is important for outputs such as:

```text
per-partition files
per-dataset outputs
final combined tables
final reports
```

The sink contract tells Flow enough about its lifecycle and input requirements to place it appropriately in execution.

---

## Artifacts

A workflow product is a value passed between executable graph nodes. An artifact
is an externally identifiable result of execution.

Persistent outputs produced by sinks can be tracked as workflow artifacts and
participate in the workflow's provenance model.

Artifacts may include files, rendered outputs, exported data, or other
persistent results. Tracking them makes outputs easier to discover, validate,
and associate with the workflow execution that produced them.

```{note}
If another analysis operation needs to consume the result directly as part of
the data-flow graph, a sink is usually not the right abstraction.

A sink represents an output boundary. Use a transform when the result should
continue through the analysis graph.
```

---

## Extending Flow with sinks

Sinks are registered extensions just like sources, transforms, and observers.

Once provided by an active profile, an author can use the registered sink through `write`:

```yaml
write:
  - kind: my.writer
    path: output.dat
```

The common spec/implementation contract is described in {doc}`operations-and-specs`, and {doc}`registries-and-profiles` explains how packages expose capabilities to workflows.

Step-by-step extension examples belong in `fasthep-workshop`.

---

## Where next?

Sinks define output boundaries in the runtime data-flow graph.

The remaining extension mechanisms act at different points in the workflow lifecycle:

- {doc}`compile-hooks` extend the compilation process
- {doc}`execution-modifiers` adapt runtime preparation or execution behaviour

For related concepts:

- {doc}`operations-and-specs` — the common data-flow operation contract
- {doc}`transforms` — product-to-product computation
- {doc}`observers` — inspecting workflow products
- {doc}`../execution/plan` — how sinks appear in the compiled plan