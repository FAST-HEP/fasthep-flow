# Observers

Observers inspect workflow products at selected points without becoming part of the main transformation path.

They are intended for capabilities such as:

- diagnostics
- schema inspection
- validation
- monitoring
- metadata collection

Conceptually:

```text
source → transform → transform → sink
   ↓           ↓
observer    observer
```

The main data flow continues independently of the observer.

Observers use the same {doc}`specification and implementation model <operations-and-specs>` as other Flow extensions. This page focuses on what is distinctive about their role in a workflow.

```{note}
The distinction between an observer and a transform is semantic.

A transform participates in the data-flow chain; an observer instead inspects an existing point.
```

---

## Observers in an author workflow

Observers are declared separately from analysis stages.

For example, a workshop workflow can request schema snapshots with:

```yaml
observers:
  - kind: hep.schema_snapshot
    at:
      - read.events
      - stage.TriggerSelection
    out: schema
```

This asks the observer to inspect the workflow at two points:

```{mermaid}
flowchart LR
    Source["read.events"]
    Selection["stage.TriggerSelection"]
    Next["downstream operations"]

    Source --> Selection
    Selection --> Next

    Source -.-> Before["schema snapshot"]
    Selection -.-> After["schema snapshot"]
```

The `at` parameter identifies the compiled workflow points where the observer should run. The same observer can therefore inspect a product at several stages of its evolution.

In this example, comparing the snapshots before and after `TriggerSelection` makes changes to the event stream visible without modifying the analysis path itself.

---

## Diagnostics and metadata

Observers can produce information about the products or execution points they inspect.

A schema observer, for example, may record:

- field names
- field types
- array structure

Other observers might collect:

- event or object counts
- data-quality information
- validation results
- runtime measurements
- diagnostic summaries
- metadata

These results can contribute to reports or diagnostic artifacts without becoming ordinary analysis products consumed by downstream transforms.

This makes observers particularly useful for optional information that should **accompany an analysis rather than become part of the analysis data itself**.

```{note}
If downstream analysis operations need to consume the result as part of their computation, an observer is probably not the right abstraction. A transform or another product-producing operation is usually a better fit.
```

---

## Extending Flow with observers

Observers are registered extensions just like sources and transforms.

Once an observer has been provided by an active profile, an author can use its registered name:

```yaml
observers:
  - kind: my.observer
    at:
      - stage.SomeStage
```

The common spec/implementation contract is described in {doc}`operations-and-specs`, and {doc}`registries-and-profiles` explains how packages expose capabilities to workflows.

Step-by-step extension examples belong in `fasthep-workshop`.

---

## Where next?

Observers inspect workflow products without becoming part of the main transformation path.

Continue with {doc}`sinks`, which consume workflow products and turn them into persistent artifacts or other terminal outputs.

For related concepts:

- {doc}`operations-and-specs` — the common extension contract
- {doc}`transforms` — product-to-product computation
- {doc}`registries-and-profiles` — making extensions available