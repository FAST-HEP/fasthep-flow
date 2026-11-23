# Workflow compilation

Flow compiles a workflow description into an explicit, backend-independent
execution plan before runtime processing begins.

Compilation progressively assembles the workflow, constructs its logical graph,
resolves capabilities and dependencies, analyses datasets, and creates the
representation consumed by the runtime.

A typical compilation produces:

```text
compile/
├── analysis.ir.yaml
├── dataset_entries.json
├── deps.yaml
├── normalized.yaml
├── plan.yaml
└── report.compile.yaml
```

These artifacts expose different representations and analyses produced during
compilation.

```{mermaid}
flowchart LR
    Workflow["<b>workflow.yaml</b>"]:::input
    Normalised["<b>Normalised<br/>workflow</b>"]:::flow
    Graph["<b>Logical<br/>graph</b>"]:::flow
    Analysis["<b>Compiler analysis</b><br/>dependencies + datasets"]:::flow
    Plan["<b>Execution<br/>plan</b>"]:::plan

    Workflow --> Normalised --> Graph --> Analysis --> Plan
```

The files are useful representations of the compilation process, but should
not be interpreted as a guarantee that Flow will always internally consist of
exactly these sequential compiler passes.

The important boundary is the result: compilation turns a user-facing workflow
description into a resolved plan that the runtime can execute without
interpreting the original workflow language.

---

## Normalisation

The first major step is to assemble the workflow description into a canonical
representation.

Consider the beginning of the exoplanet workflow:

```yaml
version: 1.0

use:
  profiles:
    - registry
    - fasthep_workshop:registry

data:
  datasets:
    - name: nasa_exoplanets
      files: [data/NASA/exoplanets.parquet]
      eventtype: data

sources:
  planets:
    kind: workshop.parquet
    stream_type: event_stream
```

The workflow description deliberately leaves some information implicit.

During normalisation, Flow assembles conveniences such as:

- defaults
- included configuration
- references
- profile selections
- execution defaults

The resulting `normalized.yaml` is a canonical description from which later
compiler stages can construct and analyse the workflow.

This distinction is useful:

```text
workflow.yaml
    user-facing description

normalized.yaml
    canonical assembled description
```

Normalisation is still part of compilation. Scientific operations are not
executed simply to establish this representation.

It provides an important boundary: the workflow language can evolve and offer
different conveniences without requiring later compiler stages or
the runtime to understand every form directly.

---

## Building the logical graph

The normalised workflow is lowered into an explicit logical graph.

This representation makes the computational and data-flow structure of the
workflow explicit and is currently written to:

```text
compile/analysis.ir.yaml
```

At this point, author-facing concepts become graph nodes with roles such as:

* sources
* transforms
* observers
* sinks


A sequence such as:

```yaml
analysis:
  stages:
    - id: PlanetRows
      op: workshop.tabular.explode

    - id: EarthSizedPlanets
      op: workshop.tabular.filter

    - id: PlanetTable
      op: workshop.tabular.project
```

becomes part of an explicit dependency graph:

```{mermaid}
flowchart LR
    Source["<b>read.planets</b>"]:::source
    Explode["<b>stage.PlanetRows</b>"]:::transform
    Filter["<b>stage.EarthSizedPlanets</b>"]:::transform
    Project["<b>stage.PlanetTable</b>"]:::transform
    Write["<b>write.PlanetTable.0</b>"]:::sink

    Source --> Explode --> Filter --> Project --> Write
```

The logical graph provides a common structure for subsequent compiler stages regardless of how the workflow was originally authored.

For most users, the details of the intermediate representation are less important than the graph it describes. It is primarily useful for inspecting the compiler and for tooling built around Flow.

---

## Specifications participate in compilation

Knowing that two operations are connected is not sufficient to build an execution plan.

Flow also needs to understand what each capability requires and produces.

This information is exposed through **specifications**.

For example, the exoplanet workflow contains:

```yaml
- id: EarthSizedPlanets
  op: workshop.tabular.filter
  params:
    expr: "(planet_radius > 0.8) & (planet_radius < 1.2)"
```

The specification for `workshop.tabular.filter` allows the compiler to inspect this configuration and determine that the expression requires:

```text
planet_radius
```

The compiler can perform this reasoning without executing the filter implementation itself.

This distinction is fundamental:

```text
spec
    tells the compiler how to reason about a capability

impl
    performs the work at runtime
```

Specifications expose the parts of an operation's contract that the compiler needs to reason about. Depending on the capability, they may describe inputs, outputs, products, scopes, partitioning behaviour, or other properties required for planning.

The precise operation contracts are described in {doc}`../extending/operations-and-specs`.

---

## Dependency analysis

Flow combines requirements from operations across the workflow.

The resulting field-level dependency information is exposed in:

```text
compile/deps.yaml
```

For example:

```yaml
consumers:
  planet_radius:
    - stage.PlanetRows
    - stage.EarthSizedPlanets
    - stage.PlanetTable
    - write.PlanetTable.0
```

This shows the nodes whose specifications require `planet_radius`.

The compiler then determines which source must ultimately provide the field:

```yaml
required_sources:
  planets:
    data:
      - name
      - planet_name
      - planet_period
      - planet_radius
```

The resulting chain is approximately:

```{mermaid}
flowchart LR
    Params["<b>Operation parameters</b>"]:::capability
    Spec["<b>Operation spec</b><br/>requirements"]:::flow
    Requirement["<b>Field requirements</b>"]:::flow
    Source["<b>Source projection</b><br/>required fields"]:::source

    Params --> Spec --> Requirement --> Source
```

Requirements from multiple downstream consumers are combined and propagated towards their data sources.

---

## Source projections become explicit

The original workflow description of the source did not list any columns:

```yaml
sources:
  planets:
    kind: workshop.parquet
    stream_type: event_stream
```

After dependency analysis, the source node in `plan.yaml` contains:

```yaml
- id: read.planets
  role: source
  impl: workshop.parquet
  params:
    branches:
      - name
      - planet_name
      - planet_period
      - planet_radius
```

The compiler has transformed downstream requirements into an explicit request to the source implementation.

This enables source implementations to avoid reading unused fields where the underlying data format supports selective access.

Flow determines **which fields are required**. The source implementation determines **how those fields are read**.

That distinction keeps data-format-specific behaviour outside the orchestration layer.

---

## Dataset analysis and partitioning

Dependency analysis establishes what data are required. Dataset analysis establishes what data are available and provides the metadata needed to plan their execution.

Dataset information becomes part of the plan context:

```yaml
context:
  datasets:
    nasa_exoplanets:
      name: nasa_exoplanets
      files:
        - data/NASA/exoplanets.parquet
      eventtype: data
      group: nasa_exoplanets
```

Flow then constructs the partitions available for execution.

When event counts are available, a dataset may choose its own chunk size:

```yaml
data:
  datasets:
    - name: nasa_exoplanets
      files: [data/NASA/exoplanets.parquet]
      nevents: 1200000
      chunk_size: 250000
```

The effective chunk size is the first value set by the command line, the
dataset, or `execution.partitioning.chunk_size`, in that order. With none of
those set, Flow keeps its existing whole-file partitioning. A multi-file
dataset needs one `file_nevents` value per file before Flow can split the files;
otherwise it reports the missing counts instead of applying the dataset total
to each file.

`fasthep run workflow.yaml --list-partitions` reports the effective chunk size
and partition count for each dataset as JSON without compiling or running the
workflow. Add `--dataset NAME` to inspect one dataset and `--chunk-size N` to
preview a command-line override.

### Per-dataset mode

A production over many datasets can use one workflow while each job runs one
dataset. The mode is opt-in, either in the workflow or on the command line
(`--per-dataset`, which wins over the workflow value):

```yaml
execution:
  partitioning:
    per_dataset: true
```

In this mode `--dataset NAME` (also `compile`, `make-plan` and `run-plan`)
selects one dataset:

- Only the selected dataset keeps its file data. The other datasets stay as
  metadata (`name`, `group`, `eventtype`, `meta`, `chunk_size`), so applicability
  and data flow see every dataset but only the selected one is partitioned. With a
  dataset-replacing systematic variation the replacement files are resolved for
  the selected dataset only.
- Partition numbers are 1-based inside the dataset, and `--partition` counts
  inside it. `<dataset>/<N>.<ext>` is the name of a partition writer's file.
  Without `--dataset`, `--partition` on a workflow with several datasets is
  rejected as ambiguous; `--dataset NAME` alone runs all partitions of that
  dataset.
- `--dataset` without the mode is an error, an unknown name lists the names of
  the workflow, and an out-of-range partition names the dataset, its chunk size
  and the valid range.
- `run-plan` accepts `--chunk-size` only if it equals the value the plan was
  compiled with.
- Run-level work cannot be shared by concurrent jobs: observers and `run_end`
  sinks are rejected at compile time (aggregation belongs to the merge step), and
  a run does not reset the shared histogram and cutflow manifests.

Workflows without the mode keep their plan, partition numbers (`<file>_<chunk>`)
and output names.

#### Shared plan and dataset units

Jobs for many datasets compile into one output root, so a per-dataset compile
writes shared files and one unit per dataset instead of one `compile/plan.yaml`
with every partition:

```text
compile/[<variation>/]
├── plan.yaml                  shared: metadata-only datasets, partitions: []
├── deps.yaml, graph, render specs, worker_environment.json   shared
└── datasets/<name>/           unit: complete once manifest.yaml exists
    ├── manifest.yaml          files, event counts, counted flags, count origins, chunk size, record hash
    ├── normalized.yaml        this dataset's files only
    └── analysis.ir.yaml, dataset_entries.json, report.compile.yaml,
        invocation.yaml, inputs.manifest.yaml, hook artifacts, inputs/
```

- Shared files are published create-if-absent (a temporary file, `fsync`, then
  `os.link`): the first compile wins, a later one with identical content does
  nothing, and different content fails and names the file and both hashes.
  Compiling the same dataset again with the same manifest changes nothing; a
  different chunk size or file list fails and keeps the existing unit.
  A unit is built in a staging directory beside its place, claimed by an exclusive `mkdir` (a
  directory rename onto an existing one is not refused on EOS) and filled with `manifest.yaml`
  moved in last, so a reader never sees a complete-looking partial unit. A unit directory without
  `manifest.yaml` is waited for briefly (a compile of the same dataset may be filling it) and
  then, if it never appears, removed and redone, and only that unit.
- The shared plan depends on what the workflow is, not on where it was compiled from: it holds no
  `workflow_path`, `project_root`, provenance root, input files or host paths, and carries
  `context.workflow_hash`. The same workflow addressed by another path publishes the same files; a
  changed workflow conflicts. Each unit records the location in `invocation.yaml`, and
  `load_plan_file(plan, dataset=D)` puts it back into the context.
- `plan.yaml` names the unit as `execution.partitioning.manifest:
  datasets/{dataset}/manifest.yaml`. The manifest freezes what partitioning needs,
  so listing, compile and run use the same partitions.
- `load_plan_file(plan_file, dataset=NAME)` assembles the plan with the partitions
  of that unit; without `dataset` it loads every compiled unit. `run-plan` does
  this, and its `--chunk-size` check compares with the manifest.
- A compile without `--dataset` in this mode writes a unit for each dataset.
  `normalized.yaml` of the compile root is not written.

#### Run roots, bindings and partition records

A per-dataset run does not write one set of records for the whole out-dir. Each partition keeps its
own below a run root, so concurrent jobs of one out-dir share no record file:

```text
runs/[<variation>/]<dataset>/
├── binding.json                  variation, plan_hash, manifest_hash, chunk_size
└── <N>/
    ├── run_summary.yaml          written last and atomically: present means the partition completed
    ├── provenance/               manifest.json, execution.json, records/
    ├── writers/<writer>/manifest.json
    └── attempts/<UTC second>-<4 hex>/attempt.json
artifacts/files/<stem>/<dataset>/<N>.<ext>   data files, as before
```

- **Binding:** the first job creates `binding.json` (create-if-absent) and every later job checks its
  own plan and manifest against it before it writes anything. `plan_hash` is the whole-plan hash of
  `compiler.bundle.plan_hash` (the shared plan holds no root), `manifest_hash` the `record_hash` of the
  unit's manifest. A changed plan, manifest or chunk size fails with the names of the fields that
  differ; recompile into a new out-dir. A replacement variation has its own unit and run root.
- **Partition summary:** `variation`, `dataset`, `partition` (N), `partitions` (K of the dataset),
  `chunk_size`, `plan_hash`, `manifest_hash` and the `attempt` id, next to the usual run summary.
  An invocation that selects several partitions (`--dataset D`, or `--partition 1,3`) still writes one
  summary per partition, because the partitions run one after the other, each through the backend.
- **Attempts:** each run of a partition claims a new attempt directory. A rerun with the same binding
  replaces `run_summary.yaml` and the data file (the last complete attempt wins); a failed partition
  leaves no summary. A partition writer fills `<N>.tmp-<attempt>.<ext>` and Flow renames it once the
  node succeeded, so no attempt leaves a torn file.
- Concurrent compiles of the same dataset are safe (see above).

#### Compile hooks and event counts in a unit

- A phase compile hook runs once per unit, with a context whose `datasets` holds only the
  selected dataset (the others are stubs without files, which a counting hook would record as
  empty). Its artifacts are written into the unit as `datasets/<name>/<artifact>.json`, so two
  datasets never share a path. `hook_scope` (`compiler/hooks/registry.py`) says where a hook
  runs: a parameter hook is plan scope (metadata only), a phase hook dataset scope.
- A hook that produces the `dataset_metadata` artifact can supply the per-file event counts a
  dataset lacks. The unit writer maps its per-file entries (`files: {<path>: {entries: N}}`, the
  curator hook needs `add_per_file: true`) onto `file_nevents` by file path: a file without a
  declared count takes the hook's count, a declared count stays (a different hook count warns),
  and a file the hook did not count stays uncounted (it becomes a whole-file partition and a warning
  says how many). `manifest.yaml` records where each count
  came from in `count_origin` (`declared`, `hook` or null).
- The plan `compile_workflow_file` returns is the one a job loads: its partitions come from the published
  manifests, hook counts included. `list_workflow_partitions` never opens files, so a dataset whose counts
  come from a hook is listed only after `compile`; the error says so. Phase hooks of both phases
  (`after_datasets`, `after_compile`) see the selected dataset only.
- With such a hook configured, the chunk-count check (and the whole-file warning) moves from the
  plan build to the unit writer, after the hook ran, and its message names the hook and the
  `add_per_file` option. Without one nothing changes.
- A recompile builds its manifest from declared data only and compares it with the existing one
  without the counts a hook added, so an identical recompile does not open the files again. A
  changed file list, declared count or chunk size conflicts as before. Manifest `version` is now
  2 (a unit compiled by an earlier version is refused: recompile into a new out-dir).
- Not covered yet: a unit's compile-hook output written by another dataset.

For the small exoplanet dataset, the plan contains a single partition:

```yaml
partitions:
  - id: planets__nasa_exoplanets__0
    dataset: nasa_exoplanets
    file: data/NASA/exoplanets.parquet
    source: planets
    part: '0_0'
    start: null
    stop: null
```

Larger workflows may contain many datasets, files, and partitions.

The distinction is:

```text
dataset
    logical collection described by the analysis

file
    physical input belonging to a dataset

partition
    unit of data made available for execution
```

Compilation constructs the units of work available to the runtime. How those partitions are scheduled and processed belongs to runtime execution.

Resolved dataset information is also written separately to `dataset_entries.json` for inspection.

---

## Resolving capabilities

Compilation resolves the capabilities required by the logical graph against the
workflow's registry.

Profiles contribute registry layers and configuration. Together these layers
determine the specifications available to the compiler and the implementations
that can later be invoked by the runtime.

For example:

```yaml
registry:
  sources:
    workshop.parquet:
      spec: fasthep_workshop.sources.parquet:PARQUET_SOURCE_SPEC
      impl: fasthep_workshop.sources.parquet:run_parquet_source

  transforms:
    workshop.tabular.filter:
      spec: fasthep_workshop.transforms.tabular:TABULAR_FILTER_SPEC
      impl: fasthep_workshop.transforms.tabular:run_tabular_filter

  backends:
    local.default:
      impl: hepflow.backends:Local
```

This is how an entry such as:

```yaml
op: workshop.tabular.filter
```
is resolved to both specification and implementation.

```{mermaid}
flowchart LR
    Operation["<b>workshop.tabular.filter</b><br/>requested capability"]:::input
    Registry["<b>Registry</b><br/>resolution"]:::capability
    Spec["<b>Specification</b><br/>compile-time contract"]:::flow
    Impl["<b>Implementation</b><br/>runtime behaviour"]:::runtime

    Operation --> Registry
    Registry --> Spec
    Registry --> Impl
```

The resolved registry is included in the execution plan so that the runtime receives the same capability environment that was used to compile the workflow.

Registries can describe more than analysis operations. Depending on the installed capabilities they may contain:

- sources
- transforms
- sinks
- observers
- backends
- hooks and compile hooks
- execution modifiers
- product handlers
- functions and constants
- rendering and reporting capabilities

Flow itself does not need to provide all of these capabilities.

### Tracking where capabilities came from

Flow also records how the resolved workflow environment was assembled.

For the exoplanet workflow, the provenance information includes:

```yaml
provenance:
  registry_layers:
    - name: builtin
      kind: builtin

    - name: registry
      kind: profile
      path: package:hepflow.profiles/registry.yaml

    - name: fasthep_workshop:registry
      kind: profile
      path: package:fasthep_workshop.profiles/registry.yaml

    - name: workflow
      kind: workflow
      path: examples/NASA/exoplanets/workflow.yaml
```

Registry changes record which capabilities were added or overwritten by each layer.

Together, the resolved registry and its provenance answer two related questions:

- **What capabilities did this workflow use?**
- **Where did those capabilities come from?**

This is useful for debugging and forms part of the provenance needed to reproduce a workflow environment.

```{note}
Provenance support is still evolving.

Work is ongoing to associate resolved workflow components with the package versions that supplied them. This will make it possible to record not only which implementation was selected, but which installed software version provided it.
```

---

## Constructing the execution plan

The execution plan is the resolved description of what should execute and forms the principal boundary between compilation and runtime.

For example:

```yaml
- id: stage.EarthSizedPlanets
  graph_node_id: stage.EarthSizedPlanets
  role: transform
  impl: workshop.tabular.filter

  inputs:
    - node_id: stage.PlanetRows
      output_name: stream
      input_name: stream

  params:
    expr: (planet_radius > 0.8) & (planet_radius < 1.2)

  outputs:
    stream: event_stream

  input_scope: partition
  output_scope: partition

  partitioning:
    mode: dataset
    chunk_size: null

  materialize: never
```

Several pieces of information that were implicit or distributed across the workflow description, specifications, and workflow environment are now explicit in one place.

The runtime can see:

- which implementation to invoke
- where its inputs come from
- which parameters to pass
- which products it produces
- the scopes of those products
- its partitioning behaviour
- whether its result needs to be materialised

The plan describes **what should execute** while remaining independent of **where its partitions are eventually executed**.

---

## Products connect graph nodes

Inputs and outputs in the plan explicitly describe how products move between nodes.

For example:

```yaml
inputs:
  - node_id: stage.PlanetRows
    output_name: stream
    input_name: stream

outputs:
  stream: event_stream
```

This says that `EarthSizedPlanets` receives the `stream` product produced by `PlanetRows` and exposes another `stream` product.

The final writer similarly connects to the transformed stream:

```yaml
inputs:
  - node_id: stage.PlanetTable
    output_name: stream
    input_name: target
```

Its output is a different product:

```yaml
outputs:
  artifact: artifact
```

These product relationships form the edges of the compiled workflow graph.

Product types also allow Flow and extensions to reason about how values should be handled across execution boundaries.

---

## Execution configuration is part of the plan

The plan includes the resolved execution configuration:

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

For the exoplanet workflow this is deliberately simple.

More complex execution environments can provide resource descriptions, pools, environment configuration, or backend-specific settings without changing the scientific operations themselves.

Compilation resolves this configuration; the runtime and selected backend determine how it is applied.

See {doc}`runtime` for execution in more detail.

---

## Compilation can be extended

Compilation itself is extensible.

**Compile hooks** allow installed capabilities to participate in compilation without requiring their behaviour to become part of Flow's core compiler.

A compile hook may, for example:

- inspect input files
- enrich dataset metadata
- perform additional validation
- generate diagnostics
- produce workflow visualisations
- write additional compilation artifacts

One example is dataset inspection: an extension can inspect source files during compilation and add information such as event counts to the resolved dataset metadata.

Another is graph rendering, where the compiled workflow can be turned into a D2, SVG, or other visual representation.

Conceptually:

```{mermaid}
flowchart LR
    Compiler["<b>Compilation</b>"]:::flow
    Plan["<b>Execution plan</b>"]:::plan
    Hooks["<b>Compile hooks</b><br/>inspect + augment"]:::capability
    Artifacts["<b>Additional artifacts</b>"]:::artifact

    Compiler --> Plan
    Compiler -.-> Hooks
    Hooks -.-> Plan
    Hooks --> Artifacts
```

Compile hooks are not necessarily a single compiler stage. They can participate at defined points in compilation to inspect or augment compiler representations.

---

## Inspecting compilation

Different artifacts answer different questions.

| Question                                         | Start with             |
| ------------------------------------------------ | ---------------------- |
| What did my **workflow description** resolve to? | `normalized.yaml`      |
| What **logical graph** did Flow construct?       | `analysis.ir.yaml`     |
| Why is a field being requested?                  | `deps.yaml`            |
| Which datasets and files were resolved?          | `dataset_entries.json` |
| What will the runtime actually receive?          | `plan.yaml`            |
| Were there compilation diagnostics?              | `report.compile.yaml`  |

Additional files may be produced by compile hooks.

For most debugging, `normalized.yaml`, `deps.yaml`, and `plan.yaml` provide a useful progression:

```text
What did Flow understand?
        ↓
Why does it need these data?
        ↓
What will it execute?
```

---

## Compilation does not execute the analysis

Compilation may perform supporting work, particularly through compile hooks,
but it remains distinct from scientific workflow execution.

Its purpose is to assemble, understand, resolve, validate, and plan the
computation.

The resulting execution plan contains the information needed for the runtime to
carry out that computation without returning to the original workflow
description:

```{mermaid}
flowchart LR
    Workflow["<b>workflow.yaml</b><br/>workflow description"]:::input
    Compiler["<b>Flow compiler</b>"]:::flow
    Plan["<b>Execution plan</b>"]:::plan
    Runtime["<b>Flow runtime</b>"]:::runtime

    Workflow --> Compiler --> Plan --> Runtime
```

This boundary also allows other workflow languages and compilation systems to
target Flow.

`workflow.yaml` is the standard FAST-HEP workflow language, but it is not a
requirement of the runtime. Any tool capable of constructing a compatible
execution plan can use Flow for execution.

---

## Where next?

Continue with {doc}`runtime` to see how the compiled plan is executed.

For the contracts that allow the compiler to reason about registered capabilities, see {doc}`../extending/operations-and-specs`.

For the complete structure of the execution plan and other compiler representations, see {doc}`../reference/index`.
