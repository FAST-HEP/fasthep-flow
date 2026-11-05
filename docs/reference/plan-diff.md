# Comparing compiled plans

`hepflow.api.diff_plan_files(old, new)` compares two compiled plan files and answers one narrow
question:

> Are these compiled plans equal except for recorded origin and derivation history?

It does **not** claim that equal plans produce equal runtime outputs or physics results. It does
not compare produced data, runtime environments, nondeterminism or external resources.

```python
from hepflow.api import diff_plan_files

report = diff_plan_files("old/compile/plan.yaml", "new/compile/plan.yaml")
report.equal               # no differences of any kind
report.semantically_equal  # no differences outside provenance
```

Both predicates are computed over the same complete report. Every section except `provenance`
counts towards semantic inequality, including execution policy and report configuration. Unknown
fields fail closed: a changed field the classifier does not know belongs to `unknown` and counts as
semantic.

## Sections

The public order is fixed: `graph`, `data`, `execution`, `reports`, `provenance`, `unknown`.

Classification uses an exact structural-path table and never infers provenance from key names such
as `path`, `source` or `hash`. The longest matching key-path prefix wins; list indices and identity
selectors are skipped; anything unregistered is `unknown`. Classifier version: 1
(`report.to_dict()["classifier_version"]`).

| Plan content (key path) | Section |
|---|---|
| `context.datasets`, `context.dataset_names`, `partitions` | `data` |
| `context.globals`, `context.variation`, `data_flow` | `graph` |
| `context.workflow_path`, `context.project_root` | `provenance` |
| `execution`, `execution_hooks` | `execution` |
| `reports` | `reports` |
| `provenance` (including `provenance.compilation`) | `provenance` |
| `nodes.id`, `nodes.graph_node_id`, `nodes.role`, `nodes.impl`, `nodes.inputs`, `nodes.params`, `nodes.outputs` | `graph` |
| `nodes.input_scope`, `nodes.output_scope`, `nodes.partitioning`, `nodes.materialize`, `nodes.meta.execution` | `execution` |
| `nodes.meta.compile_hooks` (the whole mapping) | `provenance` |
| `nodes.meta.stage_id`, `nodes.meta.workflow_op`, `nodes.meta.workflow_kind`, `nodes.meta.output_layout`, `nodes.meta.applies_to`, `nodes.meta.join_name`, `nodes.meta.source_name`, `nodes.meta.stream_id`, `nodes.meta.inserted_by`, `nodes.meta.observed_node`, `nodes.meta.parent_kind`, `nodes.meta.parent_name`, `nodes.meta.style_ref`, `nodes.meta.input_node`, `nodes.meta.variation`, `nodes.meta.variation_of`, `nodes.meta.export_of`, `nodes.meta.collection_for` | `graph` |
| `registry.functions`, `registry.constants`, `registry.sources`, `registry.transforms`, `registry.sinks`, `registry.observers`, `registry.render` | `graph` |
| `registry.backends`, `registry.hooks`, `registry.progress_sinks`, `registry.execution_modifiers`, `registry.product_handlers`, `registry.resource_kinds` | `execution` |
| `registry.report_templates` | `reports` |
| `registry.compile_hooks` | `provenance` |
| any other top-level key, `context.*`, `nodes.*`, node meta key or `registry.*` section | `unknown` |

A compile-hook record describes how a value was derived; the final value that affects the analysis
is already in `node.params`. A changed hook record with unchanged parameters is therefore
semantically equal.

## Matching lists

Mapping key order is irrelevant. Scalars compare by type and value exactly: `true`, `1`, `1.0` and
`"1"` are distinct and floats have no tolerance. A key that is absent differs from a present `null`.

Only the identities below are matched by identity (the order of such a list does not matter). There
is no generic "object with `id` or `name`" matcher.

| List | Identity |
|---|---|
| `nodes` | `id` |
| `partitions` | `(source, dataset, file, part)` |
| node `inputs` | `(node_id, input_name)` |
| `reports` | `id` |
| `provenance.inputs` | `(role, declared)` |
| `provenance.registry_layers`, `execution_layers`, `execution_hook_layers` | `(kind, name)` |
| `provenance.registry_changes.added` | `(section, key, layer)` |
| `provenance.registry_changes.overwritten` | `(section, key, previous_layer, new_layer)` |
| `provenance.optional_profiles_skipped` | `(profile, included_by)` |

An unmatched identified object is one `added` or `removed` entry holding the whole object (its
section follows its list: nodes `graph`, partitions `data`, reports `reports`, provenance lists
`provenance`); a matched object is compared down to leaf differences. Duplicate `nodes`,
`partitions` or `reports` identities are an `InvalidPlan` error. In provenance lists, rows equal in
every field collapse first; remaining duplicate identities produce an `identity_conflict` entry and
the list is compared by sequence alignment, with the resulting entries marked `fallback`.

Every other list is anonymous and order-sensitive. It is compared by sequence alignment, so one
insertion is one `added` entry rather than a shift of every later index; a pure reorder remains a
difference.

## Moves

A relocated input is one `moved` entry, not an `added` plus a `removed`. It is a `provenance`
entry: `equal` is false and `semantically_equal` is unaffected.

Moves are looked for among the unmatched rows of `provenance.inputs` (identity `(role, declared)`).
A move is reported only when all of these hold:

- one removed and one added row pair uniquely on `(role, sha256)`;
- both rows carry a valid canonical SHA-256 (64 lowercase hex characters);
- the SHA-256 values match;
- the non-location metadata matches: `size` and `note`.

The location fields that may differ are `declared`, `resolved` and `origin`. Nothing is guessed:
ambiguous equal hashes (several rows sharing `(role, sha256)` on either side), a missing or
malformed hash, a changed role, size or note all leave the rows as plain `added` / `removed`
entries. A row whose `(role, declared)` is unchanged but whose `resolved` path differs is an
ordinary `changed` leaf, not a move. Compile-hook records (`nodes[].meta.compile_hooks`) and layer
`path` fields are never paired; they are `changed` leaves with path display.

## Path display

Each side is displayed relative to its own recorded `context.project_root`. The registry of
path-valued fields is exact (no key-name guessing):

- `context.workflow_path`
- `context.project_root`
- `provenance.root`
- `provenance.inputs.declared`
- `provenance.inputs.resolved`
- `provenance.registry_layers.path`
- `provenance.execution_layers.path`
- `provenance.execution_hook_layers.path`
- `nodes.meta.compile_hooks.*.resolved_path`

Normalisation is lexical (`posixpath`); it never resolves symlinks or reads the filesystem. The
display value of a leaf entry is `{"value", "label"}` in `old_display` / `new_display`, with the
raw value kept in `old` / `new`. A moved entry has a display for `declared` and `resolved` per
side. Labels:

| Label | Meaning |
|---|---|
| `relative` | under the recorded root (shown relative to it; the root itself is `.`); an already relative value stays as written |
| `external` | an absolute path outside the root, unchanged |
| `uri` | `scheme://...`, unchanged |
| `no_root` | the plan records no `context.project_root` (older plans), unchanged |
| `root` | the root fields themselves, shown as written |

Whole added or removed objects expose raw values only.

## Resolving a build directory

`hepflow.api.resolve_plan_input(path)` turns what a user passes into one plan file; `diff_plan_files`
itself always takes files.

1. An existing regular file is returned exactly as supplied (its contents are not read here).
2. A directory is a build root (it holds `compile/`) or a compile directory (it holds `plan.yaml` or
   `systematics.yaml`); both at once is an error.
3. `compile/plan.yaml` is the canonical plan of standard and inline-systematics builds. If both
   `plan.yaml` and `systematics.yaml` are present the layout is conflicting.
4. Otherwise `systematics.yaml` supplies the candidates: only its `variations[].plan` entries, which
   are relative to the build root and include the `compile/` prefix.
5. Exactly one declared plan is selected. Several raise `AmbiguousPlans`, listing each variation name
   and path; `nominal` is never chosen silently.

Nothing is searched recursively. Errors carry a stable `code`:

| Error | Code | When |
|---|---|---|
| `InputNotFound` | `input_not_found` | the path or a declared plan does not exist |
| `NoPlanFound` | `no_plan_found` | a directory holds no plan, or `systematics.yaml` declares none |
| `AmbiguousPlans` | `ambiguous_plans` | several declared plans; pass an explicit plan file |
| `InvalidPlan` | `invalid_plan` | malformed or duplicate declarations, a path escaping the build root, conflicting artifacts, a plan that fails validation |
| `PlanDiffError` | `comparison_failed` | base class and fallback for any other comparison failure |

## Validation

Both plans are validated before comparison without being modified: the root must be a mapping,
keys must be strings, values must be JSON-native (YAML dates and sets are rejected) and finite,
known containers must have the expected type, and the identity fields above must be present. A
violation raises `InvalidPlan` with the path of the offending value.

## The report

`PlanDiffReport` exposes `entries`, `warnings`, `equal`, `semantically_equal`, `counts()`, `scoped()`
and `to_dict()`. Entry kinds are `added`, `removed`, `changed`, `moved` and `identity_conflict`
(`moved` pairs a relocated input, see above). Each `PlanDiffEntry` has a section, kind, typed
`location` (mapping keys, sequence positions, identity selectors), a rendered location, identity,
`old_present` / `new_present`, the raw `old` / `new` values and a `fallback` flag. Counts count
entries, not every scalar inside an added object. Entries are ordered by section, then location,
then kind.

`to_dict()` is deterministic and JSON-serialisable:

```text
report_schema_version, classifier_version, equal, semantically_equal,
counts {sections, kinds}, entries [...], warnings [...], scope
```

`report.scoped(include=..., ignore=...)` applies the included sections first and the ignored
sections second, records the scope, recomputes both predicates and leaves the original report
unchanged; an empty selection raises `EmptyScopeError`.

## Warnings

Warnings never change equality, counts or an exit status. `flow_version_mismatch` is reported when
both plans record `provenance.compilation.flow_version` and the strings differ (compared exactly);
`flow_version_unknown` when only some of the plans record it. The version difference itself is an
ordinary provenance entry.

## Examples

A relocated workflow: only provenance differs, so `equal` is false and `semantically_equal` is true.

```text
provenance  changed  context.workflow_path       '/a/wf.yaml' -> '/b/wf.yaml'
provenance  changed  provenance.root             '/a' -> '/b'
```

A changed parameter: one graph entry.

```text
graph  changed  nodes[id='Scale'].params.factor  2.0 -> 3.0
```

A relocated input (same hash, new path): one provenance move.

```text
provenance  moved  provenance.inputs[role='workflow',sha256='3b0c...']
                   skim/legacy/Run3/x.yaml -> skim/productions/x.yaml
```

A field the classifier does not know: fail-closed.

```text
unknown  added  brand_new_section  {'k': 1}
```

Two provenance rows with the same `(role, declared)` but different content:

```text
provenance  identity_conflict  provenance.inputs[role='param:S.p',declared='x.json']
```
