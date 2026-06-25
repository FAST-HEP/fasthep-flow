# Operations and Specs

Operations are contributed by extension packages through registry entries. Flow
loads the operation implementation and its component spec from the registry, then
uses that spec during compilation and runtime planning.

The intended component-author imports from `hepflow.model` are deliberately
small:

```python
from hepflow.model import (
    ComponentSpec,
    ExecutionHook,
    FlowIssue,
    IssueLevel,
    OperationResult,
    ProductRef,
)
```

Most transform and sink authors only need `ComponentSpec`, and many components
can simply expose a `ComponentSpec`-shaped dictionary.

Do not treat backend classes, Dask helpers, lowered graph nodes, execution-plan
internals, or render Data Transfer Objects (DTOs) as component-author API. Those are compiler/runtime,
backend, or package-specific implementation details.

## Component spec shape

Use one component-spec shape for sources, transforms, sinks, observers,
modifiers, and compile hooks. Role-specific behavior should be represented as
fields on the shared spec and by the registry section where the component is
registered.

```python
from hepflow.model import ComponentSpec

EXAMPLE_TRANSFORM_SPEC = {
    "name": "example.define",
    "kind": "transform",
    "params": {
        "variables": {"type": "list", "required": True},
    },
    "requires": {
        "symbols": [
            {"from": "params.variables.*.expr", "kind": "expr"},
        ],
    },
    "provides": {
        "symbols": [
            {"from": "params.variables.*.name", "kind": "field_list"},
        ],
    },
}

# Optional validation/coercion if your package wants a dataclass instance:
EXAMPLE_TRANSFORM_SPEC = ComponentSpec.from_obj(EXAMPLE_TRANSFORM_SPEC)
```

Equivalent registry entry:

```yaml
registry:
  transforms:
    example.define:
      spec: my_package.transforms.define:EXAMPLE_TRANSFORM_SPEC
      impl: my_package.transforms.define:run_define
```

## Declaring requirements and provided symbols

Flow discovers data dependencies from declarative `requires.symbols` entries.
The `from` value is a path into the normalized component parameters; the `kind`
selects how values are interpreted.

```yaml
requires:
  symbols:
    - from: params.keep
      kind: field_list
```

Currently used parser kinds are:

- `field_list`: a field name or list of field names
- `expr`: an expression
- `expr_or_field`: an expression or simple field name
- `cutflow`: nested cutflow selection expressions

Use `provides.symbols` when a transform creates stream fields that downstream
components may consume:

```yaml
provides:
  symbols:
    - from: params.variables.*.name
      kind: field_list
```

Optionality comes from the parameter schema and normalization defaults. Do not
add a separate `optional` flag to `requires`.

## Transform example

```python
DEFINE_PT_SPEC = {
    "name": "example.pt_ratio",
    "kind": "transform",
    "params": {
        "variables": {"type": "list", "required": True},
    },
    "requires": {
        "symbols": [
            {"from": "params.variables.*.expr", "kind": "expr"},
        ],
    },
    "provides": {
        "symbols": [
            {"from": "params.variables.*.name", "kind": "field_list"},
        ],
    },
}


def run_pt_ratio(data, *, params, ctx=None):
    # Read user configuration from params, update the input stream, and return
    # the value expected by the operation's result contract.
    return data
```

Registry entry:

```yaml
registry:
  transforms:
    example.pt_ratio:
      spec: my_package.transforms.pt_ratio:DEFINE_PT_SPEC
      impl: my_package.transforms.pt_ratio:run_pt_ratio
```

Author usage:

```yaml
analysis:
  stages:
    - id: BuildMuonPt
      op: example.pt_ratio
      params:
        variables:
          - name: Muon_Pt
            expr: "sqrt(Muon_Px ** 2 + Muon_Py ** 2)"
```

From this stage, `flow` sees:
```yaml
requires:
  symbols:
    - Muon_Px
    - Muon_Py

provides:
  symbols:
    - Muon_Pt
```

The required symbols come from parsing `params.variables.*.expr` as an
expression. The provided symbol comes from reading `params.variables.*.name`.

## Sink example

Sinks and writers should also declare the stream symbols they consume, especially
when the sink selects output columns that are not otherwise referenced by a
transform.

```python
WRITE_TABLE_SPEC = {
    "name": "example.write_table",
    "kind": "sink",
    "params": {
        "path": {"type": "string", "required": True},
        "keep": {"type": "list", "default": None},
    },
    "requires": {
        "symbols": [
            {"from": "params.keep", "kind": "field_list"},
        ],
    },
}


def run_write_table(data, *, params, ctx=None):
    # Write artifacts using runtime context and params.
    return None
```

Registry entry:

```yaml
registry:
  sinks:
    example.write_table:
      spec: my_package.sinks.write_table:WRITE_TABLE_SPEC
      impl: my_package.sinks.write_table:run_write_table
```

Author usage:

```yaml
analysis:
  stages:
    - id: SelectEvents
      op: hep.selection.cutflow
      params:
        selection:
          signal_region:
            - "NMuon >= 2"
      write:
        - kind: example.write_table
          path: selected_events.csv
          keep:
            - NMuon
            - Muon_Pt
            - EventWeight
```

From the attached writer `flow` normalises a sink node and sees:

```yaml
requires:
  symbols:
    - NMuon
    - Muon_Pt
    - EventWeight

provides:
  symbols: []
```

The required symbols come from reading `params.keep` as a field list. This means
a writer can request fields even if no transform otherwise uses them.

:::{note}
The meaning of `keep` is implementation-specific.

In this example, `keep` is interpreted as the list of fields that should be
written to the output artifact. Other writers may use different conventions.

If `keep` is omitted, the default behaviour depends on the writer
implementation. Some writers may write the complete event stream, while others
may require an explicit field list.

Regardless of the runtime behaviour, any fields listed in `keep` participate in
dependency discovery. This allows a sink to request fields that are not
otherwise used by upstream transforms.
:::

## Runtime hook example

Runtime hooks are components that attach to lifecycle events rather than graph
nodes. They use the same component-spec shape as other components, with
supported callback events declared under `lifecycle.events`.

Hook implementations may subclass `ExecutionHook` to inherit no-op callback
methods, but `ExecutionHook` is only the runtime callback base class. It is not a
spec model.

```python
from hepflow.model import ExecutionHook

RUNTIME_DIAGNOSTICS_SPEC = {
    "name": "curator.runtime_diagnostics",
    "kind": "hook",
    "version": "1.0",
    "params": {
        "out": {"type": "string", "default": "diagnostics"},
    },
    "lifecycle": {
        "events": ["before_node", "after_node", "on_node_error"],
    },
    "context_outputs": ["diagnostics"],
}


class RuntimeDiagnosticsHook(ExecutionHook):
    def before_node(self, *, node, inputs, ctx):
        ...
```

Registry entry:

```yaml
registry:
  hooks:
    curator.runtime_diagnostics:
      spec: my_package.hooks.runtime_diagnostics:RUNTIME_DIAGNOSTICS_SPEC
      impl: my_package.hooks.runtime_diagnostics:RuntimeDiagnosticsHook
```

Hook specs must use the shared `ComponentSpec`-shaped dictionary form.

## Compile hook example

Compile hooks are components that attach to compile lifecycle phases rather than
runtime events or graph nodes. They use the same component-spec shape, with the
compile phase declared under `lifecycle.when`.

```python
DATASET_METADATA_SPEC = {
    "name": "curator.dataset_metadata",
    "kind": "compile_hook",
    "version": "1.0",
    "lifecycle": {
        "when": "after_datasets",
    },
    "inputs": [
        "datasets",
    ],
    "outputs": [
        "dataset_metadata",
    ],
}


def run_dataset_metadata(ctx, **params):
    ...
    return {"dataset_metadata": {...}}
```

Registry entry:

```yaml
registry:
  compile_hooks:
    curator.dataset_metadata:
      spec: my_package.compile_hooks.dataset_metadata:DATASET_METADATA_SPEC
      impl: my_package.compile_hooks.dataset_metadata:run_dataset_metadata
```

`lifecycle.when` may be a string phase name or a list of phase names. Current
compile hooks run during the phases explicitly invoked by Flow; for example,
`after_datasets` runs after dataset entries have been collected and before the
compile report is written.

Runtime and compile hooks share the same principle:

```yaml
kind: hook
lifecycle:
  events:
    - before_node
    - after_node

kind: compile_hook
lifecycle:
  when: after_datasets
```
