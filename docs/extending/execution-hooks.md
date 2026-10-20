# Execution hooks

Execution hooks are runtime callbacks that Flow calls at fixed points of a run. They observe execution (timing, memory, warnings, errors, dataset context) without being part of the workflow graph.

```{note}
Execution hooks are not the same thing as {doc}`observers`. An observer is a node-like capability declared in the workflow's `observers:` block and attached to a compiled workflow point with `at:`. An execution hook is enabled by a profile through `execution_hooks:` and reacts to lifecycle events, not to workflow points. Both are used for diagnostics, and the two mechanisms have developed separately; choose by what you need to look at:

- the **products at a workflow point** (a schema, a count): observer;
- the **execution itself** (how long, how much memory, which warnings, which partition failed): execution hook.
```

---

## Lifecycle events

A hook is a subclass of `hepflow.model.hooks.ExecutionHook` and implements any of these methods. Flow calls only the events the hook's spec lists.

| Event | Called | Receives |
| --- | --- | --- |
| `partition_start` | before the first node of a partition | `partition`, `ctx` |
| `around_node` | around every node (context manager) | `node`, `inputs`, `ctx` |
| `on_node_error` | when a node raises | `node`, `inputs`, `ctx`, `exc` |
| `partition_end` | after the last node of a partition | `partition`, `ctx`, `value_store` |
| `dataset_end` | after the last partition of a dataset | `dataset_name`, `ctx`, `value_store` |
| `run_end` | once, when the run finishes | `plan`, `ctx`, `summary` |

Hooks should treat `inputs`, `outputs` and `ctx` as read-only. Execution modifiers (see {doc}`execution-modifiers`) share the node lifecycle but may change them.

---

## Registering and enabling a hook

A hook is registered like other extensions: a spec with `kind: hook` and a `lifecycle.events` list, and an implementation class, both named in a registry:

```python
RESOURCE_USAGE_HOOK_SPEC = {
    "name": "hep.resource_usage",
    "kind": "hook",
    "version": "1.0",
    "lifecycle": {"events": ["partition_start", "partition_end"]},
}
```

```yaml
registry:
  hooks:
    hep.resource_usage:
      spec: fasthep_curator.hooks.resource_usage:RESOURCE_USAGE_HOOK_SPEC
      impl: fasthep_curator.hooks.resource_usage:ResourceUsageHook
```

Hooks are switched on by a profile, so a workflow opts in with `use: profiles:`:

```yaml
execution_hooks:
  - kind: hep.resource_usage
    events: [partition_start, partition_end]
```

`events` may list a subset of the spec's events. Listing an event the spec does not support raises an error when the hook manager is built from the plan. The enabled hooks appear in the plan under `execution_hooks`.

---

## Getting results out of a hook

There are three channels:

- **`results()`** (optional method returning a dict): included in the hook's entry of the usage summary (`hooks.enabled[].results` in `run_summary.yaml`). This is the channel for per-partition records. Backends return the usage summary from where the hook ran, so results survive partition tasks on remote workers; the Dask backend concatenates list results across partitions.
- **`summary()`** (optional method returning a dict): merged into the run summary by the process that calls `run_end`. Lists are appended, dicts are updated. Use it for records that exist where `run_end` runs.
- **`ctx`**: a hook may append records to a list in the context. The warning-capture hook uses `ctx["_warnings"]`, which backends return as `warnings`.

```{note}
`results()` carries data, so keep it small and serialisable (numbers, strings, lists and dicts of those). Do not put secrets or large arrays in it.
```

---

## Example

`fasthep-curator` provides `hep.dataset_context`, `hep.error_report`, `hep.warning_capture` and `hep.resource_usage` (wall time, CPU time and peak memory per partition, enabled with the `fasthep_curator:resource_usage` profile). Their sources are in `fasthep_curator/hooks/`.

---

## Where next?

- {doc}`observers`: inspecting products at workflow points
- {doc}`execution-modifiers`: changing how a plan is executed
- {doc}`registries-and-profiles`: making hooks available to workflows
