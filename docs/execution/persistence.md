# Product Persistence Schema

Flow can carry a product in memory and write a sink's files
([artifact locations](artifacts.md)). Persistence is the next step: save a product,
restore it in another process, and keep its provenance and execution evidence intact.
This page is the schema for that. It decides what is stored and how it is restored.
Where a file lives is the [artifact location contract](artifacts.md),
taken as given here: a location is one string, one `ArtifactReference` per file,
`ArtifactManifest.root` for a group.

The implementation is tracked in
[fasthep-flow#115](https://github.com/FAST-HEP/fasthep-flow/issues/115). The
decision is [fasthep-flow#152](https://github.com/FAST-HEP/fasthep-flow/issues/152).

## Three levels

| Level | Where | What it holds |
|---|---|---|
| Registered capability | `product_handlers.<kind>.persistence.formats.<name>` | what can be saved and loaded, and by which implementation |
| Resolved plan binding | the execution plan | which capability a product uses, with its parameters resolved |
| Published storage binding | the product's `ArtifactReference` | what was actually written, recorded at publication |

Restore uses the published storage binding and nothing else. A changed registry
default or a newer capability never changes how an old product is read.

Parameters are resolved in this order: an explicit persistence choice, then
explicitly bound writer or plan values, then the capability's registered defaults.
Flow does not search unrelated node parameters.

Products of a kind without persistence keep working in memory. They fail only when a
durable transfer is needed.

## Registered capability

A capability is a validated storage descriptor plus a save and load pair. It is its
own type, not a runtime component spec. The pair is either registered components
with a package-owned adapter, or callbacks.

| Field | Required | Meaning |
|---|---|---|
| `name` | yes | stable capability name, for example `histogram.data` |
| `version` | yes | capability version, changed when the stored layout changes |
| `kind` | yes | the product kind it stores |
| `container` | yes | the physical container, for example `json` or `root` |
| `variants` | no | named representations the reader can verify, for example `ttree`, `rntuple` |
| `params` | no | parameter schema and defaults for save and load |
| `bindings` | no | how workflow or plan values map to save and load parameters |
| `payload_roles` | yes | the payload files the capability writes, predictable before execution |
| `fidelity` | yes | what a round trip preserves, and what it does not |

The first histogram capability is data-only: axes, categories, storage type,
weighted values, variances and metadata. It does not use pickle. A projected or
selected ROOT result is a legitimate derived product; it is never claimed to
reconstruct its pre-projection input. ROOT histogram storage is not part of this
schema. It will use the same mechanism later.

## Storage binding

`ArtifactReference.storage` is a typed `StorageBinding`. There is no
`metadata["storage"]` fallback.

| Field | Required | Meaning |
|---|---|---|
| `capability` | yes | capability name |
| `capability_version` | yes | capability version |
| `container` | yes | physical container |
| `variant` | no | the variant actually written |
| `locator` | no | the object inside the container, for example a histogram name; distinct from the file location |
| `save` | yes | `{implementation, version, params}` that wrote it |
| `load` | yes | `{implementation, version, params}` that will read it |

The implementation and adapter versions are evidence. They do not select the reader.

## Result manifest

One result manifest describes one published product set. It extends
`ArtifactManifest`, so it has `root` and the references below it.

| Field | Required | Meaning |
|---|---|---|
| `schema_version` | yes | version of this manifest format |
| `plan_hash` | yes | the semantic plan hash ([fasthep-flow#105](https://github.com/FAST-HEP/fasthep-flow/issues/105)) |
| `root` | yes | the location of the product set, by the location rule |
| `products` | yes | one entry per product instance: identity, `storage` and its payload references |
| `evidence` | yes | one location of the shared evidence file |

Every payload reference records `size_bytes` and `sha256`. Both are verified on load.

The manifest is written last and atomically: a temporary file in the same directory,
then a rename. Its presence is the commit marker. A payload without a manifest, or a
payload that is missing or has the wrong size, is never reusable. Flow validates a
manifest. `fasthep-distributed` owns attempts and completion, and trusts only a manifest
Flow has validated.

Illustrative manifest for a histogram (not a locked layout):

```json
{
  "schema_version": "1",
  "plan_hash": "sha256:…",
  "root": "artifacts/products/pt_hist",
  "products": [
    {
      "producer_node": "stage.Fill",
      "output_name": "pt",
      "product_kind": "histogram",
      "dataset_name": "Zmumu",
      "scope": "dataset",
      "storage": {
        "capability": "histogram.data",
        "capability_version": "1",
        "container": "json",
        "variant": null,
        "locator": null,
        "save": {"implementation": "fasthep_carpenter.products:save_hist", "version": "0.1", "params": {}},
        "load": {"implementation": "fasthep_carpenter.products:load_hist", "version": "0.1", "params": {}}
      },
      "payloads": [
        {"uri": "pt_hist/pt.json", "size_bytes": 2048, "sha256": "…"}
      ]
    }
  ],
  "evidence": "pt_hist/evidence.json"
}
```

## Identity

A product instance is identified by these fields. Anything not listed is outside
identity.

| Field | In identity |
|---|---|
| producer node | yes |
| output name | yes |
| product kind | yes |
| dataset | yes |
| partition or aggregation scope | yes |
| semantic plan hash | yes |
| location, container | no |
| lossless re-encoding | no |
| scientific projection or selection | yes |

Flow owns this vocabulary. The later run artifact index extends it.

## Restoring

1. Read the manifest. A missing manifest means nothing was published.
2. For each product, find the reader in the trusted registry by the recorded
   `capability` and `capability_version`. The match is exact.
3. Check the payloads: present, `size_bytes` and `sha256` equal.
4. Load. Restore is a consumption event, not a new scientific producer invocation.
   Hooks are not replayed.

There is no migration and no fallback to another reader or another version.

## Errors

Each error carries the producer node, the output name and the product kind.

| Error | Raised when |
|---|---|
| `unknown_capability` | the recorded capability is not registered |
| `unsupported_version` | the capability is registered, but not at the recorded version |
| `missing_reader` | the capability has no load implementation available |
| `invalid_binding` | the storage binding fails validation, for example a required field is missing |
| `kind_mismatch` | the binding's kind is not the product's kind |
| `variant_mismatch` | the recorded variant is not one the reader supports |
| `missing_payload` | a payload named in the manifest does not exist |
| `size_or_hash_mismatch` | a payload's `size_bytes` or `sha256` differs from the manifest |
| `missing_evidence` | the manifest names evidence that cannot be read |
| `conflicting_provenance` | an imported record has the same identity as an existing one but different content |

## Evidence import

- Importing an identical record again changes nothing.
- The same identity with different content fails with `conflicting_provenance`.
- Retries stay distinct records.
- A failed attempt never appears as a successful producer.
- A restore adds a consumption record and replays no hook.
- Manifests, parameters and evidence never contain credentials.

## Breaking change

This is an alpha change with no shim.

- `ArtifactReference.format` is removed. The storage binding's `capability`, `container`
  and `variant` replace it.
- `schema_version` changes once, in the implementation of
  [fasthep-flow#115](https://github.com/FAST-HEP/fasthep-flow/issues/115), after the
  bump of the [location contract](artifacts.md) ("Bundle versions").

Consumers that set or read `format` change in the same stack:

- fasthep-flow itself: `ArtifactReference` and `ArtifactManifest` serialisation,
  writer manifests, report records;
- fasthep-render: its sinks pass `format` in their results;
- fasthep-carpenter: the ROOT writer's result and persistence specs;
- fasthep-curator: the observers' results and persistence specs;
- the provenance viewer reads the recorded format.

`fasthep-distributed` and `fasthep-cli` only read the manifest.

## Not covered

- the per-node resource index, render inputs and the run artifact index (later, the
  run artifact index; it will define lineage completeness);
- ROOT histogram storage and shared ROOT containers;
- migration of stored products between capability versions;
- cache lookup.

"Publication completeness" is the commit state of a manifest. "Lineage completeness" is
reserved for the run artifact index and means something else: whether every product has
its producers recorded.
