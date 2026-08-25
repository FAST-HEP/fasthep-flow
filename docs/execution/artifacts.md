# Artifact References and Manifests

Artifacts are files or storage objects produced by sinks, such as ROOT skim
parts or rendered plots. Flow does not treat artifact contents as ordinary
runtime values when they cross execution boundaries. It carries explicit,
lightweight references.

An artifact reference identifies an already-written artifact. It includes a
storage-neutral locator, product kind, optional format, producer/output
identity, dataset identity, partition identity when applicable, and lightweight
metadata. Constructing or reducing references must not open, copy, deserialize,
or inspect artifact contents.

Partitioned artifacts are represented at dataset scope as an
`ArtifactManifest`. A manifest is one logical artifact product made of zero or
more ordered artifact references. For a partitioned ROOT skim, the default
dataset product is therefore a manifest of ROOT part files, not one merged ROOT
file.

## Boundary Representations

Boundary product policies keep lifetime and representation separate:

- `value` carries the runtime value across the boundary.
- `reference` carries an existing `ArtifactReference` or `ArtifactManifest`.
- `materialize` invokes the registered product materializer and carries the
  returned reference-like value.

`reference` does not call a materializer. If a partition writer already wrote a
ROOT part and returned an artifact reference, Flow carries that reference.

`materialize` requires a registered materializer. The materializer receives the
value plus runtime context such as node, output name, output directory, dataset,
partition, and source/destination scopes. It must return an
`ArtifactReference` or `ArtifactManifest`; returning a large in-memory value is
not materialization.

## Dataset Manifests

At dataset finalization, the artifact product handler reduces collected
references into an `ArtifactManifest`. It validates that references belong to
one logical product, preserves deterministic partition ordering, and does not
perform artifact-content I/O.

Physical coalescing is deliberately not implicit. A later operation may consume
an `ArtifactManifest` and publish a new coalesced artifact reference, but the
manifest remains the default representation.

Example serialized manifest:

```json
{
  "type": "artifact_manifest",
  "product_kind": "root_tree",
  "format": "rntuple",
  "producer_node": "write.SelectedEvents.0",
  "output_name": "artifact",
  "dataset_name": "data",
  "parts": [
    {
      "type": "artifact_reference",
      "uri": "artifacts/files/skim/data/0_0.root",
      "product_kind": "artifact",
      "format": "root",
      "producer_node": "write.SelectedEvents.0",
      "output_name": "artifact",
      "dataset_name": "data",
      "partition_id": "events__data__0",
      "partition_index": 0
    }
  ]
}
```

## Lifecycle

The expected lifecycle for partition artifacts is:

1. A partition writer creates and closes a physical artifact part.
2. The writer returns an `ArtifactReference`.
3. Flow validates and accepts the reference into reduction state.
4. The partition value store is released.
5. Dataset finalization creates an `ArtifactManifest`.
6. Dataset-end sinks receive the manifest.
7. Writer and provenance manifests are published from references/manifests.

Future distributed backends should return artifact references from completed
partition work and use the same manifest reduction contract. They should not
flatten different datasets into one undifferentiated list of files, and they
should not rely on local filesystem operations for opaque URI locators.
