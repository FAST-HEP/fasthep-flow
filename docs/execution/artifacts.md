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

How a product is saved and restored in another process, with its provenance, is the
[product persistence schema](persistence.md).

## Artifact Locations

A location is one string. Flow stores it as one of three kinds, and `path_type` is
derived from the string:

| Location | `path_type` |
|---|---|
| a path inside the outdir, relative to it (POSIX separators) | `relative_to_outdir` |
| an absolute path (outside the outdir) | `absolute` |
| a full URI (`scheme://`, for example `root://` or `file://`) | `uri` |

### What a sink gets and returns

Flow allocates one **artifact root** per sink output and passes it as
`ctx["artifact_root"]`. The sink treats it as its working directory: it writes there
or in subfolders (`ctx.artifact_path("a/b.png")` joins a name below the root and
rejects one that leaves it). Flow does not change the process working directory.

A sink reports every file it wrote, one result or a list. A relative path it returns
is read against its artifact root, never against the process working directory. A
file outside the root is returned as an absolute path or URI and is recorded as such.
A sink never computes sizes or checksums, and Flow never walks a directory: what a
sink does not report is not recorded.

The root of a writer is its first folder below `artifacts/files`; the root of a
render sink is the folder holding its output file (`artifacts/plots`), so the file
and a folder written next to it (the `NumberMuons_heatmap` folder of a per-dataset
heatmap) are both below it.

### What Flow records

Flow normalises each file once, in the process that produced it:

- one file stays a bare `ArtifactReference`; several files become an
  `ArtifactManifest` carrying `producer_node`, `output_name`, `dataset_name` and
  `root` (the root as a location by the rule above); no file is an empty list;
- every reference carries the node identity and, for a partitioned run, the dataset
  and partition;
- `size_bytes` is recorded for every local file; `adler32` only when a delivery
  destination is configured;
- a reported local file that does not exist (or is a directory) fails the node,
  naming the sink, the node and the path; URIs are not checked;
- two files with one location in one result fail the node.

Paths are made absolute and normalised lexically (`..` is collapsed); symbolic links
are not resolved. The result is the same for a relative and an absolute `--outdir`.
Provenance evidence is read-relocatable: a finished bundle can be copied elsewhere
and every `relative_to_outdir` record resolves under the copy. This is not a restart
guarantee.

### Delivery by a backend

A backend that delivers files itself (for example HTCondor sending job-directory
files to EOS) sets the run-level context entry `artifact_root` to the directory the
`files` family is written below (`.` is a job directory) and `output_destination` to
where the files end up. A sink still sees only its own root. A file below that base
is recorded as the destination URI joined with its path relative to the base, and
carries `size_bytes` and `adler32` of the local file. Without a destination the
local path is recorded, which is only meaningful for a run on one machine.

The backend owns the transfer and the check. It reports one status per file,
`ok`, `missing`, `size_mismatch`, `checksum_mismatch` or `error`, and Flow records it
with `hepflow.runtime.artifacts.apply_delivery_status` on the file's record, in the
writer manifest and as counts in the partition's `run_summary.yaml` (`delivery`). A
failed delivery is recorded; it does not fail the run.

### Bundle versions

Provenance documents are version `2.0` and writer manifests carry
`schema_version: "2"`. Bundles written before this contract are not read:
`load_provenance_document` rejects them and they should be regenerated. Writer
manifests no longer record `relative_to_files_root` or `destination`; a delivered
file's location is its destination URI.

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

A dataset manifest merged from single-file partition results (as below) has no
`root`: a single file stays a bare reference and carries none. A sink that reports
several files in one result gets a manifest with `root`, for example a per-dataset
heatmap writing `artifacts/plots/NumberMuons_heatmap/<dataset>.png` for nine
datasets, root `artifacts/plots`.

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
      "partition_index": 0,
      "path_type": "relative_to_outdir",
      "size_bytes": 1048576
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
