# Changelog

All notable changes to this project are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). This project uses
calendar versioning (`YYYY.MM.N`) rather than semantic versioning.

Add new entries under `## [Unreleased]`, in the category they belong to. At release time, rename
`## [Unreleased]` to `## [<version>] - <YYYY-MM-DD>` and start a fresh `## [Unreleased]` above it.

## [Unreleased]

### Added

- Opt-in reproducible "compile bundle" workflow: `compile(..., snapshot=True)` snapshots the
  compile sources, `compile_from_bundle` recompiles from a saved snapshot, and compile reports now
  include a `BundleReport`.
- The compile manifest is now version 2 and records a `plan_hash` and `bundle_id` for every
  compile, every compile hook run with artifact hashes, and the invocation, workflow, include and
  profile sources that were used.
- A new `flow.software` compile hook records the installed environment and package versions at
  compile time.
- Compile-time path resolution: the project root is now explicit and recorded; path-typed
  parameters and `load_mapping`-style file parameters resolve against it, and a new
  `compile/inputs.manifest.yaml` lists every input path and how it resolved.
- A new `resource_kinds` registry (with a builtin `file` kind) lets profiles declare resource paths
  that resolve against the project root at compile time.
- Runtime hook results are now returned alongside the run's usage summary.
- Output delivery: writers can report `output_files_root` and record an `output_destination`;
  delivered files get an adler32 checksum; `hepflow.build_layout.partition_output_roots` lists the
  top-level output directories a partition writer will create.
- Backends can now accept a shared-prefix worker environment for execution.

### Changed

- Flow now depends on `fasthep-toolbench` and uses its file-fingerprinting helper instead of its
  own (new hard dependency).
- Internal compile-time plumbing now threads one compile context through profile resolvers,
  parameter hooks and the plan, and input collection is consolidated into one place — supports the
  path-resolution and compile-bundle features above; no public API change on its own.
- The `flow.software` environment scan is now memoised, improving compile performance for large
  environments.

### Deprecated

- Resolving a relative input path against the entry file's directory (instead of the project root)
  is deprecated and now emits a `FutureWarning`. Workflows relying on paths outside the project
  root should migrate to root-relative paths before this fallback is removed.

### Fixed

- Inline variations are no longer silently skipped at nodes the variation doesn't apply to.
- Systematics now emit graphs correctly for independent variations.
- Writers named only by a path token (for example `{dataset}/{part}.root`) now get a readable name
  derived from their node id instead of the token itself.
- Compiling with a symlinked project root now keeps stable source rows and `plan_hash` across
  repeated compiles.
- `bundle_id` and `plan_hash` stay stable and the recorded invocation is sanitised, so otherwise
  identical compiles no longer drift.
- The `flow.software` hook's cost is now bounded so it doesn't scale badly with large environments.
- The entry-directory path-fallback warning is now actually visible, and each input gets exactly
  one manifest row.

## [2026.08.4] - 2026-08-28

### Added

- Progress reporting now covers distributed worker-environment preparation.
- Dask/HTCondor backends can plan and deploy portable, Pixi-packed worker environments.
- Sink nodes can declare a `node::role`, with proper scope handling.
- Dependency discovery for data-flow analysis is now more thorough.

### Changed

- Backend policy now lives in backend specifications rather than being hardcoded.
- Dask support is no longer mandatory: registry items can be declared optional, so Dask-free
  registries are possible.
- HTCondor submission is more robust: wheel builds are replaced with environment snapshots.

### Fixed

- HTCondor jobs now get an explicit walltime setting (previously unset).
- Mixed-scope inputs and empty streams are now preserved correctly instead of being dropped.
- The compiler now rejects conflicting output paths between writers instead of allowing a silent
  collision.
