# Paths and the project root

A workflow refers to files: correction payloads, data-quality masks, mapping and schema
files, profiles. Authors write these paths **relative**; the compiler turns them into
absolute paths in the compiled plan. A plan therefore runs anywhere (for example in an
HTCondor job directory) without depending on the directory it is started from.

## The project root

Relative paths resolve against the **project root**:

- by default the current working directory of the compile (`fasthep compile`,
  `compile_workflow_file`);
- or the directory given explicitly with `root=` (the Python API) or `--root` (the
  command line, once the CLI exposes it).

The root must be an existing directory. Compile from the project directory, or pass `root=`
explicitly, when the entry workflow lives elsewhere. It is recorded in `compile/inputs.manifest.yaml`
and in the plan provenance (`provenance.root`).

Includes are different: an `include:` path resolves against the file that contains it.
Includes describe the structure of a workflow, not the files it uses.

## What is resolved

Only **input** paths, files the compiler can read:

| Where | How it is declared |
|---|---|
| `execution.resources` entries | the resource's `kind` lists its path fields in `registry.resource_kinds` |
| stage parameters | the component spec marks the parameter with `kind: path` |
| mapping and schema files | `flow.load_mapping` parameters (for example `align_schema.schema`) |
| profile files | `use.profiles` entries that are paths |

Output paths (a writer's `path`, `output_destination`, build directories) are never
resolved or hashed; they follow the output layout rules.

```yaml
# registry (provided by the package that owns the resource kind)
resource_kinds:
  data_quality_mask:
    path_fields: [paths]

# workflow
execution:
  resources:
    cms.lumimask:
      kind: data_quality_mask
      paths:
        - data/lumimasks/Cert_Collisions2022_355100_362760_Golden.json
```

The compiled plan holds the absolute path of each file. A resource whose kind is not in
`resource_kinds` is left untouched and the compiler warns, because it cannot know which
fields are paths. Flow provides the generic kind `file` (`path_fields: [path]`).

## What is left alone

- absolute paths are kept as they are;
- values with a URI scheme (`root://`, `https://`, `file://`) are kept and never hashed;
- `~` expands to the home directory at compile time;
- environment variables are **not** expanded.

## Checks and hashes

```{warning}
Every declared input path is opened and hashed. Do not declare credential files (an X509
proxy, a token) as input paths: the compiler would read them and record a hash of their
contents in the plan and the manifest. Site and user settings such as a proxy path belong
in the execution configuration, not in `execution.resources`.
```

A relative path must exist under the root, otherwise the compile fails and the message
names the resource or parameter. Every resolved file is hashed (sha256). An absolute path
that cannot be read at compile time, for example `/cvmfs/...` on a laptop, is recorded
without a hash and the compiler warns: the machine that runs the plan may differ from the
machine that compiled it.

## The inputs manifest

Every compile writes `compile/inputs.manifest.yaml`, the manifest of the compile bundle
(fasthep-flow#94). It is version 2 and lists everything that went into the plan: the
workflow, its includes and profile files, the resource path fields, the `kind: path`
parameters and the files loaded by `flow.load_mapping`:

```yaml
version: 2
root: /path/to/project
bundle_id: 71b8...
plan_hash:
  sha256: bf9a...
  canonicalization: root-prefix
software:
  path: software.json
  sha256: 717d...
  size: 14260
invocation:
  workflow_path: /path/to/project/workflow.yaml
  outdir: /path/to/build
  root: /path/to/project
  timestamp: '2026-10-25T22:03:21+00:00'
  hostname: lxplus9108.cern.ch
  caller: {}
  chunk_size: null
hooks:
  - name: flow.software
    phase: after_datasets
    params: {}
    artifacts:
      software:
        path: software.json
        sha256: 717d...
inputs:
  - role: resource:cms.lumimask.paths[0]
    declared: data/lumimasks/Cert_Collisions2022_355100_362760_Golden.json
    resolved: /path/to/project/data/lumimasks/Cert_Collisions2022_355100_362760_Golden.json
    origin: root          # root | absolute | uri | entry_fallback
    sha256: 3b1f...
    size: 18432
    note: null
```

The top-level keys, in this order:

| Key | Meaning |
|---|---|
| `version` | manifest format, `2` |
| `root` | the project root of the compile |
| `bundle_id` | identity of the whole compile bundle (see below) |
| `plan_hash` | hash of `plan.yaml` that does not depend on the project root (see below) |
| `software` | `software.json` listed with its `sha256` and `size`; `null` when no compile hook wrote it |
| `invocation` | how the compile was called: `workflow_path`, `outdir`, `root`, `timestamp`, `hostname`, the caller's own options (`caller`) and `chunk_size` |
| `hooks` | the phase compile hooks that ran (`after_datasets`, `after_compile`), with the parameters they ran with and the artifacts they wrote |
| `inputs` | one row per input file |

In `inputs`, `declared` is what the author wrote, `resolved` what ended up in the plan. For
the `workflow`, `include` and `profile` rows `declared` is the absolute path of the file, not
the text the author wrote. Rows are sorted by `role`. The roles are:

- `workflow`: the entry workflow file;
- `include`: a file pulled in by `include:`;
- `profile`: a profile file from `use.profiles`;
- `resource:<id>.<field>`: a resource path field;
- `param:<stage>.<name>`: a `kind: path` parameter;
- the files loaded by `flow.load_mapping`.

Parameter compile hooks such as `flow.load_mapping` are not listed under `hooks`; their
records stay in the plan node `meta`, as before. Values in `caller` and in hook parameters
are stored as plain YAML: paths become strings, sets sorted lists, and any other object its
`repr`.

### `plan_hash` and `bundle_id`

`plan_hash` is the sha256 of `plan.yaml`, loaded and dumped again without line wrapping,
after every occurrence of the project root has been replaced by a placeholder
(`canonicalization: root-prefix`). The same project compiled under
two different roots, for example in two checkouts, gives the same `plan_hash`. It tells you
whether the plan content is the same, wherever it was compiled. `plan_hash` is the portable
value to compare across machines and checkouts.

`bundle_id` is the sha256 of the manifest without `invocation` and without the `bundle_id`
key itself. A hook counts with its name, phase, parameters and the names of its artifacts,
not with the artifacts' `sha256` (a rendered graph, for example, names files below the
output directory). It identifies the whole bundle, so it is **not** root independent. It
changes with:

- the project root and the resolved paths, because the manifest records them;
- the content of any input file (the input hashes and the `plan_hash` change);
- the software environment, through `software.json`: the platform string (which includes
  the kernel release), the interpreter path and prefix, the installed distributions and,
  with the default `git` capture, the commit and the dirty state of every editable
  checkout. A new commit in an editable checkout changes the `bundle_id`. A git call that
  times out (5 s per call) records a `note` instead of the revision, so it can change the
  `bundle_id` too.

It does not change with the output directory, the time or the invocation options, so
compiling the same bundle again in the same environment gives the same `bundle_id`. `chunk_size` is recorded in
`invocation`, but it also changes the partitioning in the plan, so it changes the
`plan_hash` and with it the `bundle_id`.

`run_summary.yaml` carries the `bundle_id` of the plan it ran, as a top-level key. A plan
compiled by an older Flow has no manifest: the run works and the summary has no
`bundle_id` key.

### Software

`software` is a reference, not an inlined record: `software.json` sits next to the manifest
and is listed with its `sha256` and `size`. The built-in compile hook `flow.software` writes
it (the hook can be replaced in the registry). It records the Python version and executable,
the installed distributions, the lock file and the git state (commit and dirty flag) of the
project and of editable checkouts. The
hook parameter `git: false` switches the git capture off; the whole `git` value is then
`null` and a sibling `git_note` says why. When git is missing or fails, each path entry
(`git.root` and every `git.editable` entry) carries a `note` instead. A packaged profile
layer is listed in `layers` by its distribution name; when no installed distribution matches
its import package, the import name is listed and `layer_notes` says so. Within one process
the distribution scan and the git states are reused for up to 30 seconds. The only environment variables recorded are those starting with
`FASTHEP_` or `HEPFLOW_`.

### What is never recorded

- the full environment: only the `FASTHEP_*` and `HEPFLOW_*` variables are captured;
- secrets: do not declare credential files as inputs (see above). Hook parameters are
  recorded in the manifest exactly as written in the registry, so never put a secret in
  one.

## Migrating from entry-directory paths

Before the project root existed, profile paths, `flow.load_mapping` files and
`align_schema.schema` files resolved against the directory of the entry workflow. For one
release a path that is not found under the root falls back to the entry directory and the
compiler emits a `FutureWarning` (`origin: entry_fallback` in the manifest; it is a
`FutureWarning` rather than a `DeprecationWarning` because Python hides the latter from end users). Move such
files, or adjust the paths, so that they resolve against the project root.
