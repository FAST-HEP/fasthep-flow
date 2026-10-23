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

Every compile writes `compile/inputs.manifest.yaml`. It lists the resource path fields, the
`kind: path` parameters and the files loaded by `flow.load_mapping`. Profile files are
resolved by the same rule but are not recorded in the manifest yet (the reproducible compile
bundle, fasthep-flow#94, extends it):

```yaml
version: 1
root: /path/to/project
inputs:
  - role: resource:cms.lumimask.paths[0]
    declared: data/lumimasks/Cert_Collisions2022_355100_362760_Golden.json
    resolved: /path/to/project/data/lumimasks/Cert_Collisions2022_355100_362760_Golden.json
    origin: root          # root | absolute | uri | entry_fallback
    sha256: 3b1f...
    size: 18432
    note: null
```

`declared` is what the author wrote, `resolved` what ended up in the plan. Rows are sorted
by `role`, so two compiles of the same inputs give the same file.

## Migrating from entry-directory paths

Before the project root existed, profile paths, `flow.load_mapping` files and
`align_schema.schema` files resolved against the directory of the entry workflow. For one
release a path that is not found under the root falls back to the entry directory and the
compiler emits a `FutureWarning` (`origin: entry_fallback` in the manifest; it is a
`FutureWarning` rather than a `DeprecationWarning` because Python hides the latter from end users). Move such
files, or adjust the paths, so that they resolve against the project root.
