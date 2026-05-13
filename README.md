# fasthep-flow

`fasthep-flow` provides the core FAST-HEP workflow language and execution engine.

It is responsible for:

* workflow compilation
* execution planning
* runtime orchestration
* registry/profile loading
* backend interfaces
* artifact lifecycle management

The Python import namespace is:

```python
import hepflow
```

## Scope

`fasthep-flow` is intentionally lightweight and domain-agnostic.

It does **not** implement:

* ROOT IO
* histogram filling
* HEP-specific transforms
* plotting/rendering
* experiment-specific analysis logic

Those capabilities are provided by companion FAST-HEP packages.

## Recommended companion packages

For High Energy Physics (HEP) workflows, most users will also want:

* `fasthep-carpenter`

  * HEP analysis transforms
  * ROOT/awkward sources and writers
  * histogramming
  * cutflows
  * object reconstruction helpers

* `fasthep-curator`

  * dataset inspection
  * schema snapshots
  * diagnostics
  * runtime hooks

* `fasthep-render`

  * plotting
  * tables
  * reports
  * render styles

* `fasthep-cli`

  * the `fasthep` command-line interface

Alternatively, install the meta package:

```bash
pip install fasthep
```

## Installation

Core workflow package only:

```bash
pip install fasthep-flow
```

Development environment:

```bash
pixi install
pixi run ci
```

## Minimal example

```python
from hepflow.api import compile_author_file, run_author_file

compile_author_file(
    "analysis/author.yaml",
    work_dir="build/example",
)

run_author_file(
    "analysis/author.yaml",
    outdir="build/example",
)
```

## Design principles

`fasthep-flow` focuses on:

* small and composable interfaces
* explicit workflow compilation stages
* registry-driven extensibility
* backend-independent execution planning
* reproducible workflow artifacts
* minimal domain assumptions

The long-term goal is to make the workflow layer reusable beyond High Energy Physics.

## Documentation

Main FAST-HEP documentation:

* [https://fast-hep.github.io](https://fast-hep.github.io)

API documentation for this package:

* [https://fasthep-flow.readthedocs.io/en/latest/](https://fasthep-flow.readthedocs.io/en/latest/)

## Repository

Main FAST-HEP repository and project links:

* [https://github.com/FAST-HEP/fasthep](https://github.com/FAST-HEP/fasthep)

## Contributing

Contribution guidelines, development setup, and project-wide documentation are maintained centrally in the main FAST-HEP repository.

## Legacy branch

The pre-split prototype implementation is preserved in the `legacy` branch.

The new `main` branch contains the split-package architecture.

## Status

FAST-HEP is currently in active pre-alpha development.

Interfaces may still evolve rapidly while the package split and stabilization work continues.
