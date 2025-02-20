# fasthep-flow

[![Actions Status][actions-badge]][actions-link]
[![Documentation Status][rtd-badge]][rtd-link]

[![PyPI version][pypi-version]][pypi-link]
[![PyPI platforms][pypi-platforms]][pypi-link]

[![GitHub Discussion][github-discussions-badge]][github-discussions-link]

<!-- SPHINX-START -->

<!-- prettier-ignore-start -->
[actions-badge]:            https://github.com/FAST-HEP/fasthep-flow/workflows/CI/badge.svg
[actions-link]:             https://github.com/FAST-HEP/fasthep-flow/actions
[github-discussions-badge]: https://img.shields.io/static/v1?label=Discussions&message=Ask&color=blue&logo=github
[github-discussions-link]:  https://github.com/FAST-HEP/fasthep-flow/discussions
[pypi-link]:                https://pypi.org/project/fasthep-flow/
[pypi-platforms]:           https://img.shields.io/pypi/pyversions/fasthep-flow
[pypi-version]:             https://img.shields.io/pypi/v/fasthep-flow
[rtd-badge]:                https://readthedocs.org/projects/fasthep-flow/badge/?version=latest
[rtd-link]:                 https://fasthep-flow.readthedocs.io/en/latest/?badge=latest

<!-- prettier-ignore-end -->

## Introduction

> [!NOTE] `fasthep-flow` is still in early development, which means it is
> incomplete and the API is not yet stable. Please report any issues you find on
> the [GitHub issue tracker](https://github.com/FAST-HEP/fasthep-flow/issues).

`fasthep-flow` is a package for describing data analysis workflows in YAML and
converting them into serializable compute graphs that can be evaluated by
software like [Dask](https://www.dask.org/), as
[Interaction Combinator](https://www.semanticscholar.org/paper/Interaction-Combinators-Lafont/6cfe09aa6e5da6ce98077b7a048cb1badd78cc76)
or step-wise for with workflow managers like
[Snakemake](https://snakemake.readthedocs.io/en/stable/). `fasthep-flow` is
designed to be used with the [FAST-HEP](https://fast-hep.github.io/) package
ecosystem, but can be used independently.

The primary use-case of this package is to define a data processing workflow,
e.g. a High-Energy-Physics (HEP) analysis, in a YAML file, and then convert that
YAML file into a compute graphs that can be converted in meaningful node sets to
Direct Acyclic Graphs (DAGs) that can be processed by external (to fasthep-flow)
executors. These executors can then run on a local machine, or on a cluster
using [CERN's HTCondor](https://batchdocs.web.cern.ch/local/submit.html) (via
Dask) or [Google Cloud Composer](https://cloud.google.com/composer).

In `fasthep-flow`'s YAML files draw inspiration from Continuous Integration (CI)
pipelines and Ansible Playbooks to define the workflow, where each independent
task that can be run in parallel. `fasthep-flow` will check the parameters of
each task, and then generate the compute graph. The compute graph consists of
nodes that describe input/output data and the compute task and edges for the
dependencies between the tasks.

## Documentation

This project is in early development. The documentation is available at
[fasthep-flow.readthedocs.io](https://fasthep-flow.readthedocs.io/en/latest/)
and contains mostly fictional features. The most useful information can be found
in the [FAST-HEP documentation](https://fast-hep.github.io/). It describes the
current status and plans for the FAST-HEP projects, including `fasthep-flow`
(see [Developer's Corner](https://fast-hep.github.io/developers-corner/)).

## Installation

```bash
pip install fasthep-flow[dask, visualisation]
```

## Examples

```bash
fasthep-flow execute docs/examples/hello_world.yaml
# example with plugins
fasthep-flow execute tests/data/plugins.yaml --dev --save-path=$PWD/output
```

## Contributing

You had a look and are interested to contribute? That's great! There are three
main ways to contribute to this project:

1. Head to the [issues tab](https://github.com/FAST-HEP/fasthep-flow/issues) and
   see if there is anything you can help with.
2. If you have a new feature in mind,
   [please open an issue](https://github.com/FAST-HEP/fasthep-flow/issues/new)
   first to discuss it. This way we can ensure that your work is not in vain.
3. You can also help by improving the documentation or fixing typos.

Once you have something to work on, you can have a look at the
[contributing guidelines](./.github/CONTRIBUTING.md). It contains
recommendations for setting up your development environment, testing, and more
(compiled by the Scientific Python Community). That said, how you customise your
development environment is up to you. You like
[uv](https://github.com/astral-sh/uv)? Be our guest. You prefer
[nox](https://nox.thea.codes/en/stable/)? That's fine too. You want to use
<your custom workflow>? Go ahead. We are happy as long as you are happy. Ideally
you should be able to run `pylint`, `pytest`, and the pre-commit hooks. If you
can do that, you are good to go.

If you are looking for example workflows to run, have a look at the
`tests/data/` directory. Each of the configs can be run via

```bash
fasthep-flow execute <config> --dev --save-path=$PWD/output
```

The `--dev` flag will enable the development mode, which will allow you to
(optionally) overwrite the workflow snapshot. This is useful for changes in
`fasthep-flow` code without changes to the config file.

## License

This project is licensed under the terms of the Apache 2.0 license. See
[LICENSE](./LICENSE) for more details.

## Acknowledgements

Special thanks to the gracious help of FAST-HEP contributors:

<!-- readme: m-glowacki,seriksen,collaborators,contributors -start -->
<table>
	<tbody>
		<tr>
            <td align="center">
                <a href="https://github.com/m-glowacki">
                    <img src="https://avatars.githubusercontent.com/u/69155366?v=4" width="100;" alt="m-glowacki"/>
                    <br />
                    <sub><b>Maciek Glowacki</b></sub>
                </a>
            </td>
            <td align="center">
                <a href="https://github.com/seriksen">
                    <img src="https://avatars.githubusercontent.com/u/5619270?v=4" width="100;" alt="seriksen"/>
                    <br />
                    <sub><b>Null</b></sub>
                </a>
            </td>
            <td align="center">
                <a href="https://github.com/kreczko">
                    <img src="https://avatars.githubusercontent.com/u/1213276?v=4" width="100;" alt="kreczko"/>
                    <br />
                    <sub><b>Luke Kreczko</b></sub>
                </a>
            </td>
		</tr>
	<tbody>
</table>
<!-- readme: m-glowacki,seriksen,collaborators,contributors -end -->
