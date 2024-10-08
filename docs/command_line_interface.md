# Command Line Interface

fasthep-flow provides a command line interface (CLI) for interacting with the
package. The CLI is available either directly via the `fasthep-flow` command or
via `fasthep` command (requires `fasthep-cli` to be installed). The former is
recommended for development, while the latter is recommended for users. The two
methods are equivalent, all `fasthep-flow` commands map onto `fasthep flow`
commands. For simplicity, we will use `fasthep flow` in the following.

## Basic usage

The `fasthep flow` command has a number of subcommands, which can be listed
using `fasthep flow --help`. The most common subcommands are `fasthep flow lint`
and `fasthep flow run`.

### Linting

The `fasthep flow lint` command can be used to check the syntax of a workflow
file. For example, to check the syntax of the
[CMS Public Tutorial example](https://fast-hep.github.io/examples/cms_pub_example.html),
run:

```bash
fasthep flow lint examples/cms_pub_example.yaml
```

The command will return `0` if the syntax is correct, and `1` if there is an
error.

### Running

The `fasthep flow run` command can be used to run a workflow file. For example,
to run the
[CMS Public Tutorial example](https://fast-hep.github.io/examples/cms_pub_example.html),
run:

```bash
fasthep flow run \
    examples/cms_pub_example.yaml \
    --output examples/output/cms_pub_example
```

You can also specify the number of events to process, the executor to use, and
the task to run:

```bash
fasthep flow run \
    examples/cms_pub_example.yaml \
    --output examples/output/cms_pub_example \
    --n-events 1000 \
    --executor dask-local \
    --tasks "Select events", "Histograms after selection"
```

This will run the workflow for 1000 events, using the `dask-local` executor, and
only run the `Select events` and `Histograms after selection` tasks.
