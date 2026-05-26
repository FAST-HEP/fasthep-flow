# Compilation pipeline

A FAST-HEP workflow typically begins as a human-authored YAML file:

```text
author.yaml
```

`fasthep-flow` then compiles this workflow through several stages:

```{mermaid}
flowchart TD

    subgraph Compile["Compilation and planning"]
        Author["author.yaml"]
        Profiles["profiles and registries"]
        Normalised["normalised workflow"]
        Dependency["dependency inference"]
        Plan["execution plan"]

        Author --> Normalised
        Profiles --> Normalised
        Normalised --> Dependency
        Dependency --> Plan
    end

    subgraph Execute["Runtime execution"]
        Runtime["runtime execution"]
        Outputs["artifacts and outputs"]

        Runtime --> Outputs
    end

    Plan --> Runtime
```

This separation between *workflow intent* and *execution strategy* is a core design principle of `fasthep-flow`.

Workflows can therefore be:

- validated before execution
- inspected and serialised
- transformed into backend-specific plans
- optimised independently of user workflow logic
- executed on different runtime backends without changing analysis definitions

---

## Compilation stages

The compilation pipeline progressively transforms a human-authored workflow into a runtime-ready execution plan.

### Author workflow

The workflow begins as a user-authored YAML description:

```yaml
analysis:
  stages:
    - id: BasicVars
      op: hep.define
```

At this stage the workflow focuses on readability and intent rather than runtime structure.

---

### Profiles and registries

Profiles are resolved and registries are loaded.

This stage makes operations, sources, sinks, hooks, and rendering implementations available to the workflow compiler.

For example:

```yaml
use:
  profiles:
    - registry
    - fasthep_carpenter:registry
```

loads operations from the FAST-HEP ecosystem or custom user operations into the active workflow.

---

### Normalised workflow

The workflow is then transformed into a normalised internal representation.

This stage may:

- resolve defaults
- expand shorthand syntax
- apply profile-provided defaults
- resolve reusable styles
- construct explicit workflow objects

The resulting workflow representation is more explicit and machine-oriented than the original author YAML.

---

### Dependency inference

`fasthep-flow` then infers workflow dependencies automatically.

For example:

```yaml
expr: "sqrt(Muon_Px ** 2 + Muon_Py ** 2)"
```

implicitly depends on `Muon_Px` and `Muon_Py`


The workflow engine therefore constructs dependency edges automatically without requiring users to manually wire execution graphs together.

```{note}
Dependency inference currently focuses primarily on workflow structure and operation relationships.

More advanced validation and semantic inspection tooling is still evolving as part of the rewrite.
```

---

### Execution plans

The final result of compilation is a serialisable execution plan:

```text
plan.yaml
```

Execution plans contain:

- resolved workflow structure
- explicit dependencies
- runtime configuration
- backend information
- execution ordering
- operation metadata

Plans are intended to be:

- inspectable
- reproducible
- serialisable
- largely backend-independent

The execution plan acts as the boundary between workflow compilation and runtime execution.

```{note}
Execution plans may contain backend-specific runtime configuration while still remaining largely backend-independent.

For example, the same workflow plan may be executed:

- locally
- with Dask
- through workflow managers
- with alternative runtime implementations

without changing the original author workflow.

Backend configuration may therefore be embedded, overridden, or replaced at runtime depending on the execution environment.
```

---

## CLI usage

Workflows are most commonly compiled and executed through the `fasthep` command-line interface.

Typical workflows move through several stages:

```text
author.yaml
  → normalised workflow
  → execution plan
  → runtime execution
```

The CLI exposes commands for inspecting and interacting with these stages individually.

---

### Workflow compilation

Compile and execute a workflow directly:

```bash
fasthep run author.yaml
```

This performs:

1. workflow loading
2. profile resolution
3. normalisation
4. dependency inference
5. execution planning
6. runtime execution

---

### Normalisation

Inspect the normalised workflow representation:

```bash
fasthep normalise author.yaml
```

or:

```bash
fasthep normalize author.yaml
```

This expands defaults, resolves profiles, and produces a more explicit workflow representation.

---

### Plan generation

Generate a serialisable execution plan without executing the workflow:

```bash
fasthep make-plan author.yaml
```

or:

```bash
fasthep compile author.yaml
```

This produces:

```text
plan.yaml
```

which can later be executed independently of the original author workflow.

---

### Plan execution

Execute a previously generated plan:

```bash
fasthep run-plan plan.yaml
```

---

## Python API usage

Workflows may also be compiled programmatically through Python APIs.

This is useful for:

- notebooks
- custom tooling
- workflow services
- testing
- alternative runtimes
- experimental optimisation pipelines

A typical workflow compilation flow looks conceptually like:

```python
from hepflow.api import load_workflow, compile_workflow

workflow = load_workflow("author.yaml")
plan = compile_workflow(workflow)
```

The exact APIs are still evolving during the rewrite.

---

## Runtime execution

Once compiled, execution plans may be evaluated by different runtime backends.

Current and planned backends include:

- local execution
- Dask-based distributed execution
- workflow-manager orchestration
- experimental optimisation and execution backends

Backends are discussed in more detail in {doc}`../extensibility/backends`.

---

## Next steps

Continue with:

- {doc}`execution-model`
- {doc}`../extensibility/profiles-and-registries`
- {doc}`../extensibility/operations-and-specs`
