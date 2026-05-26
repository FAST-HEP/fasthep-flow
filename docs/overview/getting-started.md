# Getting started

`fasthep-flow` is typically used through the `fasthep` command-line interface and the broader FAST-HEP package ecosystem.

The recommended installation method is the `fasthep` meta package, which provides curated installation profiles for different use cases.

```{warning}
The FAST-HEP ecosystem is currently undergoing a major rewrite and alpha-stage reorganisation.

Not all packages are available on PyPI yet, and some installation details may change while the ecosystem stabilises.

We will try to keep the main installation entry points stable for their intended use cases.
```

---

## Installation profiles

The `fasthep` meta package provides several installation profiles.

---

## Minimal installation

Recommended for:

- workflow language exploration
- backend/runtime development
- lightweight experimentation
- non-HEP workflows

Installs:

- `fasthep-flow`
- `fasthep-cli`

```bash
pip install "fasthep[minimal]"
```

This is the smallest supported FAST-HEP installation profile without any HEP-specific dependencies.

---

## HEP installation

Recommended for most HEP users.

Includes:

- workflow execution
- ROOT and awkward support
- transforms and histogramming
- rendering
- diagnostics
- CLI tooling

```bash
pip install "fasthep[hep]"
```

This is expected to become the standard installation profile for typical HEP analyses.

---

## Full installation

Installs the complete FAST-HEP ecosystem and optional tooling.

```bash
pip install "fasthep[full]"
```

This profile is mainly intended for:

- developers
- integration testing
- ecosystem experimentation

---

## Verifying the installation

After installation, verify that the CLI is available:

```bash
fasthep version
```

You can also inspect installed package versions:

```bash
fasthep versions
```

---

## Next steps

There are two common paths after installation.

### Learn the workflow system

If you want to understand how `fasthep-flow` works internally, continue with:

1. {doc}`workflow-language`
2. {doc}`compilation-pipeline`
3. {doc}`execution-model`

These pages explain:

- declarative workflows
- dependency inference
- execution planning
- runtime orchestration
- backend abstraction
- serialisable execution plans

---

### Start building workflows immediately

If you want to start running workflows and tutorials right away, head to the FAST-HEP Workshop:

- [FAST-HEP Workshop docs](https://fasthep-workshop.readthedocs.io/en/latest/index.html)

The workshop contains:

- beginner tutorials
- runnable example workflows
- toy datasets
- rendering examples
- workflow debugging exercises
- advanced backend and extensibility examples

The workshop is the recommended hands-on entry point for most users.

---

## Development installations

For active development, editable installs, and integration testing, see https://github.com/FAST-HEP/fasthep-dev
