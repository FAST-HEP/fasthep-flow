# Registries and profiles

Registries and profiles determine which capabilities are available when Flow compiles and executes a workflow.

They solve two related but different problems:

```text
registry
    maps names to capabilities

profile
    assembles capabilities and configuration
    into a reusable environment
```

A workflow therefore does not need to know which Python module implements every operation it uses. It selects one or more profiles, Flow resolves their contents, and compilation uses the resulting environment.

---

## Registries map names to capabilities

Author workflows refer to capabilities by declarative names:

```yaml
- id: BasicVars
  op: hep.define
```

A registry maps that name to the capability supplied by an installed package:

```yaml
registry:
  transforms:
    hep.define:
      spec: fasthep_carpenter.operations.define:DEFINE_SPEC
      impl: fasthep_carpenter.operations.define:run_define_transform
```

```{mermaid}
flowchart LR
    Author["author workflow<br/><code>hep.define</code>"]
    Registry["resolved registry"]
    Capability["fasthep-carpenter<br/>capability"]

    Author --> Registry --> Capability
```

The common relationship between specifications and implementations is described in {doc}`operations-and-specs`.

A registry can contain many kinds of capabilities:

```yaml
registry:
  sources:
    ...
  transforms:
    ...
  observers:
    ...
  sinks:
    ...
  compile_hooks:
    ...
  execution_modifiers:
    ...
  backends:
    ...
```

It can also contain supporting capabilities such as functions, constants, product handlers, and execution hooks.

The important point is that the registry provides a **common namespace through which Flow discovers capabilities**, even though those capabilities participate at different points in the workflow lifecycle.

---

## Registries are provided by packages

Extensions do not need to live inside Flow.

For example, `fasthep-carpenter` can provide:

```yaml
registry:
  sources:
    root_tree:
      spec: fasthep_carpenter.sources.root_tree:ROOT_TREE_SOURCE_SPEC
      impl: fasthep_carpenter.sources.root_tree:run_root_tree_source

  transforms:
    hep.define:
      spec: fasthep_carpenter.operations.define:DEFINE_SPEC
      impl: fasthep_carpenter.operations.define:run_define_transform

  sinks:
    root_tree:
      spec: fasthep_carpenter.sinks.root_tree:ROOT_TREE_WRITE_SPEC
      impl: fasthep_carpenter.sinks.root_tree:run_root_tree_write
```

Flow does not need to understand the scientific meaning of these capabilities. It consumes the contracts exposed by the package and orchestrates their implementations.

The same mechanism can be used by:

```text
FAST-HEP packages
experiment packages
analysis packages
user packages
```

This keeps ownership of specialised behaviour with the package that implements it.

---

## Profiles assemble environments

A registry **provides capabilities**.

A profile can combine those capabilities with other profiles and configuration to construct a useful environment.

For example, the standard `hep` profile combines several FAST-HEP components:

```yaml
includes:
  - basic
  - fasthep_carpenter:registry
  - fasthep_curator:registry
  - fasthep_curator:default_context
  - fasthep_render:registry
```

An author can then request:

```yaml
use:
  profiles:
    - hep
```

rather than listing each component individually.

Profiles can themselves include other profiles. For example:

```yaml
includes:
  - hep
  - fasthep_curator:runtime_diagnostics
```

can provide a more specialised debugging environment.

Conceptually:

```{mermaid}
flowchart TD
    Basic["basic"]
    HEP["hep"]
    Debug["hep_debug"]

    Carpenter["Carpenter"]
    Curator["Curator"]
    Render["Render"]
    Diagnostics["runtime diagnostics"]

    Basic --> HEP
    Carpenter --> HEP
    Curator --> HEP
    Render --> HEP

    HEP --> Debug
    Diagnostics --> Debug
```

The same pattern can continue outside the standard FAST-HEP packages:

```text
basic
  ↓
HEP environment
  ↓
experiment environment
  ↓
analysis environment
```

Each layer can add capabilities or configuration without modifying the layers below it.

```{note}
A registry and a profile are therefore not synonyms.

A **registry entry provides a capability**. A **profile assembles an environment** and may include registries, other profiles, and configuration.
```

---

## Package profiles

Profiles provided by installed packages can be referenced as:

```text
package_name:profile_name
```

For example:

```yaml
use:
  profiles:
    - fasthep_workshop:registry
```

refers to the `registry` profile distributed by the `fasthep-workshop` package.

In the source tree, this can for example correspond to:

```text
src/
└── fasthep_workshop/
    └── registry.yaml
```

so that:

```text
fasthep_workshop:registry
        ↓
src/fasthep_workshop/registry.yaml
```

The package-resource syntax allows workflows to refer to configuration distributed with installed Python packages without depending on their physical installation path.

The same package can provide additional profiles. For example:

```text
my_experiment:analysis
```

might select an `analysis` profile provided by `my_experiment`.

Installing a package alone does not activate its capabilities. The relevant profile must be selected explicitly or included by another active profile.

This avoids implicit plugin discovery: installing an unrelated Python package does not silently change the meaning of existing workflows.

---

## Layering and overrides

Profiles are resolved in order into the environment used for compilation.

If a later registry layer defines the same capability as an earlier layer, it can replace that registration.

For example:

```text
standard profile
    hep.operation → implementation A

experimental profile
    hep.operation → implementation B
```

The author can still write:

```yaml
op: hep.operation
```

while selecting an environment in which a different implementation is used.

This can support:

- alternative algorithms
- accelerated implementations
- experiment-specific behaviour
- testing new implementations of existing contracts

The replacement must still satisfy the contract Flow relies upon.

The author configuration forms the final layer of the resolved environment, allowing reusable profiles to provide defaults while individual workflows retain control over their final configuration.

```{note}
Compilation always stores the fully resolved registry.

This makes the exact capability mapping used to compile a workflow available for reproducibility and comparison, including cases where profile layering has replaced an earlier registration.
```

---

## Resolution is recorded

The environment used during compilation affects the meaning of the workflow, so Flow records how it was assembled.

For example, the NASA workflow records:

```yaml
provenance:
  registry_layers:
    - name: builtin
      kind: builtin

    - name: registry
      kind: profile
      path: package:hepflow.profiles/registry.yaml

    - name: fasthep_workshop:registry
      kind: profile
      path: package:fasthep_workshop.profiles/registry.yaml

    - name: author
      kind: author
      path: examples/NASA/exoplanets/author.yaml
```

Registry provenance can also record which layer introduced or replaced individual capabilities.

This allows questions such as:

```text
Which capability was resolved?
Where did it come from?
Was an earlier definition replaced?
Which profile introduced it?
```

to be answered from compilation output rather than hidden runtime state.

Package-version provenance is still evolving. Ultimately, reproducibility should also make it possible to determine which installed package version supplied a resolved capability.

---

## The resolved registry travels with the plan

Once profiles have been resolved, the resulting registry state is included in the compiled representation.

For example:

```yaml
registry:
  transforms:
    workshop.tabular.filter:
      spec: fasthep_workshop.transforms.tabular:TABULAR_FILTER_SPEC
      impl: fasthep_workshop.transforms.tabular:run_tabular_filter

  backends:
    local.default:
      impl: hepflow.backends:Local
```

The runtime therefore does not need to rediscover the intended environment from the original author file.

Conceptually:

```text
author description
       +
profiles
       +
registries
       ↓
resolved environment
       ↓
execution plan
       ↓
runtime
```

See {doc}`../execution/compilation` for the complete compilation process.

---

## Choosing between a registry and a profile

A useful rule of thumb is:

> Use a **registry entry** to provide a capability.

> Use a **profile** to assemble an environment.

For example:

```yaml
registry:
  transforms:
    my.calculate:
      spec: ...
      impl: ...
```

provides `my.calculate`.

A profile such as:

```yaml
includes:
  - hep
  - my_package:registry
  - my_package:diagnostics
```

assembles capabilities and configuration into something an author can activate with one name.

Many packages therefore expose a `registry` profile as their lowest-level capability bundle and additional profiles for more opinionated environments.

---

## Where next?

With the extension environment established, the remaining pages describe the individual capability roles.

Continue with:

- {doc}`sources` for introducing data into a workflow
- {doc}`transforms` for product-to-product computation
- {doc}`observers` for inspection and diagnostics
- {doc}`sinks` for producing outputs and artifacts
- {doc}`compile-hooks` for extending compilation
- {doc}`execution-modifiers` for adapting runtime execution
- {doc}`backends` for mapping execution onto computing infrastructure

For the common operation contract behind sources, transforms, observers, and sinks, see {doc}`operations-and-specs`.
