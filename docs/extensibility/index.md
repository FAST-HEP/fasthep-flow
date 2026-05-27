# Extensibility

`fasthep-flow` is designed as a composable workflow compilation and orchestration framework.

Most workflow functionality is not hardcoded into the core runtime. Instead, capabilities are introduced dynamically through:

- profiles and registries
- runtime backends and execution strategies

This allows workflows to remain:

- modular
- backend-independent
- reusable
- domain-extensible

This system enables users and packages to contribute:

- (data) {doc}`sources <../custom-operations/sources>`
- {doc}`transforms <../custom-operations/transforms>`
- {doc}`sinks <../custom-operations/sinks>`
- {doc}`renderers <../custom-operations/renderers>`
- {doc}`hooks <../custom-operations/hooks>`
- execution strategies
- backend integrations

without modifying the core workflow language itself.

```{note}
While FAST-HEP workflows are commonly authored as YAML files, the execution plan itself is the true runtime boundary.

In principle, alternative frontend languages and workflow formats could generate compatible execution plans without using the standard `author.yaml` compilation pipeline.

This would allow:

- custom workflow languages
- graphical workflow editors
- notebook-driven workflow generation
- domain-specific frontends
- alternative serialisation formats

while still targeting the same execution and backend infrastructure.

At present, such alternative compilation frontends would need to exist outside of `fasthep-flow` itself.
```

The following sections describe the major extensibility mechanisms used throughout the FAST-HEP ecosystem.

```{toctree}
:maxdepth: 2

profiles-and-registries
operations-and-specs
strategies-and-backends
```
