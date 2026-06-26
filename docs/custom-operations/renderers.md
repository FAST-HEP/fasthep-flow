# Rendering components

From Flow's perspective, a render operation is just a sink:

```yaml
registry:
  sinks:
    hep.render.hist1d:
      spec: fasthep_render.sinks.hist1d:HIST1D_RENDER_SPEC
      impl: fasthep_render.sinks.hist1d:run_hist1d_render
```

The sink's `ComponentSpec` declares the execution contract and any dependency
contract. Flow loads the sink, passes normalized sink params through,
invokes the implementation, and tracks the returned artifact or product result.

Plot-specific params are intentionally opaque to Flow. For example, Flow passes
fields such as `axes`, `style`, `legend`, `data_mc`, `heatmap2d`,
`comparison`, and `project` through as component params; validation and parsing
are the render package's responsibility.

Third-party render extensions should register sink components under
`registry.sinks`. The old `registry.renderers` section is no longer supported.
