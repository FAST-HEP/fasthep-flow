# Changelog

## Unreleased

### Migration: component registries

The deprecated `registry.ops` extension path and its `OpSpec`/`OpEntry`
dependency DSL have been removed. Register runtime components in the registry
section matching their role—`sources`, `transforms`, `observers`, or `sinks`—and
provide a component spec with declarative `requires.symbols` and
`provides.symbols` metadata where dependency discovery is needed.

Profiles that still define `registry.ops`, including an empty mapping, now fail
with a migration diagnostic instead of being silently accepted.
