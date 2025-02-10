"""THIS IS A DRAFT
Defines the FAST-HEP flow DSL specification.

The DSL defines how a config is interpreted and how the tasks are resolved.
All implementations have to return a valid fasthep_flow.Workflow object.

A particular DSL can be selected with the `version` key in the config:

```yaml
version: v0
```

or more explicitly:

```yaml
version: fasthep_flow.dsl.v0
```
"""

from __future__ import annotations

from .default import v0

__all__ = ["v0"]
