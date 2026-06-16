# Concepts

FAST-HEP separates three concerns:

- Workflows describe scientific intent.
- Execution infrastructure describes where work runs.
- Extensibility describes how new capabilities are added.

The concepts in this section explain the core ideas behind the execution and workflow model.

## Key Principles

### Portability

Workflows should run locally or on distributed infrastructure with minimal changes.

### Reproducibility

Execution behaviour should be explicit and version controlled.

### Scalability

The same workflow language should support both exploratory analysis and large-scale production processing.

### Separation of Concerns

Analysis logic should remain independent from execution infrastructure.

```{toctree}
:maxdepth: 1

execution-infrastructure
resource-pools
worker-environments
credentials
```