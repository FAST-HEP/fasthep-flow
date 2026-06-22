# Creating Custom Sinks

## Attaching a writer

Writers are currently attached to the analysis stage whose output they consume by
using the stage's `write` field. For example, the ROOT-tree writer supplied by
`fasthep-carpenter` is configured as follows:

```yaml
analysis:
  stages:
    - id: SelectDimuonEvents
      op: hep.selection.cutflow
      params:
        selection:
          dimuon_candidates:
            - "NIsolatedMuon >= 2"
      write:
        - kind: root_tree
          path: dimuon_candidates.root
          tree: events
```

The author format does **not** currently support a top-level `sinks:` block. The
compiler reports an error if one is present; use `analysis.stages[].write`
instead. A writer is an attachment, not an `analysis.stages` operation of its
own.

Relative writer `path` values are resolved below the run output directory at
`artifacts/files/`. Writers run once per input partition by default, so FAST-HEP
expands a path such as `dimuon_candidates.root` to paths of the form
`artifacts/files/dimuon_candidates/<dataset>/<part>.root`. This prevents files
from different datasets or partitions from overwriting one another. Absolute
paths remain outside `artifacts/files/`, with the same per-partition expansion.
