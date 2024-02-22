# Provenance

## What is Provenance?

Provenance refers to the detailed history of the origin, lineage, and changes
made to data throughout its lifecycle. It encompasses the documentation of
processes, inputs, outputs, and transformations that data undergoes, providing a
comprehensive audit trail that can be used to verify the data's integrity and
authenticity.

## Why Does Provenance Matter for Scientific Data Analysis?

In scientific data analysis, provenance is crucial as it ensures the
reproducibility and reliability of results. It enables researchers to trace back
through the analysis workflow to understand how data was altered, what
computational steps were performed, and by whom. This traceability is essential
for validating research findings, facilitating peer reviews, and enabling other
researchers to replicate and build upon the work.

## Our Approach to Provenance in the YAML Config

To integrate provenance into our workflows, we introduce a dedicated provenance
section within the YAML configuration. This section describes which metadata
should be captured, e.g. version of the dataset used, the origin of the data,
the specific parameters set for each analysis stage, and the individual
responsible for each step (taken from git history). By embedding this
information directly into the workflow configuration, we ensure that every step
of data processing is transparent and traceable. This not only adheres to best
practices in scientific data handling but also empowers users to conduct robust
and transparent analyses.

### Example

```yaml
provenance:
  datasets:
    source: fasthep-curator # Specifies the tool used for dataset curation
  analysis:
    include:
      - steps # Enumerates the individual steps taken in the analysis
      - parameters # Parameters used at each step for reproducibility
      - git # Git commit hash, branch, and status for version control
      - performance # Metrics to measure the efficiency of the analysis
      - environment # Software environment, including library versions
      - hardware # Hardware specifications where the analysis was run
  airflow:
    include:
      - db # Database configurations and states within Airflow
```
