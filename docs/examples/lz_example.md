# LUX-ZEPLIN Analysis tutorial

[LUX-ZEPLIN](https://lz.lbl.gov/) (LZ) is a direct detection Dark Matter
experiment located at the Sanford Underground Research Facility
([SURF](https://sanfordlab.org/)) in Lead, South Dakota. It is a dual-phase
xenon time projection chamber, with a fiducial mass of 5.6 tonnes of liquid
xenon. The goal of the experiment is to detect the scattering of Weakly
Interacting Massive Particles (WIMPs) off xenon nuclei. The experiment has been
taking data since 2021.

When a WIMP scatters off a xenon nucleus, it produces a small amount of light
and produces free electrons. This prompt light signal is usually referred to as
S1. The electrons are drifted to the top of the detector by an electric field,
where they are extracted into the gas phase. In the gas phase, the electrons
produce a second light signal, usually referred to as S2. The time between the
two light signals is used to determine the depth of the interaction, and the
amount of light is used to determine the energy of the interaction:

```{figure} images/LZ_DetectorInteraction.jpg
---
class: with-border
scale: 50%
---
A single scatter of an incoming particle with the liquid xenon.
```

In this example we'll generate a Single Scatters
Log<sub>10</sub>(S2<sub>c</sub>) vs S1<sub>c</sub> histogram with hyper from
Xe127 MDC3 data.

The steps for this analysis are:

1. **Select data**: select corrected S1 and S2 for entries identified as a
   single scatter
2. **Create histograms** of the S1 and S2 phd values to fill a 2D histogram
3. Present the results in a **publication-ready plot**.

## Setup

## Preparing the data

TBD

## Putting together the workflow

### Input data

The first step is to define the input data. In this case, we will use the output
from the fasthep-curator step and pass it to the first stage of the workflow.

```yaml
stages:
  - name: Input data
    type: fasthep_carpenter.operators.InputDataOperator
    kwargs:
      curator_config: "/path/to/curator.yaml"
      split_strategy: "file"
      split_kwargs:
        n: 1
      method: uproot5
```

We typically would only need the `name`, `type`, and `curator_config` here as
the other values are defaults. However, we have included them here for
completeness.

### Selecting single scatters

Since we are working with reduced quantities, most of the work has already been
done for us. We just need to use the identifier to select the single scatters.

```yaml
- name: Select single scatters
  type: fasthep_carpenter.operators.SelectOperator
  kwargs:
    when: "Scatters.ss.nSingleScatters == 1"
```

### Histogramming

```yaml
- name: S1 vs S2
  type: fasthep_carpenter.operators.HistogramOperator
  kwargs:
    histograms:
      - name: s1c_log10_s2c_hist
        input:
          [
            "Scatters.ss.correctedS1Area_phd",
            "log10(Scatters.ss.correctedS2Area_phd)",
          ]
        bins:
          [
            { low: 0., high: 2200, nbins: 1100 },
            { low: 3.5, high: 6.5, nbins: 1000 },
          ]
```

### Making paper-ready plots

The final step is to make a paper-ready plot. We will use the
`airflow.operators.bash.BashOperator` for this:

```yaml
- name: Make paper-ready plot
  type: airflow.operators.bash.BashOperator
  kwargs:
    bash_command: |
      fasthep plotter \
        --input /path/to/output \
        --output /path/to/output/plots/
```

### Putting it all together

TODO
